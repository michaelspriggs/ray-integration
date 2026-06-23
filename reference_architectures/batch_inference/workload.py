#!/usr/bin/env python3

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Any

import ray
import ray.data

from common.utils import (
    load_config,
    resolve_path,
    validate_config,
    configure_logging,
)

# --------------------------------------------
# Logging
# --------------------------------------------
logger = logging.getLogger(__name__)

# --------------------------------------------
# Global model (per Ray worker process)
# --------------------------------------------
_model = None
_sampling_params_class = None


# --------------------------------------------
# Inference Callable
# --------------------------------------------
class VLLMInference:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model_cfg = config["model"]
        self.gen_cfg = config["generation"]
        self.exec_cfg = config["execution"]

        self.cpu_only = self.exec_cfg.get("device") == "cpu"

    def __call__(self, batch: Dict[str, List[str]]) -> Dict[str, List[Any]]:
        global _model, _sampling_params_class

        # Lazy model initialization (once per worker)
        if _model is None:
            logger.info(f"[Worker {os.getpid()}] Loading model...")
            logger.info(f"CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES')}")
            logger.info(f"CPU-only mode: {self.cpu_only}")

            if self.cpu_only:
                # CPU inference (Transformers)
                from transformers import AutoTokenizer, AutoModelForCausalLM
                import torch

                _model = {
                    "tokenizer": AutoTokenizer.from_pretrained(self.model_cfg["name"]),
                    "model": AutoModelForCausalLM.from_pretrained(
                        self.model_cfg["name"],
                        torch_dtype=torch.float32,
                        device_map="cpu",
                    ),
                }

                logger.info(f"[Worker {os.getpid()}] HF model loaded (CPU)")

            else:
                # GPU inference (vLLM)
                from vllm import LLM, SamplingParams
                _sampling_params_class = SamplingParams

                tp = self.model_cfg.get("tensor_parallel_size", 1)
                if tp == "auto":
                    tp = 1

                _model = LLM(
                    model=self.model_cfg["name"],
                    tensor_parallel_size=tp,
                    max_model_len=self.model_cfg["max_model_len"],
                    max_num_seqs=self.model_cfg["max_num_seqs"],
                    gpu_memory_utilization=self.model_cfg["gpu_memory_utilization"],
                    quantization=self.model_cfg.get("quantization"),
                    dtype=self.model_cfg.get("dtype", "half"),
                )

                logger.info(f"[Worker {os.getpid()}] vLLM model loaded")

        # Extract prompts
        prompts = batch.get("text") or batch.get("prompt")
        if prompts is None or len(prompts) == 0:
            raise ValueError("Batch must contain 'text' or 'prompt'")

        # --------------------------------------------
        # CPU (Transformers)
        # --------------------------------------------
        if self.cpu_only:
            import torch

            tokenizer = _model["tokenizer"]
            model = _model["model"]

            results = {
                "prompt": [],
                "generated_text": [],
                "finish_reason": [],
                "num_tokens": [],
            }

            # ✅ Batch tokenize
            inputs = tokenizer(prompts, return_tensors="pt", padding=True)

            with torch.no_grad():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=self.gen_cfg["max_tokens"],
                    temperature=self.gen_cfg["temperature"],
                    top_p=self.gen_cfg["top_p"],
                    do_sample=True if self.gen_cfg["temperature"] > 0 else False,
                    pad_token_id=tokenizer.eos_token_id,
                )

            # ✅ FIX: use attention mask for correct lengths
            input_lens = inputs["attention_mask"].sum(dim=1)

            for i, prompt in enumerate(prompts):
                prompt_len = input_lens[i]
                generated_ids = outputs[i][prompt_len:]
                generated_text = tokenizer.decode(generated_ids, skip_special_tokens=True)

                results["prompt"].append(prompt)
                results["generated_text"].append([generated_text])
                results["finish_reason"].append(["stop"])
                results["num_tokens"].append([len(generated_ids)])

        # --------------------------------------------
        # GPU (vLLM)
        # --------------------------------------------
        else:
            sampling_params = _sampling_params_class(
                temperature=self.gen_cfg["temperature"],
                top_p=self.gen_cfg["top_p"],
                max_tokens=self.gen_cfg["max_tokens"],
                n=self.gen_cfg["n"],
                stop=self.gen_cfg.get("stop"),
            )

            outputs = _model.generate(prompts, sampling_params)

            results = {
                "prompt": [],
                "generated_text": [],
                "finish_reason": [],
                "num_tokens": [],
            }

            for out in outputs:
                results["prompt"].append(out.prompt)
                results["generated_text"].append([o.text for o in out.outputs])
                results["finish_reason"].append([o.finish_reason for o in out.outputs])
                results["num_tokens"].append([len(o.token_ids) for o in out.outputs])

        return results


# --------------------------------------------
# Main
# --------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Batch inference with Ray Data + vLLM")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    # Load + validate config
    config = load_config(args.config)
    validate_config(config)

    configure_logging(config["logging"].get("level", "INFO"))

    logger.info("=== Batch Inference ===")
    logger.info(f"Model: {config['model']['name']}")

    # Connect to Ray
    try:
        ray.init(address="auto")
    except Exception as e:
        logger.error(f"Failed to connect to Ray: {e}")
        sys.exit(1)

    resources = ray.cluster_resources()
    logger.info(f"Cluster resources: {resources}")

    # Load dataset
    input_path = resolve_path(config["data"]["input_path"])
    logger.info(f"Reading dataset: {input_path}")

    ds = ray.data.read_json(input_path)

    if config["data"].get("max_prompts"):
        ds = ds.limit(config["data"]["max_prompts"])

    count = ds.count()
    if count == 0:
        raise RuntimeError("No prompts loaded")

    logger.info(f"Loaded {count} prompts")

    # Resource planning
    exec_cfg = config["execution"]
    model_cfg = config["model"]
    lsf_cfg = config["lsf"]

    cpu_only = exec_cfg.get("device") == "cpu"

    cpus_per_worker = lsf_cfg.get("cpus_per_worker", 1)
    tensor_parallel_size = model_cfg.get("tensor_parallel_size", 1)
    if tensor_parallel_size == "auto":
        tensor_parallel_size = 1

    concurrency = lsf_cfg["num_workers"]

    if cpu_only:
        num_gpus_per_task = 0
        num_cpus_per_task = cpus_per_worker
    else:
        num_gpus_per_task = tensor_parallel_size
        num_cpus_per_task = cpus_per_worker

    logger.info("=== Execution Plan ===")
    logger.info(f"Workers: {concurrency}")
    logger.info(f"CPUs/task: {num_cpus_per_task}")
    logger.info(f"GPUs/task: {num_gpus_per_task}")

    # Run pipeline
    batch_size = exec_cfg["batch_size"]

    inference = VLLMInference(config)

    ds = ds.map_batches(
        inference,
        batch_size=batch_size,
        num_cpus=num_cpus_per_task,
        num_gpus=num_gpus_per_task,
        concurrency=concurrency,
    )

    # Output
    output_dir = Path(resolve_path(config["data"]["output_dir"]))
    output_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy(args.config, output_dir / "config.yaml")
    logger.info(f"Config copied to {output_dir / 'config.yaml'}")

    temp_output_dir = output_dir / "results_temp"
    ds.write_json(str(temp_output_dir))

    output_path = output_dir / "results.jsonl"

    import glob
    with open(output_path, "w") as outfile:
        for f in sorted(glob.glob(str(temp_output_dir / "*.json"))):
            with open(f, "r") as infile:
                outfile.write(infile.read())

    shutil.rmtree(temp_output_dir)

    logger.info(f"Results saved to {output_path}")

    ray.shutdown()


if __name__ == "__main__":
    main()
