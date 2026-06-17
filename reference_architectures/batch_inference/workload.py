#!/usr/bin/env python3

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

import ray
import ray.data

from common.utils import (
    load_config,
    resolve_output_path,
    validate_config,
    configure_logging,
    ensure_dir,
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
            logger.info(f"[Worker {os.getpid()}] Loading vLLM model...")
            logger.info(f"CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES')}")

            from vllm import LLM, SamplingParams
            _sampling_params_class = SamplingParams

            tp = self.model_cfg.get("tensor_parallel_size", 1)
            if tp == "auto":
                tp = 1

            if self.cpu_only:
                _model = LLM(
                    model=self.model_cfg["name"],
                    device="cpu",
                    tensor_parallel_size=1,
                    max_model_len=self.model_cfg["max_model_len"],
                    max_num_seqs=self.model_cfg["max_num_seqs"],
                )
            else:
                _model = LLM(
                    model=self.model_cfg["name"],
                    tensor_parallel_size=tp,
                    max_model_len=self.model_cfg["max_model_len"],
                    max_num_seqs=self.model_cfg["max_num_seqs"],
                    gpu_memory_utilization=self.model_cfg["gpu_memory_utilization"],
                    quantization=self.model_cfg.get("quantization"),
                    dtype=self.model_cfg.get("dtype", "half"),
                )

            logger.info(f"[Worker {os.getpid()}] Model loaded")

        # Extract prompts
        prompts = batch.get("text") or batch.get("prompt")
        if not prompts:
            raise ValueError("Batch must contain 'text' or 'prompt' field")

        # Sampling parameters
        sampling_params = _sampling_params_class(
            temperature=self.gen_cfg["temperature"],
            top_p=self.gen_cfg["top_p"],
            max_tokens=self.gen_cfg["max_tokens"],
            n=self.gen_cfg["n"],
            stop=self.gen_cfg.get("stop"),
        )

        # Run inference
        outputs = _model.generate(prompts, sampling_params)

        # Format output
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
    parser = argparse.ArgumentParser(
        description="Batch inference with vLLM using Ray Data"
    )
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    # ✅ Load + validate config
    config = load_config(args.config)
    validate_config(config)

    # ✅ Logging
    configure_logging(config["logging"].get("level", "INFO"))

    logger.info("=== Ray Data + vLLM Batch Inference ===")
    logger.info(f"Model: {config['model']['name']}")

    # ✅ Connect to Ray
    try:
        ray.init(address="auto")
    except Exception as e:
        logger.error(f"Failed to connect to Ray cluster: {e}")
        sys.exit(1)

    resources = ray.cluster_resources()
    num_gpus = int(resources.get("GPU", 0))
    num_cpus = int(resources.get("CPU", 0))

    logger.info(f"Cluster resources: CPUs={num_cpus}, GPUs={num_gpus}")
    logger.info(f"Ray resources: {resources}")

    # --------------------------------------------
    # Dataset loading
    # --------------------------------------------
    input_path = config["data"]["input_path"]

    logger.info(f"Reading dataset: {input_path}")
    ds = ray.data.read_json(input_path)

    if config["data"].get("max_prompts"):
        ds = ds.limit(config["data"]["max_prompts"])

    count = ds.count()
    if count == 0:
        raise RuntimeError("No prompts loaded")

    logger.info(f"Loaded {count} prompts")

    # --------------------------------------------
    # Resource model (LSF-aligned)
    # --------------------------------------------
    exec_cfg = config["execution"]
    model_cfg = config["model"]
    lsf_cfg = config["lsf"]

    cpu_only = exec_cfg.get("device") == "cpu"
    cpus_per_worker = exec_cfg.get("cpus_per_worker", 1)

    tensor_parallel_size = model_cfg.get("tensor_parallel_size", 1)
    if tensor_parallel_size == "auto":
        tensor_parallel_size = 1

    # ✅ KEY: match LSF workers
    concurrency = lsf_cfg["num_workers"]

    if cpu_only:
        num_gpus_per_task = 0
        num_cpus_per_task = cpus_per_worker
    else:
        num_gpus_per_task = tensor_parallel_size
        num_cpus_per_task = cpus_per_worker

    # ✅ Validation
    if not cpu_only:
        required_gpus = concurrency * tensor_parallel_size
        if required_gpus > num_gpus:
            raise RuntimeError(
                f"Not enough GPUs: required={required_gpus}, available={num_gpus}"
            )

    logger.info("=== Execution Plan ===")
    logger.info(f"LSF workers: {lsf_cfg['num_workers']}")
    logger.info(f"Concurrency: {concurrency}")
    logger.info(f"CPUs per task: {num_cpus_per_task}")
    logger.info(f"GPUs per task: {num_gpus_per_task}")

    # --------------------------------------------
    # Run pipeline
    # --------------------------------------------
    batch_size = exec_cfg["batch_size"]

    if batch_size > 1024:
        logger.warning(f"Large batch_size={batch_size} may cause OOM")

    inference = VLLMInference(config)

    ds = ds.map_batches(
        inference,
        batch_size=batch_size,
        num_cpus=num_cpus_per_task,
        num_gpus=num_gpus_per_task,
        concurrency=concurrency,
    )

    # --------------------------------------------
    # Write output
    # --------------------------------------------
    output_path = resolve_output_path(config["data"]["output_path"])
    ensure_dir(output_path)

    logger.info(f"Writing results to {output_path}")
    ds.write_json(output_path)

    logger.info("=== Inference Complete ===")
    logger.info(f"Results saved to {output_path}")

    ray.shutdown()


if __name__ == "__main__":
    main()
