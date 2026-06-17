#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

import ray
from tqdm import tqdm

from common.utils import (
    load_config,
    resolve_output_path,
    load_jsonl_prompts,
    validate_config,
    configure_logging,
    ensure_dir,
)


# --------------------------------------------
# Logging
# --------------------------------------------
logger = logging.getLogger(__name__)


# --------------------------------------------
# Ray Actor
# --------------------------------------------
@ray.remote
class VLLMWorker:
    def __init__(
        self,
        model_name: str,
        tensor_parallel_size: int = 1,
        max_model_len: int = 2048,
        max_num_seqs: int = 256,
        gpu_memory_utilization: float = 0.9,
        quantization: Optional[str] = None,
        cpu_only: bool = False,
        dtype: str = "half",
    ):
        self.model_name = model_name
        self.cpu_only = cpu_only

        msg = f"[Actor {os.getpid()}] Initializing worker model={model_name} tp={tensor_parallel_size}"
        print(msg, flush=True)
        logger.info(msg)
        
        msg = f"[Actor {os.getpid()}] CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES')}"
        print(msg, flush=True)
        logger.info(msg)

        try:
            print(f"[Actor {os.getpid()}] Importing vLLM...", flush=True)
            from vllm import LLM, SamplingParams

            print(f"[Actor {os.getpid()}] Creating LLM instance...", flush=True)
            if cpu_only:
                self.llm = LLM(
                    model=model_name,
                    device="cpu",
                    tensor_parallel_size=1,
                    max_model_len=max_model_len,
                    max_num_seqs=max_num_seqs,
                )
            else:
                self.llm = LLM(
                    model=model_name,
                    tensor_parallel_size=tensor_parallel_size,
                    max_model_len=max_model_len,
                    max_num_seqs=max_num_seqs,
                    gpu_memory_utilization=gpu_memory_utilization,
                    quantization=quantization,
                    dtype=dtype,
                )

            self.SamplingParams = SamplingParams
            msg = f"[Actor {os.getpid()}] Worker initialized successfully"
            print(msg, flush=True)
            logger.info(msg)

        except Exception as e:
            msg = f"[Actor {os.getpid()}] Failed to initialize model: {e}"
            print(msg, flush=True)
            logger.error(msg)
            raise

    def generate(self, prompts: List[str], params: Dict[str, Any]):
        sampling = self.SamplingParams(**params)

        outputs = self.llm.generate(prompts, sampling)

        results = []
        for output in outputs:
            results.append({
                "prompt": output.prompt,
                "generated_text": [o.text for o in output.outputs],
                "finish_reason": [o.finish_reason for o in output.outputs],
                "num_tokens": [len(o.token_ids) for o in output.outputs],
            })

        return results

    def get_model_info(self):
        return {
            "model_name": self.model_name,
            "cpu_only": self.cpu_only,
        }


# --------------------------------------------
# Worker Creation (LSF-aligned)
# --------------------------------------------
def create_workers(config: Dict[str, Any], num_gpus_available: int):
    model = config["model"]
    exec_cfg = config["execution"]
    lsf_cfg = config["lsf"]

    device = exec_cfg.get("device", "gpu")
    cpu_only = device == "cpu"
    cpus_per_worker = exec_cfg.get("cpus_per_worker", 1)

    tensor_parallel_size = model.get("tensor_parallel_size", 1)
    if tensor_parallel_size == "auto":
        tensor_parallel_size = 1

    # ✅ Correct auto behavior: match LSF
    if exec_cfg["num_workers"] == "auto":
        num_workers = lsf_cfg["num_workers"]
    else:
        num_workers = exec_cfg["num_workers"]

    # ✅ Validation
    if not cpu_only:
        total_required_gpus = num_workers * tensor_parallel_size
        if total_required_gpus > num_gpus_available:
            raise ValueError(
                f"Not enough GPUs: required={total_required_gpus}, available={num_gpus_available}"
            )

    logger.info(f"Creating {num_workers} workers (tp={tensor_parallel_size})")

    workers = []
    for i in range(num_workers):
        worker = VLLMWorker.options(
            num_cpus=cpus_per_worker,
            num_gpus=0 if cpu_only else tensor_parallel_size,
        ).remote(
            model_name=model["name"],
            tensor_parallel_size=tensor_parallel_size,
            max_model_len=model["max_model_len"],
            max_num_seqs=model["max_num_seqs"],
            gpu_memory_utilization=model["gpu_memory_utilization"],
            quantization=model.get("quantization"),
            cpu_only=cpu_only,
            dtype=model.get("dtype", "half"),
        )

        workers.append(worker)
        logger.info(f"Created worker {i+1}/{num_workers}")

    return workers


# --------------------------------------------
# Backpressure Scheduler
# --------------------------------------------
def run_batch_inference(
    workers,
    prompts,
    config: Dict[str, Any],
):
    batch_size = config["execution"]["batch_size"]
    gen_cfg = config["generation"]

    batches = [
        prompts[i:i + batch_size]
        for i in range(0, len(prompts), batch_size)
    ]

    logger.info(f"{len(prompts)} prompts → {len(batches)} batches")

    params = dict(
        temperature=gen_cfg["temperature"],
        top_p=gen_cfg["top_p"],
        max_tokens=gen_cfg["max_tokens"],
        n=gen_cfg["n"],
        stop=gen_cfg.get("stop"),
    )

    results = []
    in_flight = []
    max_in_flight = len(workers) * 2

    for i, batch in enumerate(batches):
        while len(in_flight) >= max_in_flight:
            done, in_flight = ray.wait(in_flight, num_returns=1)
            results.extend(ray.get(done[0]))

        worker = workers[i % len(workers)]
        fut = worker.generate.remote(batch, params)
        in_flight.append(fut)

    while in_flight:
        done, in_flight = ray.wait(in_flight, num_returns=1)
        results.extend(ray.get(done[0]))

    return results


# --------------------------------------------
# Save Results
# --------------------------------------------
def save_results(results, output_path: str):
    with open(output_path, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    logger.info(f"Saved {len(results)} results to {output_path}")


# --------------------------------------------
# Main
# --------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Batch inference with vLLM on Ray + LSF"
    )
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    # ✅ Load + validate config
    config = load_config(args.config)
    validate_config(config)

    # ✅ Logging
    configure_logging(config["logging"].get("level", "INFO"))

    logger.info("=== Ray + vLLM Batch Inference ===")
    logger.info(f"Model: {config['model']['name']}")

    # ✅ Connect to Ray
    ray.init(address="auto")

    resources = ray.cluster_resources()
    num_gpus = int(resources.get("GPU", 0))
    num_cpus = int(resources.get("CPU", 0))

    logger.info(f"Cluster resources: {num_cpus} CPUs, {num_gpus} GPUs")

    # ✅ Load prompts
    prompts = load_jsonl_prompts(
        config["data"]["input_path"],
        config["data"].get("max_prompts"),
    )

    if not prompts:
        raise RuntimeError("No prompts loaded")

    # ✅ Create workers
    workers = create_workers(config, num_gpus)

    # Warmup
    logger.info("Warming up workers...")
    ray.get([w.get_model_info.remote() for w in workers])

    start_time = time.time()

    # ✅ Run inference
    results = run_batch_inference(workers, prompts, config)

    # ✅ Save output
    output_path = resolve_output_path(config["data"]["output_path"])
    ensure_dir(output_path)
    save_results(results, output_path)

    # ✅ Stats
    elapsed = time.time() - start_time
    logger.info(f"Processed {len(prompts)} prompts in {elapsed:.2f}s")
    logger.info(f"Throughput: {len(prompts)/elapsed:.2f} prompts/sec")

    ray.shutdown()


if __name__ == "__main__":
    main()
