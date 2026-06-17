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
import yaml


# --------------------------------------------
# Logging
# --------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# --------------------------------------------
# Ray Actor
# --------------------------------------------
@ray.remote
class VLLMWorker:
    def __init__(
        self,
        model_name: str,
        tensor_parallel_size: int,
        max_model_len: int,
        max_num_seqs: int,
        gpu_memory_utilization: float,
        quantization: Optional[str],
        dtype: str,
        cpu_only: bool,
    ):
        from vllm import LLM, SamplingParams

        self.model_name = model_name
        self.cpu_only = cpu_only

        logger.info(f"[Worker] Starting model={model_name} tp={tensor_parallel_size}")

        if cpu_only:
            self.llm = LLM(
                model=model_name,
                device="cpu",
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

    def generate(self, prompts: List[str], params: Dict[str, Any]):
        sampling = self.SamplingParams(**params)
        outputs = self.llm.generate(prompts, sampling)

        results = []
        for o in outputs:
            results.append({
                "prompt": o.prompt,
                "generated_text": [x.text for x in o.outputs],
                "finish_reason": [x.finish_reason for x in o.outputs],
                "num_tokens": [len(x.token_ids) for x in o.outputs],
            })
        return results


# --------------------------------------------
# Utilities
# --------------------------------------------
def load_config(path: str):
    return yaml.safe_load(open(path))


def load_prompts(path: str, max_prompts=None):
    prompts = []
    with open(path) as f:
        for line in f:
            data = json.loads(line)
            p = data.get("text") or data.get("prompt")
            if p:
                prompts.append(p)
            if max_prompts and len(prompts) >= max_prompts:
                break
    return prompts


def save_results(results, path):
    job_id = os.environ.get("LSB_JOBID", "")
    path = path.replace("%J", job_id).replace("{job_id}", job_id)
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    logger.info(f"Saved results to {path}")


# --------------------------------------------
# Worker creation (FIXED AUTO LOGIC)
# --------------------------------------------
def create_workers(config, num_gpus_available):
    model = config["model"]
    exec_cfg = config["execution"]
    lsf_cfg = config["lsf"]

    device = exec_cfg.get("device", "gpu")
    cpu_only = device == "cpu"
    cpus_per_worker = exec_cfg.get("cpus_per_worker", 1)

    tensor_parallel_size = model["tensor_parallel_size"]
    if tensor_parallel_size == "auto":
        tensor_parallel_size = 1

    # ✅ Correct "auto" logic
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

        if num_gpus_available % tensor_parallel_size != 0:
            raise ValueError(
                f"GPU count {num_gpus_available} not divisible by tensor_parallel_size={tensor_parallel_size}"
            )

    logger.info(f"Creating {num_workers} workers (tp={tensor_parallel_size})")

    workers = []
    for i in range(num_workers):
        w = VLLMWorker.options(
            num_cpus=cpus_per_worker,
            num_gpus=0 if cpu_only else tensor_parallel_size,
        ).remote(
            model_name=model["name"],
            tensor_parallel_size=tensor_parallel_size,
            max_model_len=model["max_model_len"],
            max_num_seqs=model["max_num_seqs"],
            gpu_memory_utilization=model["gpu_memory_utilization"],
            quantization=model.get("quantization"),
            dtype=model.get("dtype", "half"),
            cpu_only=cpu_only,
        )

        workers.append(w)

    return workers


# --------------------------------------------
# Backpressure Scheduler (IMPORTANT)
# --------------------------------------------
def run_inference(workers, prompts, config):
    batch_size = config["execution"]["batch_size"]
    gen_cfg = config["generation"]

    batches = [
        prompts[i:i+batch_size] for i in range(0, len(prompts), batch_size)
    ]

    logger.info(f"{len(prompts)} prompts → {len(batches)} batches")

    params = dict(
        temperature=gen_cfg["temperature"],
        top_p=gen_cfg["top_p"],
        max_tokens=gen_cfg["max_tokens"],
        n=gen_cfg["n"],
        stop=gen_cfg.get("stop"),
    )

    in_flight = []
    results = []

    max_in_flight = len(workers) * 2

    for i, batch in enumerate(batches):
        while len(in_flight) >= max_in_flight:
            done, in_flight = ray.wait(in_flight, num_returns=1)
            results.extend(ray.get(done[0]))

        worker = workers[i % len(workers)]
        fut = worker.generate.remote(batch, params)
        in_flight.append(fut)

    # drain
    while in_flight:
        done, in_flight = ray.wait(in_flight, num_returns=1)
        results.extend(ray.get(done[0]))

    return results


# --------------------------------------------
# Main
# --------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)

    logger.info("Connecting to Ray...")
    ray.init(address="auto")

    resources = ray.cluster_resources()
    num_gpus = int(resources.get("GPU", 0))
    num_cpus = int(resources.get("CPU", 0))

    logger.info(f"Cluster: CPUs={num_cpus}, GPUs={num_gpus}")

    prompts = load_prompts(
        config["data"]["input_path"],
        config["data"].get("max_prompts")
    )

    if not prompts:
        raise RuntimeError("No prompts loaded")

    workers = create_workers(config, num_gpus)

    logger.info("Initializing workers...")
    ray.get([w.generate.remote(["warmup"], dict(max_tokens=1)) for w in workers])

    start = time.time()

    results = run_inference(workers, prompts, config)

    elapsed = time.time() - start

    save_results(results, config["data"]["output_path"])

    logger.info(f"Done. {len(prompts)} prompts in {elapsed:.2f}s")
    logger.info(f"Throughput: {len(prompts)/elapsed:.2f} prompts/sec")

    ray.shutdown()


if __name__ == "__main__":
    main()
