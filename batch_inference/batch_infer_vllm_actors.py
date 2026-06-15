#!/usr/bin/env python3
"""
Batch Inference with vLLM on Ray + LSF

This script demonstrates distributed batch inference using:
- Ray 2.x for distributed execution
- vLLM for efficient LLM inference
- LSF for resource management

The script automatically detects GPUs via CUDA_VISIBLE_DEVICES set by LSF
and creates vLLM actors accordingly.
"""

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
from tqdm import tqdm


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@ray.remote
class VLLMWorker:
    """
    Ray actor that wraps a vLLM engine for batch inference.
    
    Each actor can use one or more GPUs (tensor parallelism) and processes
    batches of prompts independently.
    """
    
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
        """
        Initialize vLLM engine.
        
        Args:
            model_name: HuggingFace model name or local path
            tensor_parallel_size: Number of GPUs for tensor parallelism
            max_model_len: Maximum sequence length
            max_num_seqs: Maximum number of sequences in parallel
            gpu_memory_utilization: GPU memory utilization (0.0-1.0)
            quantization: Quantization method (awq, gptq, or None)
            cpu_only: Use CPU-only mode for testing
            dtype: Model dtype for vLLM (for example: auto, half, float16, bfloat16)
        """
        self.model_name = model_name
        self.cpu_only = cpu_only
        
        logger.info(f"Initializing vLLM worker with model: {model_name}")
        logger.info(f"Tensor parallel size: {tensor_parallel_size}")
        logger.info(f"CPU-only mode: {cpu_only}")
        
        try:
            from vllm import LLM, SamplingParams
            
            # Initialize vLLM engine
            if cpu_only:
                # CPU-only mode for testing
                self.llm = LLM(
                    model=model_name,
                    tensor_parallel_size=1,
                    max_model_len=max_model_len,
                    max_num_seqs=max_num_seqs,
                    device="cpu",
                )
            else:
                # GPU mode - vLLM will use CUDA_VISIBLE_DEVICES
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
            logger.info("vLLM worker initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize vLLM: {e}")
            raise
    
    def generate(
        self,
        prompts: List[str],
        temperature: float = 0.7,
        top_p: float = 0.9,
        max_tokens: int = 512,
        n: int = 1,
        stop: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate completions for a batch of prompts.
        
        Args:
            prompts: List of input prompts
            temperature: Sampling temperature
            top_p: Top-p sampling parameter
            max_tokens: Maximum tokens to generate
            n: Number of completions per prompt
            stop: Stop sequences
            
        Returns:
            List of results with generated text and metadata
        """
        try:
            sampling_params = self.SamplingParams(
                temperature=temperature,
                top_p=top_p,
                max_tokens=max_tokens,
                n=n,
                stop=stop,
            )
            
            outputs = self.llm.generate(prompts, sampling_params)
            
            results = []
            for output in outputs:
                result = {
                    "prompt": output.prompt,
                    "generated_text": [o.text for o in output.outputs],
                    "finish_reason": [o.finish_reason for o in output.outputs],
                    "num_tokens": [len(o.token_ids) for o in output.outputs],
                }
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Generation failed: {e}")
            raise
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the loaded model."""
        return {
            "model_name": self.model_name,
            "cpu_only": self.cpu_only,
        }


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def load_prompts(input_path: str, max_prompts: Optional[int] = None) -> List[str]:
    """
    Load prompts from JSONL file.
    
    Expected format: {"text": "prompt"} or {"prompt": "prompt"}
    """
    prompts = []
    with open(input_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if max_prompts and len(prompts) >= max_prompts:
                break
            data = json.loads(line)
            prompt = data.get('text') or data.get('prompt')
            if prompt:
                prompts.append(prompt)
    
    logger.info(f"Loaded {len(prompts)} prompts from {input_path}")
    return prompts


def save_results(results: List[Dict[str, Any]], output_path: str):
    """Save results to JSONL file."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        for result in results:
            f.write(json.dumps(result) + '\n')
    
    logger.info(f"Saved {len(results)} results to {output_path}")


def create_workers(
    config: Dict[str, Any],
    num_gpus_available: int,
) -> List[ray.ObjectRef]:
    """
    Create vLLM worker actors based on available resources.
    
    Args:
        config: Configuration dictionary
        num_gpus_available: Number of GPUs available in the cluster
        
    Returns:
        List of worker actor references
    """
    model_config = config['model']
    exec_config = config['execution']
    
    tensor_parallel_size = model_config['tensor_parallel_size']
    num_workers = exec_config['num_workers']
    cpu_only = exec_config.get('cpu_only', False)
    
    # Determine number of workers
    if num_workers == "auto":
        if cpu_only:
            # For CPU, use 4 workers by default
            num_workers = 4
        else:
            # For GPU, one worker per tensor_parallel_size GPUs
            if tensor_parallel_size == "auto":
                tensor_parallel_size = max(1, num_gpus_available)
                num_workers = 1
            else:
                num_workers = max(1, num_gpus_available // tensor_parallel_size)
    
    if tensor_parallel_size == "auto":
        tensor_parallel_size = 1
    
    logger.info(f"Creating {num_workers} workers with tensor_parallel_size={tensor_parallel_size}")
    logger.info(f"CPU-only mode: {cpu_only}")
    
    workers = []
    for i in range(num_workers):
        if cpu_only:
            # CPU-only worker
            worker = VLLMWorker.remote(
                model_name=model_config['name'],
                tensor_parallel_size=1,
                max_model_len=model_config['max_model_len'],
                max_num_seqs=model_config['max_num_seqs'],
                cpu_only=True,
            )
        else:
            # GPU worker
            worker = VLLMWorker.options(
                num_gpus=tensor_parallel_size
            ).remote(
                model_name=model_config['name'],
                tensor_parallel_size=tensor_parallel_size,
                max_model_len=model_config['max_model_len'],
                max_num_seqs=model_config['max_num_seqs'],
                gpu_memory_utilization=model_config['gpu_memory_utilization'],
                quantization=model_config.get('quantization'),
                cpu_only=False,
                dtype=model_config.get('dtype', 'half'),
            )
        workers.append(worker)
        logger.info(f"Created worker {i+1}/{num_workers}")
    
    return workers


def run_batch_inference(
    workers: List[ray.ObjectRef],
    prompts: List[str],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Run batch inference across multiple workers.
    
    Args:
        workers: List of vLLM worker actors
        prompts: List of prompts to process
        config: Configuration dictionary
        
    Returns:
        List of results
    """
    batch_size = config['execution']['batch_size']
    gen_config = config['generation']
    show_progress = config['logging'].get('show_progress', True)
    
    # Split prompts into batches
    batches = [
        prompts[i:i + batch_size]
        for i in range(0, len(prompts), batch_size)
    ]
    
    logger.info(f"Processing {len(prompts)} prompts in {len(batches)} batches")
    logger.info(f"Batch size: {batch_size}, Workers: {len(workers)}")
    
    # Distribute batches across workers
    futures = []
    for i, batch in enumerate(batches):
        worker = workers[i % len(workers)]
        future = worker.generate.remote(
            prompts=batch,
            temperature=gen_config['temperature'],
            top_p=gen_config['top_p'],
            max_tokens=gen_config['max_tokens'],
            n=gen_config['n'],
            stop=gen_config.get('stop'),
        )
        futures.append(future)
    
    # Collect results with progress bar
    results = []
    if show_progress:
        for future in tqdm(futures, desc="Processing batches"):
            batch_results = ray.get(future)
            results.extend(batch_results)
    else:
        results = ray.get(futures)
        # Flatten results
        flattened = []
        for batch_results in results:
            flattened.extend(batch_results)
        results = flattened
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Batch inference with vLLM on Ray + LSF"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="batch_inference/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--cpu-only",
        action="store_true",
        help="Force CPU-only mode (overrides config)"
    )
    parser.add_argument(
        "--model",
        type=str,
        help="Model name (overrides config)"
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Input file path (overrides config)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (overrides config)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with command-line arguments
    if args.cpu_only:
        config['execution']['cpu_only'] = True
        config['execution']['use_gpu'] = False
    if args.model:
        config['model']['name'] = args.model
    if args.input:
        config['data']['input_path'] = args.input
    if args.output:
        config['data']['output_path'] = args.output
    
    # Set logging level
    log_level = config['logging'].get('level', 'INFO')
    logging.getLogger().setLevel(getattr(logging, log_level))
    
    logger.info("=== Ray + vLLM Batch Inference ===")
    logger.info(f"Model: {config['model']['name']}")
    logger.info(f"Input: {config['data']['input_path']}")
    logger.info(f"Output: {config['data']['output_path']}")
    
    # Connect to Ray cluster (should already be running via ray_launch_cluster.sh)
    try:
        ray.init(address="auto")
        logger.info("Connected to Ray cluster")
    except Exception as e:
        logger.error(f"Failed to connect to Ray cluster: {e}")
        logger.info("Make sure Ray cluster is running via ray_launch_cluster.sh")
        sys.exit(1)
    
    # Get cluster resources
    resources = ray.cluster_resources()
    num_gpus = int(resources.get("GPU", 0))
    num_cpus = int(resources.get("CPU", 0))
    
    logger.info(f"Cluster resources: {num_cpus} CPUs, {num_gpus} GPUs")
    logger.info(f"CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES', 'not set')}")
    
    # Load prompts
    prompts = load_prompts(
        config['data']['input_path'],
        config['data'].get('max_prompts')
    )
    
    if not prompts:
        logger.error("No prompts loaded. Check input file.")
        sys.exit(1)
    
    # Create workers
    start_time = time.time()
    workers = create_workers(config, num_gpus)
    
    # Wait for workers to initialize
    logger.info("Waiting for workers to initialize...")
    ray.get([worker.get_model_info.remote() for worker in workers])
    logger.info("All workers initialized")
    
    # Run inference
    logger.info("Starting batch inference...")
    results = run_batch_inference(workers, prompts, config)
    
    # Save results
    save_results(results, config['data']['output_path'])
    
    # Report statistics
    elapsed_time = time.time() - start_time
    throughput = len(prompts) / elapsed_time
    
    logger.info("=== Inference Complete ===")
    logger.info(f"Processed: {len(prompts)} prompts")
    logger.info(f"Time: {elapsed_time:.2f} seconds")
    logger.info(f"Throughput: {throughput:.2f} prompts/second")
    
    # Shutdown
    ray.shutdown()
    logger.info("Ray shutdown complete")


if __name__ == "__main__":
    main()

# Made with Bob
