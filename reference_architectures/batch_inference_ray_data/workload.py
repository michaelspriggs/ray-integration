#!/usr/bin/env python3
"""
Batch Inference with vLLM using Ray Data Pipeline

This is an alternative implementation using Ray Data for streaming inference.
Ray Data provides automatic batching, fault tolerance, and simpler code for
data-centric workloads.

Advantages over actor-based approach:
- Simpler code
- Automatic data partitioning
- Built-in fault tolerance
- Better for streaming/large datasets

Use this when:
- You have large datasets that don't fit in memory
- You want simpler code
- You need automatic fault tolerance
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

import ray
import ray.data
import yaml


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Global model instance (lazy loaded per worker)
_model = None
_sampling_params_class = None


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def resolve_output_path(output_path: str) -> str:
    """Resolve output path placeholders using the current job environment."""
    job_id = os.environ.get("LSB_JOBID")
    if job_id:
        output_path = output_path.replace("%J", job_id).replace("{job_id}", job_id)
    return output_path


class VLLMInference:
    """Callable class for vLLM inference in Ray Data pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize inference configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.model_config = config['model']
        self.gen_config = config['generation']
        self.cpu_only = config['execution'].get('cpu_only', False)
    
    def __call__(self, batch: Dict[str, List[str]]) -> Dict[str, List[Any]]:
        """
        Process a batch of prompts.
        
        Args:
            batch: Dictionary with 'text' or 'prompt' key containing list of prompts
            
        Returns:
            Dictionary with results
        """
        global _model, _sampling_params_class
        
        # Lazy load model (once per worker)
        if _model is None:
            logger.info("Loading vLLM model...")
            try:
                from vllm import LLM, SamplingParams
                
                _sampling_params_class = SamplingParams
                
                if self.cpu_only:
                    _model = LLM(
                        model=self.model_config['name'],
                        tensor_parallel_size=1,
                        max_model_len=self.model_config['max_model_len'],
                        max_num_seqs=self.model_config['max_num_seqs'],
                        device="cpu",
                    )
                else:
                    _model = LLM(
                        model=self.model_config['name'],
                        tensor_parallel_size=self.model_config.get('tensor_parallel_size', 1),
                        max_model_len=self.model_config['max_model_len'],
                        max_num_seqs=self.model_config['max_num_seqs'],
                        gpu_memory_utilization=self.model_config['gpu_memory_utilization'],
                        quantization=self.model_config.get('quantization'),
                    )
                
                logger.info("vLLM model loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load vLLM model: {e}")
                raise
        
        # Get prompts from batch
        prompts = batch.get('text') or batch.get('prompt')
        if not prompts:
            raise ValueError("Batch must contain 'text' or 'prompt' key")
        
        # Create sampling parameters
        sampling_params = _sampling_params_class(
            temperature=self.gen_config['temperature'],
            top_p=self.gen_config['top_p'],
            max_tokens=self.gen_config['max_tokens'],
            n=self.gen_config['n'],
            stop=self.gen_config.get('stop'),
        )
        
        # Generate completions
        outputs = _model.generate(prompts, sampling_params)
        
        # Format results
        results = {
            'prompt': [],
            'generated_text': [],
            'finish_reason': [],
            'num_tokens': [],
        }
        
        for output in outputs:
            results['prompt'].append(output.prompt)
            results['generated_text'].append([o.text for o in output.outputs])
            results['finish_reason'].append([o.finish_reason for o in output.outputs])
            results['num_tokens'].append([len(o.token_ids) for o in output.outputs])
        
        return results


def main():
    parser = argparse.ArgumentParser(
        description="Batch inference with vLLM using Ray Data"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="reference_architectures/batch_inference_ray_data/config.yaml",
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

    config['data']['output_path'] = resolve_output_path(config['data']['output_path'])
    
    # Set logging level
    log_level = config['logging'].get('level', 'INFO')
    logging.getLogger().setLevel(getattr(logging, log_level))
    
    logger.info("=== Ray Data + vLLM Batch Inference ===")
    logger.info(f"Model: {config['model']['name']}")
    logger.info(f"Input: {config['data']['input_path']}")
    logger.info(f"Output: {config['data']['output_path']}")
    
    # Connect to Ray cluster
    try:
        ray.init(address="auto")
        logger.info("Connected to Ray cluster")
    except Exception as e:
        logger.error(f"Failed to connect to Ray cluster: {e}")
        logger.info("Make sure Ray cluster is running via common/start_ray_cluster.sh")
        sys.exit(1)
    
    # Get cluster resources
    resources = ray.cluster_resources()
    num_gpus = int(resources.get("GPU", 0))
    num_cpus = int(resources.get("CPU", 0))
    
    logger.info(f"Cluster resources: {num_cpus} CPUs, {num_gpus} GPUs")
    logger.info(f"CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES', 'not set')}")
    
    # Read input data
    logger.info(f"Reading data from {config['data']['input_path']}")
    ds = ray.data.read_json(config['data']['input_path'])
    
    # Limit number of prompts if specified
    max_prompts = config['data'].get('max_prompts')
    if max_prompts:
        ds = ds.limit(max_prompts)
    
    num_rows = ds.count()
    logger.info(f"Loaded {num_rows} prompts")
    
    if num_rows == 0:
        logger.error("No prompts loaded. Check input file.")
        sys.exit(1)
    
    # Determine resources per task
    batch_size = config['execution']['batch_size']
    cpu_only = config['execution'].get('cpu_only', False)
    tensor_parallel_size = config['model'].get('tensor_parallel_size', 1)
    
    if cpu_only:
        # CPU-only mode
        num_gpus_per_task = 0
        num_cpus_per_task = 2
        concurrency = min(4, num_cpus // 2)
    else:
        # GPU mode
        if tensor_parallel_size == "auto":
            tensor_parallel_size = max(1, num_gpus)
        num_gpus_per_task = tensor_parallel_size
        num_cpus_per_task = 1
        concurrency = max(1, num_gpus // tensor_parallel_size)
    
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"GPUs per task: {num_gpus_per_task}")
    logger.info(f"CPUs per task: {num_cpus_per_task}")
    logger.info(f"Concurrency: {concurrency}")
    
    # Create inference callable
    inference_fn = VLLMInference(config)
    
    # Run inference pipeline
    logger.info("Starting inference pipeline...")
    
    ds = ds.map_batches(
        inference_fn,
        batch_size=batch_size,
        num_gpus=num_gpus_per_task,
        num_cpus=num_cpus_per_task,
        concurrency=concurrency,
    )
    
    # Write results
    output_path = config['data']['output_path']
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Writing results to {output_path}")
    ds.write_json(output_path)
    
    logger.info("=== Inference Complete ===")
    logger.info(f"Results saved to {output_path}")
    
    # Shutdown
    ray.shutdown()
    logger.info("Ray shutdown complete")


if __name__ == "__main__":
    main()

# Made with Bob
