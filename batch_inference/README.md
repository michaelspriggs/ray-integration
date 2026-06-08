# Batch Inference with vLLM on Ray + LSF

This directory contains a production-ready reference implementation for distributed batch inference using:
- **Ray 2.x** for distributed execution
- **vLLM** for efficient LLM inference
- **LSF** for resource management and job scheduling

## Quick Start

### 1. Setup Environment

Create the conda environment:

```bash
# For CPU-only testing
conda env create -f sample_conda_env/ray_2x_cpu.yml
conda activate ray_cpu

# For GPU inference
conda env create -f sample_conda_env/ray_2x_gpu.yml
conda activate ray_gpu
```

### 2. CPU-Only Testing (Development)

Test the pipeline without GPUs using a small model:

```bash
# Submit to LSF
bsub -n 4 -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_cpu \
  -c "python batch_inference/batch_infer_vllm_actors.py --cpu-only --model gpt2" \
  -m 10000000000
```

### 3. GPU Inference (Production)

Run with GPU acceleration:

```bash
# Submit to LSF with 8 GPUs (1 per task, exclusive access)
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_gpu \
  -c "python batch_inference/batch_infer_vllm_actors.py" \
  -m 20000000000
```

## Architecture

### Two Implementation Approaches

#### 1. Actor-Based (Recommended for most use cases)

**File:** `batch_infer_vllm_actors.py`

**Advantages:**
- Fine-grained control over resource allocation
- Better for heterogeneous GPU clusters
- Explicit actor placement
- Easier debugging

**Use when:**
- You need precise control over GPU allocation
- You have heterogeneous hardware
- You want to optimize for specific workload patterns

#### 2. Ray Data Pipeline (Simpler alternative)

**File:** `batch_infer_ray_data.py`

**Advantages:**
- Simpler code
- Automatic data partitioning
- Built-in fault tolerance
- Better for streaming workloads

**Use when:**
- You have large datasets that don't fit in memory
- You want simpler code
- You need automatic fault tolerance

## Configuration

Edit `config.yaml` to customize:

```yaml
model:
  name: "meta-llama/Llama-2-7b-hf"  # HuggingFace model or local path
  tensor_parallel_size: 1            # GPUs per model instance
  max_model_len: 2048                # Maximum sequence length

execution:
  num_workers: "auto"                # "auto" or specific number
  batch_size: 16                     # Prompts per batch
  use_gpu: true                      # Enable GPU
  cpu_only: false                    # Force CPU-only mode

data:
  input_path: "dataset/sample_prompts.jsonl"
  output_path: "output/results.jsonl"

generation:
  temperature: 0.7
  top_p: 0.9
  max_tokens: 512
```

## LSF Submission Patterns

### Pattern 1: CPU-Only (Testing)

```bash
bsub -n 8 -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_cpu \
  -c "python batch_inference/batch_infer_vllm_actors.py --cpu-only --model gpt2" \
  -m 10000000000
```

**Explanation:**
- `-n 8`: Request 8 CPU slots
- `-o output.%J`: Output file with job ID
- No GPU specification
- Uses small model (gpt2) for testing

### Pattern 2: GPU with Exclusive Access (Production)

```bash
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_gpu \
  -c "python batch_inference/batch_infer_vllm_actors.py" \
  -m 20000000000
```

**Explanation:**
- `-n 8`: Request 8 slots
- `-gpu "num=1/task:j_exclusive=yes"`: 1 GPU per task, exclusive access
- `j_exclusive=yes`: Prevents GPU sharing between jobs
- LSF sets `CUDA_VISIBLE_DEVICES` automatically
- Ray auto-detects GPUs from environment

### Optional LSF Parameters

Add these to customize resource requirements:

```bash
# Specify queue
-q gpu_queue

# Set memory limit
-M 100GB

# Set wall time limit
-W 2:00

# Reserve memory per task
-R "rusage[mem=10GB]"

# Complete example
bsub -n 8 \
  -gpu "num=1/task:j_exclusive=yes" \
  -q gpu_queue \
  -M 100GB \
  -W 2:00 \
  -R "rusage[mem=10GB]" \
  -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_gpu \
  -c "python batch_inference/batch_infer_vllm_actors.py" \
  -m 20000000000
```

## Scaling Guidelines

### Small Models (< 7B parameters)

**Configuration:**
```yaml
model:
  tensor_parallel_size: 1  # 1 GPU per actor
execution:
  num_workers: "auto"      # One actor per GPU
  batch_size: 32           # Larger batches
```

**LSF Submission:**
```bash
# 8 GPUs = 8 workers
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh -n ray_gpu \
  -c "python batch_inference/batch_infer_vllm_actors.py" \
  -m 20000000000
```

**Expected throughput:** 50-100 prompts/second (depends on model and prompt length)

### Medium Models (7-13B parameters)

**Configuration:**
```yaml
model:
  tensor_parallel_size: 2  # 2 GPUs per actor
execution:
  num_workers: "auto"      # Auto-calculate based on available GPUs
  batch_size: 16
```

**LSF Submission:**
```bash
# 8 GPUs = 4 workers (2 GPUs each)
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh -n ray_gpu \
  -c "python batch_inference/batch_infer_vllm_actors.py" \
  -m 40000000000
```

### Large Models (70B+ parameters)

**Configuration:**
```yaml
model:
  tensor_parallel_size: 8  # 8 GPUs per actor
execution:
  num_workers: 1           # Single actor
  batch_size: 8            # Smaller batches
```

**LSF Submission:**
```bash
# 8 GPUs = 1 worker (8 GPUs for tensor parallelism)
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh -n ray_gpu \
  -c "python batch_inference/batch_infer_vllm_actors.py" \
  -m 80000000000
```

## Input Data Format

Create a JSONL file with one prompt per line:

```jsonl
{"text": "Explain quantum computing"}
{"text": "What is machine learning?"}
{"text": "Describe neural networks"}
```

See `dataset/README.md` for more details and examples.

## Output Format

Results are saved in JSONL format:

```jsonl
{
  "prompt": "Explain quantum computing",
  "generated_text": ["Quantum computing is..."],
  "finish_reason": ["stop"],
  "num_tokens": [156]
}
```

## Monitoring

### Ray Dashboard

Access the Ray dashboard to monitor cluster status:

1. Find the head node and dashboard port from job output:
   ```
   Starting ray head node on: node-01
   View the Ray dashboard at http://127.0.0.1:8265
   ```

2. Port forward from the head node:
   ```bash
   export PORT=8265
   export HEAD_NODE=node-01.your-domain.com
   ssh -L $PORT:localhost:$PORT -N -f -l $USER $HEAD_NODE
   ```

3. Access dashboard at: `http://127.0.0.1:8265`

### Progress Tracking

The scripts provide real-time progress updates:
- Number of prompts processed
- Throughput (prompts/second)
- Worker status
- Error messages

## Troubleshooting

### Issue: "Failed to connect to Ray cluster"

**Solution:** Make sure you're running the script via `ray_launch_cluster.sh`, not directly.

### Issue: "CUDA out of memory"

**Solutions:**
1. Reduce `batch_size` in config
2. Reduce `max_model_len`
3. Lower `gpu_memory_utilization` (e.g., 0.8 instead of 0.9)
4. Use quantization: `quantization: "awq"` or `"gptq"`

### Issue: "Model not found"

**Solutions:**
1. Check model name is correct
2. Ensure you have HuggingFace access token if needed:
   ```bash
   export HF_TOKEN=your_token_here
   ```
3. Use local model path if downloaded

### Issue: Slow inference

**Solutions:**
1. Increase `batch_size` for better GPU utilization
2. Use tensor parallelism for large models
3. Enable flash attention (automatically used if available)
4. Check GPU utilization with `nvidia-smi`

## Performance Tuning

### Batch Size

- **Too small:** Underutilizes GPU, low throughput
- **Too large:** May cause OOM errors
- **Optimal:** Experiment with 8, 16, 32, 64

### GPU Memory Utilization

```yaml
gpu_memory_utilization: 0.9  # Use 90% of GPU memory
```

- Higher values = more KV cache = better throughput
- Lower values = more headroom = fewer OOM errors

### Tensor Parallelism

For models that don't fit on a single GPU:

```yaml
tensor_parallel_size: 4  # Split model across 4 GPUs
```

Must be a power of 2: 1, 2, 4, 8

## Advanced Usage

### Custom Model Path

```bash
python batch_inference/batch_infer_vllm_actors.py \
  --model /path/to/local/model
```

### Override Configuration

```bash
python batch_inference/batch_infer_vllm_actors.py \
  --config my_config.yaml \
  --input my_prompts.jsonl \
  --output my_results.jsonl
```

### Use Ray Data Pipeline

```bash
python batch_inference/batch_infer_ray_data.py \
  --config config.yaml
```

## Comparison with Alternatives

### vs. KServe/TorchServe

**Batch Inference (this implementation):**
- ✅ Optimized for throughput
- ✅ Cost-effective for large batches
- ✅ Simple deployment
- ❌ Not for real-time serving

**KServe/TorchServe:**
- ✅ Real-time inference
- ✅ REST/gRPC APIs
- ❌ More complex setup
- ❌ Lower throughput for batches

### vs. Ray Serve

**Batch Inference:**
- ✅ Higher throughput for offline workloads
- ✅ Simpler for batch processing
- ✅ Better resource utilization

**Ray Serve:**
- ✅ Online serving with HTTP endpoints
- ✅ Auto-scaling
- ❌ More overhead for batch workloads

## Best Practices

1. **Start with CPU testing** using small models (gpt2)
2. **Use exclusive GPU access** (`j_exclusive=yes`) in production
3. **Monitor GPU utilization** to optimize batch size
4. **Enable quantization** for large models to save memory
5. **Use tensor parallelism** for models > 13B parameters
6. **Set appropriate timeouts** in LSF (`-W` flag)
7. **Save checkpoints** for long-running jobs
8. **Log everything** for debugging and monitoring

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Ray documentation: https://docs.ray.io/
3. Review vLLM documentation: https://docs.vllm.ai/
4. Check LSF documentation for your cluster

## License

See LICENSE file in the repository root.