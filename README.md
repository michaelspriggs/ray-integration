# Ray on LSF

Ray provides a simple, universal API for building distributed applications. Read more about Ray [here](https://docs.ray.io/).

This repository demonstrates how to deploy **Ray 2.x** on LSF for AI workloads, including:
- Distributed training
- **Batch inference with vLLM** (NEW)
- Flexible GPU allocation
- Production-ready patterns

## 🆕 What's New (2026 Update)

This repository has been modernized with:
- ✅ **Ray 2.40+** (upgraded from Ray 1.x)
- ✅ **Python 3.11** (upgraded from Python 3.7)
- ✅ **vLLM integration** for efficient LLM batch inference
- ✅ **Simplified LSF patterns** (CPU-only and GPU with exclusive access)
- ✅ **Flexible GPU allocation** (works with heterogeneous clusters)
- ✅ **Production-ready reference architecture**

## Quick Start

### 1. Setup Environment

Choose the appropriate environment for your use case:

**For CPU-only testing (Recommended for development):**
```bash
# Option A: Use the setup script (handles PyTorch CPU installation)
cd sample_conda_env
./setup_cpu_env.sh

# Option B: Manual setup
conda env create -f sample_conda_env/ray_2x_cpu.yml
conda activate ray_cpu
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
```

**For GPU inference (Production):**
```bash
conda env create -f sample_conda_env/ray_2x_gpu.yml
conda activate ray_gpu
```

**Legacy environment (Ray 1.x):**
```bash
conda env create -f sample_conda_env/sample_ray_env.yml
conda activate ray
```

**Note:** See [sample_conda_env/README.md](sample_conda_env/README.md) for detailed setup instructions and troubleshooting.

### 2. Verify Installation

```bash
conda activate ray_gpu  # or ray_cpu
python -c "import ray; print(f'Ray version: {ray.__version__}')"
# Expected output: Ray version: 2.40.0
```

## 🚀 Batch Inference with vLLM (NEW)

The `batch_inference/` directory contains a production-ready reference implementation for distributed LLM inference.

### Quick Example - CPU Testing

```bash
bsub -n 4 -o output.%J \
 ./ray_launch_cluster.sh \
 -n ray_cpu \
 -c "python batch_inference/batch_infer_vllm_actors.py --cpu-only --model gpt2" \
 -m 10000000000
```

### Quick Example - GPU Inference

```bash
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
 ./ray_launch_cluster.sh \
 -n ray_gpu \
 -c "python batch_inference/batch_infer_vllm_actors.py" \
 -m 20000000000
```

**See [batch_inference/README.md](batch_inference/README.md) for complete documentation.**

## Standard LSF Submission Patterns

### Pattern 1: CPU-Only (Development/Testing)

```bash
bsub -n 8 -o output.%J \
 ./ray_launch_cluster.sh \
 -n ray_cpu \
 -c "python your_workload.py" \
 -m 20000000000
```

**Use for:**
- Development and testing
- Small models (e.g., gpt2)
- Debugging

### Pattern 2: GPU with Exclusive Access (Production)

```bash
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
 ./ray_launch_cluster.sh \
 -n ray_gpu \
 -c "python your_workload.py" \
 -m 20000000000
```

**Use for:**
- Production workloads
- GPU-accelerated inference
- Large language models

**Key features:**
- `num=1/task`: One GPU per task
- `j_exclusive=yes`: Exclusive GPU access (no sharing)
- LSF automatically sets `CUDA_VISIBLE_DEVICES`
- Ray auto-detects GPUs from environment

### Optional LSF Parameters

Customize resource requirements:

```bash
bsub -n 8 \
 -gpu "num=1/task:j_exclusive=yes" \
 -q gpu_queue \              # Specify queue
 -M 100GB \                  # Memory limit
 -W 2:00 \                   # Wall time (hours:minutes)
 -R "rusage[mem=10GB]" \     # Memory reservation per task
 -o output.%J \
 ./ray_launch_cluster.sh -n ray_gpu -c "python your_workload.py" -m 20000000000
```

## Configuring Conda (Legacy)

For the legacy Ray 1.x environment:

```bash
conda env create -f sample_conda_env/sample_ray_env.yml
conda activate ray
ray --version
# Output: ray, version 1.4.0
```

**Note:** The legacy environment is provided for backward compatibility. New projects should use Ray 2.x environments.
## Sample Workloads

The `sample_workload/` directory contains example workloads:

- **`sample_code_for_ray.py`**: Simple CPU-only Ray workload
- **`cifar_pytorch_example.py`**: PyTorch training example (CPU and GPU)

**Note:** These examples use Ray 1.x APIs. For Ray 2.x examples, see the `batch_inference/` directory.

### Running Sample Workloads

**CPU-only example:**
```bash
bsub -n 4 -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_cpu \
  -c "python sample_workload/sample_code_for_ray.py" \
  -m 10000000000
```

**GPU training example (legacy):**
```bash
bsub -n 4 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray \
  -c "python sample_workload/cifar_pytorch_example.py --use-gpu --num-workers 4 --num_epochs 5" \
  -m 20000000000
```

## Accessing the Ray Dashboard

The Ray dashboard provides real-time monitoring of your cluster.

### 1. Find Dashboard Information

Check the job output for dashboard details:
```
Starting ray head node on: node-01
View the Ray dashboard at http://127.0.0.1:8265
```

### 2. Port Forward from Head Node

```bash
export PORT=8265
export HEAD_NODE=node-01.your-domain.com
ssh -L $PORT:localhost:$PORT -N -f -l $USER $HEAD_NODE
```

### 3. Access Dashboard

Open in your browser: `http://127.0.0.1:8265`

## Migration from Ray 1.x to Ray 2.x

If you have existing Ray 1.x code, here are the key changes:

### API Changes

| Ray 1.x | Ray 2.x |
|---------|---------|
| `ray.util.sgd.torch.TorchTrainer` | `ray.train.torch.TorchTrainer` |
| `TrainingOperator` | `train_loop_per_worker` function |
| `ray.util.sgd.utils` | `ray.train` |
| `ray.get_runtime_context().node_id` | `ray.get_runtime_context().get_node_id()` |

### Environment Changes

- Python 3.7 → Python 3.10/3.11
- PyTorch 1.8 → PyTorch 2.x
- CUDA 10.2 → CUDA 11.8+

See the [Ray 2.x migration guide](https://docs.ray.io/en/latest/ray-overview/migration-guide.html) for complete details.

## Repository Structure

```
ray-integration/
├── batch_inference/              # NEW: vLLM batch inference reference
│   ├── README.md                 # Complete documentation
│   ├── architecture.md           # Design rationale
│   ├── config.yaml               # Configuration template
│   ├── batch_infer_vllm_actors.py
│   ├── batch_infer_ray_data.py
│   ├── dataset/                  # Sample data
│   └── run_batch_inference.sh
├── sample_conda_env/
│   ├── ray_2x_cpu.yml           # NEW: Ray 2.x CPU environment
│   ├── ray_2x_gpu.yml           # NEW: Ray 2.x GPU environment
│   └── sample_ray_env.yml       # Legacy: Ray 1.x environment
├── sample_workload/
│   ├── cifar_pytorch_example.py # Legacy: Ray 1.x training
│   └── sample_code_for_ray.py   # Legacy: Ray 1.x example
├── ray_launch_cluster.sh        # Updated for Ray 2.x
└── README.md                     # This file
```

## Resources

- **Ray Documentation**: https://docs.ray.io/
- **vLLM Documentation**: https://docs.vllm.ai/
- **LSF Documentation**: Check your cluster's documentation
- **Batch Inference Guide**: See [batch_inference/README.md](batch_inference/README.md)
- **Architecture Details**: See [batch_inference/architecture.md](batch_inference/architecture.md)

## Contributing

See [IBMDCO.md](IBMDCO.md) for contribution guidelines.

## License

See [LICENSE](LICENSE) file for details.
