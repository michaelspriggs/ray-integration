# Conda Environment for Ray on LSF

This directory contains a unified conda environment that works for both GPU and CPU workloads.

## Unified Ray 2.x Environment

**File:** `ray_2x.yml`

**Use for:**
- All Ray workloads (GPU and CPU)
- Development and production
- Batch inference, distributed training, hyperparameter tuning
- Both vLLM (GPU) and Transformers (CPU) inference

### Setup

```bash
# Create environment
conda env create -f sample_conda_env/ray_2x.yml

# Activate environment
conda activate ray

# Verify installation
python -c "import ray; print(f'Ray version: {ray.__version__}')"
python -c "import torch; print(f'PyTorch version: {torch.__version__}')"
python -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"
```

### Key Features

**Single environment for everything:**
- Python 3.11
- Ray 2.40.0 with Data support
- PyTorch 2.1.2 (GPU-enabled, works on CPU too)
- CUDA 11.8 toolkit
- vLLM 0.3.0 (for GPU inference)
- Transformers 4.38.0 (for CPU inference)
- All necessary dependencies

**Automatic backend selection:**
The workload code automatically detects whether to use:
- **vLLM** for GPU nodes (high performance)
- **Transformers** for CPU nodes (compatibility)

This is controlled by the `device` parameter in your config file.

### Key Packages

| Package | Version | Purpose |
|---------|---------|---------|
| Python | 3.11 | Runtime |
| Ray | 2.40.0 | Distributed computing |
| PyTorch | 2.1.2 | ML framework (GPU+CPU) |
| CUDA | 11.8 | GPU acceleration |
| vLLM | 0.3.0 | GPU inference engine |
| Transformers | 4.38.0 | CPU/GPU inference |
| Accelerate | 0.27.0 | Model optimization |

## Usage Examples

### GPU Workload
```bash
conda activate ray
cd reference_architectures/batch_inference
./submit_lsf.sh --config config/gpu_actors_single_host.yaml
```

### CPU Workload
```bash
conda activate ray
cd reference_architectures/batch_inference
./submit_lsf.sh --config config/cpu_ray_data_single_host.yaml
```

The same environment works for both!

## Testing Your Environment

### Quick Test
```bash
conda activate ray

# Test Ray
python -c "import ray; ray.init(); print('Ray is working!'); ray.shutdown()"

# Test PyTorch
python -c "import torch; print(f'PyTorch: {torch.__version__}'); print(f'CUDA: {torch.cuda.is_available()}')"

# Test vLLM (GPU only)
python -c "from vllm import LLM; print('vLLM is available!')"

# Test Transformers (CPU/GPU)
python -c "from transformers import AutoTokenizer; print('Transformers is working!')"
```

### Full Test Script
```python
import ray
import torch
from transformers import AutoTokenizer

print("=== Environment Test ===")
print(f"Ray version: {ray.__version__}")
print(f"PyTorch version: {torch.__version__}")
print(f"CUDA available: {torch.cuda.is_available()}")

# Test Ray
ray.init()
print(f"Ray cluster resources: {ray.cluster_resources()}")
ray.shutdown()

print("All tests passed!")
```

## Troubleshooting

### Issue: vLLM import fails on CPU nodes

**Error:**
```
ModuleNotFoundError: No module named 'vllm'
```

**Explanation:**
This is expected on CPU-only nodes. The workload code automatically falls back to Transformers for CPU inference.

**Solution:**
No action needed. The code handles this automatically based on the `device: "cpu"` config parameter.

### Issue: CUDA version mismatch

**Error:**
```
RuntimeError: CUDA error: no kernel image is available for execution on the device
```

**Solution:**
Your GPU may require a different CUDA version. Check your GPU's compute capability:
```bash
nvidia-smi --query-gpu=compute_cap --format=csv

# For newer GPUs (compute capability 8.0+), you may need CUDA 12.x
# Modify ray_2x.yml to use cudatoolkit=12.1
```

### Issue: Out of memory during environment creation

**Solution:**
Clear conda cache and try again:
```bash
conda clean --all
conda env create -f sample_conda_env/ray_2x.yml
```

## Environment Management

### List environments
```bash
conda env list
```

### Activate environment
```bash
conda activate ray
```

### Deactivate environment
```bash
conda deactivate
```

### Remove environment
```bash
conda env remove -n ray
```

### Update environment
```bash
conda env update -f sample_conda_env/ray_2x.yml --prune
```

### Export environment
```bash
conda activate ray
conda env export > my_environment.yml
```

## Migration from Separate Environments

If you previously used separate `ray_gpu` and `ray_cpu` environments:

1. **Remove old environments:**
   ```bash
   conda env remove -n ray_gpu
   conda env remove -n ray_cpu
   ```

2. **Create unified environment:**
   ```bash
   conda env create -f sample_conda_env/ray_2x.yml
   ```

3. **Update your workflow:**
   - Always use `conda activate ray`
   - No need to switch environments based on GPU/CPU
   - The workload code handles backend selection automatically

## Legacy Environments (Deprecated)

The following environment files are kept for reference but are deprecated:
- `ray_2x_gpu.yml` - Use `ray_2x.yml` instead
- `ray_2x_cpu.yml` - Use `ray_2x.yml` instead
- `sample_ray_env.yml` - Ray 1.x (legacy)

## Best Practices

1. **Use single environment:**
   - Simpler management
   - No environment switching
   - Consistent dependencies

2. **Let code handle backend selection:**
   - Set `device: "gpu"` or `device: "cpu"` in config
   - Workload automatically uses appropriate backend
   - vLLM for GPU, Transformers for CPU

3. **Version control:**
   - Commit environment file to git
   - Document any manual installation steps
   - Test environment creation regularly

4. **Resource allocation:**
   - Use `gpus_per_worker: 0` for CPU workloads
   - Use `gpus_per_worker: 1+` for GPU workloads
   - LSF will schedule appropriately

## Additional Resources

- **Conda Documentation**: https://docs.conda.io/
- **Ray Documentation**: https://docs.ray.io/
- **PyTorch Installation**: https://pytorch.org/get-started/locally/
- **vLLM Documentation**: https://docs.vllm.ai/
- **Transformers Documentation**: https://huggingface.co/docs/transformers/

## Support

For issues with environment setup:
1. Check the troubleshooting section above
2. Review the main README.md
3. Check Ray/PyTorch documentation
4. Open an issue in the repository