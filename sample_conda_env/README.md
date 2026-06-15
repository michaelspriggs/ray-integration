# Conda Environments for Ray on LSF

This directory contains conda environment files for different use cases.

## Available Environments

### 1. Ray 2.x CPU (Development/Testing)

**File:** `ray_2x_cpu.yml`

**Use for:**
- Development and testing
- CPU-only workloads
- Small models (e.g., gpt2)
- Debugging

**Setup:**

**Option A: Using the setup script (Recommended)**
```bash
cd sample_conda_env
./setup_cpu_env.sh
```

**Option B: Manual setup**
```bash
# Create environment
conda env create -f sample_conda_env/ray_2x_cpu.yml

# Activate environment
conda activate ray_cpu

# Install PyTorch CPU version (required step)
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu

# Verify installation
python -c "import ray; print(f'Ray version: {ray.__version__}')"
python -c "import torch; print(f'PyTorch version: {torch.__version__}')"
```

**Key packages:**
- Python 3.11
- Ray 2.40.0
- PyTorch 2.2.0 (CPU)
- Transformers, Accelerate

**Note:** vLLM is not included in the CPU environment as it has limited CPU support. For CPU testing, use transformers directly or small models.

### 2. Ray 2.x GPU (Production)

**File:** `ray_2x_gpu.yml`

**Use for:**
- Production workloads
- GPU-accelerated inference
- Large language models
- vLLM inference

**Setup:**
```bash
# Create environment
conda env create -f sample_conda_env/ray_2x_gpu.yml

# Activate environment
conda activate ray_gpu

# Verify installation
python -c "import ray; print(f'Ray version: {ray.__version__}')"
python -c "import torch; print(f'PyTorch version: {torch.__version__}')"
python -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"
```

**Key packages:**
- Python 3.11
- Ray 2.40.0
- PyTorch 2.1.2 (GPU)
- CUDA 11.8
- vLLM 0.3.0
- Flash Attention is installed transitively by vLLM when compatible

### 3. Ray 1.x (Legacy)

**File:** `sample_ray_env.yml`

**Use for:**
- Backward compatibility
- Existing Ray 1.x code

**Setup:**
```bash
conda env create -f sample_conda_env/sample_ray_env.yml
conda activate ray
```

**Note:** This environment is provided for backward compatibility only. New projects should use Ray 2.x environments.

## Troubleshooting

### Issue: PyTorch CPU installation fails

**Error:**
```
ERROR: Could not find a version that satisfies the requirement torch==2.2.0+cpu
```

**Solution:**
The `+cpu` suffix cannot be used directly in conda environment files. Install PyTorch CPU separately:
```bash
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
```

Or use the provided setup script:
```bash
cd sample_conda_env
./setup_cpu_env.sh
```

### Issue: vLLM not available in CPU environment

**Explanation:**
vLLM has limited CPU support and is not included in the CPU environment. For CPU testing:

1. Use transformers directly:
```python
from transformers import AutoModelForCausalLM, AutoTokenizer

model = AutoModelForCausalLM.from_pretrained("gpt2")
tokenizer = AutoTokenizer.from_pretrained("gpt2")
```

2. Or use small models that work well on CPU

### Issue: GPU environment creation fails with dependency conflicts

**Symptoms:**
```bash
ModuleNotFoundError: No module named 'torch'
```

or

```bash
ERROR: Cannot install ... because these package versions have conflicting dependencies
```

**Explanation:**
`vllm==0.3.0` requires `torch==2.1.2`. If the environment pins newer PyTorch versions, pip resolution fails. Also, explicitly pinning `flash-attn` can trigger metadata generation before `torch` is installed.

**Solution:**
Use the repo's updated `ray_2x_gpu.yml`, which aligns PyTorch with `vllm==0.3.0` and lets vLLM manage compatible flash-attention dependencies transitively.

### Issue: CUDA version mismatch

**Error:**
```
RuntimeError: CUDA error: no kernel image is available for execution on the device
```

**Solution:**
Your GPU may require a different CUDA version. Check your GPU's compute capability and install the appropriate CUDA toolkit:
```bash
# Check GPU compute capability
nvidia-smi --query-gpu=compute_cap --format=csv

# For newer GPUs (compute capability 8.0+), you may need CUDA 12.x
# Modify ray_2x_gpu.yml to use cudatoolkit=12.1
```

### Issue: Out of memory during environment creation

**Solution:**
Clear conda cache and try again:
```bash
conda clean --all
conda env create -f sample_conda_env/ray_2x_cpu.yml
```

## Environment Management

### List environments
```bash
conda env list
```

### Activate environment
```bash
conda activate ray_cpu  # or ray_gpu
```

### Deactivate environment
```bash
conda deactivate
```

### Remove environment
```bash
conda env remove -n ray_cpu  # or ray_gpu
```

### Update environment
```bash
conda env update -f sample_conda_env/ray_2x_cpu.yml --prune
```

### Export environment
```bash
conda activate ray_cpu
conda env export > my_environment.yml
```

## Testing Your Environment

### Quick Test
```bash
conda activate ray_cpu  # or ray_gpu

# Test Ray
python -c "import ray; ray.init(); print('Ray is working!'); ray.shutdown()"

# Test PyTorch
python -c "import torch; print(f'PyTorch: {torch.__version__}'); print(f'CUDA: {torch.cuda.is_available()}')"

# Test Transformers
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

## Comparison

| Feature | CPU Environment | GPU Environment | Legacy (Ray 1.x) |
|---------|----------------|-----------------|------------------|
| Python | 3.11 | 3.11 | 3.7 |
| Ray | 2.40.0 | 2.40.0 | 1.3.0 |
| PyTorch | 2.2.0 (CPU) | 2.1.2 (GPU) | 1.8.1 |
| CUDA | N/A | 11.8 | 10.2 |
| vLLM | ❌ | ✅ | ❌ |
| Use Case | Testing | Production | Legacy |

## Best Practices

1. **Use CPU environment for development:**
   - Faster iteration
   - No GPU required
   - Test with small models (gpt2)

2. **Use GPU environment for production:**
   - Better performance
   - Support for large models
   - vLLM optimization

3. **Keep environments separate:**
   - Don't mix CPU and GPU packages
   - Use different environment names
   - Document which environment is used

4. **Version control:**
   - Commit environment files to git
   - Document any manual installation steps
   - Test environment creation regularly

## Additional Resources

- **Conda Documentation**: https://docs.conda.io/
- **Ray Documentation**: https://docs.ray.io/
- **PyTorch Installation**: https://pytorch.org/get-started/locally/
- **vLLM Documentation**: https://docs.vllm.ai/

## Support

For issues with environment setup:
1. Check the troubleshooting section above
2. Review the main README.md
3. Check Ray/PyTorch documentation
4. Open an issue in the repository