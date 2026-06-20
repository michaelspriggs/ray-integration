# Sample Conda Environment

This directory contains a sample Conda environment for running the Ray + LSF reference workloads.

## Create the Environment

```bash
conda env create -f sample_conda_env/environment.yaml
```

## Activate the Environment

```bash
conda activate ray
```

## Usage

This environment can be used for both CPU and GPU workloads.

Example:

```bash
cd reference_architectures/batch_inference
./submit_lsf.sh --config config/gpu_actors_single_host.yaml
```

## Notes

- This environment is intended as a **starting point**.
- You may need to adjust package versions for your system (e.g. CUDA or PyTorch).
- For LSF jobs, the environment is activated inside `common/run.sh`.

## Summary

- One environment (`ray`) supports both CPU and GPU workloads
- Ready to use with the batch inference examples
- Users can customize as needed
