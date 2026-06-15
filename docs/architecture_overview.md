# Architecture Overview

This repository is organized to separate reusable Ray-on-LSF platform logic from workload-specific reference implementations.

## Core Layers

### `common/`
Shared platform assets used by multiple workloads:
- cluster startup
- cluster shutdown
- shared Python helpers

### `reference_architectures/`
Self-contained workload examples intended to be copied, adapted, and extended.

Each architecture should include:
- `README.md`
- `config.yaml`
- `submit_lsf.sh`
- a workload entrypoint

### `docs/`
Cross-cutting documentation for:
- setup
- LSF behavior
- troubleshooting
- repository structure

### `legacy/`
Historical Ray 1.x content preserved for reference, but not recommended for new usage.

### `examples/`
Small sanity checks and minimal validation scripts.

## Design Goals

### Platform vs workload separation
Cluster lifecycle logic belongs in `common/`.
Workload logic belongs in `reference_architectures/`.

### Consistent user experience
Architectures should be runnable with a predictable submission flow:

```bash
bsub < submit_lsf.sh
```

### Copy-paste friendliness
Each architecture should be understandable and portable without requiring deep knowledge of the rest of the repository.

### Scalable growth
New architectures can be added without restructuring the repository.

## Current Reference Architectures

- `batch_inference_ray_data`
- `distributed_training`
- `hyperparameter_tuning`
- `data_pipeline`
- `hybrid_cpu_gpu_pipeline`

Some are currently placeholders intended to establish the target repository shape.

## Migration Notes

The repository currently retains `sample_conda_env/` as a transitional compatibility area for environment setup assets.

Legacy Ray 1.x examples have been moved under `legacy/ray1/`.