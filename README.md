# ray-on-lsf

Reference architectures and platform utilities for running Ray on IBM Spectrum LSF.

This repository is organized around a clear separation between:

- **platform assets** in `common/`
- **reference workloads** in `reference_architectures/`
- **supporting documentation** in `docs/`
- **legacy Ray 1.x content** in `legacy/`
- **small validation examples** in `examples/`

## Repository Layout

```text
ray-on-lsf/
├── README.md
├── docs/
├── common/
├── reference_architectures/
├── legacy/
└── examples/
```

## Quick Navigation

### Documentation
- [Architecture Overview](docs/architecture_overview.md)
- [Getting Started](docs/getting_started.md)
- [LSF Integration](docs/lsf_integration.md)
- [Troubleshooting](docs/troubleshooting.md)

### Platform Utilities
- [`common/start_ray_cluster.sh`](common/start_ray_cluster.sh)
- [`common/stop_ray_cluster.sh`](common/stop_ray_cluster.sh)
- [`common/utils.py`](common/utils.py)

### Reference Architectures
- [`reference_architectures/batch_inference_ray_data/`](reference_architectures/batch_inference_ray_data/)
- [`reference_architectures/distributed_training/`](reference_architectures/distributed_training/)
- [`reference_architectures/hyperparameter_tuning/`](reference_architectures/hyperparameter_tuning/)
- [`reference_architectures/data_pipeline/`](reference_architectures/data_pipeline/)
- [`reference_architectures/hybrid_cpu_gpu_pipeline/`](reference_architectures/hybrid_cpu_gpu_pipeline/)

### Legacy Content
- [`legacy/ray1/`](legacy/ray1/)

### Quick Tests
- [`examples/quick_tests/minimal_ray_test.py`](examples/quick_tests/minimal_ray_test.py)

## Design Principles

### 1. Copy-paste friendly
Each reference architecture is intended to be:
- self-contained
- easy to copy into another repo
- configurable with minimal edits

### 2. Consistent UX
Each architecture should run with a predictable flow:

```bash
bsub < submit_lsf.sh
```

### 3. Scalable growth
New architectures can be added under `reference_architectures/` without changing the platform layer.

### 4. Platform vs workload separation
Cluster startup, shutdown, and shared helpers live in `common/`.
Workload-specific logic lives in `reference_architectures/`.

## Current Recommended Starting Point

For a working end-to-end example, start with:

- [`reference_architectures/batch_inference_ray_data/`](reference_architectures/batch_inference_ray_data/)

Then follow:

- [docs/getting_started.md](docs/getting_started.md)

## Environment Notes

The repository currently retains the existing `sample_conda_env/` directory for environment setup compatibility during migration. See:

- [`sample_conda_env/README.md`](sample_conda_env/README.md)

## Legacy Note

Older Ray 1.x examples and historical scripts have been moved under `legacy/ray1/` and are not part of the recommended quick start path.

## License

See [LICENSE](LICENSE).
