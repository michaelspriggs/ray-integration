# Getting Started

This guide walks through the recommended first-use path for the repository.

## 1. Create an environment

The repository currently keeps environment definitions in `sample_conda_env/`.

### GPU environment
```bash
conda env create -f sample_conda_env/ray_2x_gpu.yml
conda activate ray_gpu
```

### CPU environment
```bash
conda env create -f sample_conda_env/ray_2x_cpu.yml
conda activate ray_cpu
```

See `sample_conda_env/README.md` for more details.

## 2. Review the recommended architecture

Start with:

- `reference_architectures/batch_inference/`

This is the most complete reference architecture currently in the repository.

## 3. Review the configuration

Inspect:

- `reference_architectures/batch_inference/config.yaml`

Adjust:
- model name
- input path
- output path
- worker count
- tensor parallel settings

## 4. Submit to LSF

From the architecture directory or repository root:

```bash
bsub < reference_architectures/batch_inference/submit_lsf.sh
```

## 5. Inspect outputs

Typical outputs include:
- LSF job logs such as `output.<jobid>`
- workload outputs written to the configured output path

## 6. Validate Ray independently

For a minimal sanity check, use:

- `examples/quick_tests/minimal_ray_test.py`

## Recommended learning order

1. `docs/architecture_overview.md`
2. `docs/lsf_integration.md`
3. `reference_architectures/batch_inference/README.md`
4. `docs/troubleshooting.md`

## Notes

- Some reference architectures are placeholders intended to establish the target repository structure.
- Legacy Ray 1.x content is preserved under `legacy/ray1/` and is not part of the recommended quick start path.
