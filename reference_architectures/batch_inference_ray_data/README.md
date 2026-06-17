# Batch Inference with Ray on LSF

This reference architecture demonstrates distributed batch inference on LSF using Ray.

## Included implementations

- `workload.py` — Ray Data-oriented implementation
- `workload_actors.py` — actor-based vLLM implementation

## Files

- `config.yaml`
- `submit_lsf.sh`
- `workload.py`
- `workload_actors.py`
- `dataset/sample_prompts.jsonl`

## Recommended usage

Activate the desired conda environment first, then submit:

```bash
conda activate ray_env
bash reference_architectures/batch_inference_ray_data/submit_lsf.sh
```

The submission script reads `config.yaml` and submits the job with `bsub`. Users should only need to edit `reference_architectures/batch_inference_ray_data/config.yaml` for their environment.

`run.sh` expects to be launched from an environment where `ray` and the workload dependencies are already available on `PATH`.

The submission flow is intentionally split by responsibility:

- `submit_lsf.sh` reads optional LSF resource settings from `config.yaml` and submits the LSF job
- `run.sh` is the LSF job command
- `run.sh` reads runtime settings from `config.yaml`, selects the workload from `execution.mode`, starts the Ray cluster, invokes the selected workload, and relies on `common/start_ray_cluster.sh` to tear the cluster down when the workload exits

This separation keeps scheduler concerns in `submit_lsf.sh` and runtime orchestration in `run.sh`.

## Configuration

Edit `config.yaml` to change:
- model name
- tensor parallel size
- execution mode (`ray_data` or `actors`)
- device selection (`gpu` or `cpu`)
- worker count
- CPUs per worker (enforced by Ray, not LSF)
- Ray object store memory
- input path
- output path
- dtype
- LSF queue, worker count, GPUs per worker, memory per worker, single-host restriction, and stdout log path

Use the `execution:` section for workload behavior, the `ray:` section for Ray runtime settings, and the `lsf:` section for scheduler settings.

The `lsf:` section is optional. If no LSF options are specified, `submit_lsf.sh` falls back to:

```bash
bsub "${SCRIPT_DIR}/run.sh"
```

When `lsf.restrict_to_single_host` is set to `true`, `submit_lsf.sh` adds:

```bash
-R "span[hosts=1]"
```

## GPU compatibility

For Tesla T4-class GPUs, use:

```yaml
dtype: "half"
```

## Outputs

Results and LSF stdout are written to the configured paths in `config.yaml`, currently:

- inference results: `reference_architectures/batch_inference_ray_data/output/results.%J.jsonl`
- LSF stdout: `reference_architectures/batch_inference_ray_data/output/output.%J`

## Notes

This is currently the most complete reference architecture in the repository.