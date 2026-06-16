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

- `submit_lsf.sh` reads LSF resource settings from `config.yaml` and submits the LSF job
- `run.sh` is the LSF job command
- `run.sh` reads runtime settings from `config.yaml`, starts the Ray cluster, invokes the selected workload, and relies on `common/start_ray_cluster.sh` to tear the cluster down when the workload exits
- `lsf.workload_script` selects the workload entrypoint

This separation keeps scheduler concerns in `submit_lsf.sh` and runtime orchestration in `run.sh`.

## Configuration

Edit `config.yaml` to change:
- model name
- tensor parallel size
- worker count
- input path
- output path
- dtype
- LSF queue, node/task sizing, GPU count, walltime, memory, stdout log path, and workload script

The `lsf:` section is the single place to adapt this reference architecture to a new cluster environment.

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