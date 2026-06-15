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

Submit with:

```bash
bsub < reference_architectures/batch_inference_ray_data/submit_lsf.sh
```

## Configuration

Edit `config.yaml` to change:
- model name
- tensor parallel size
- worker count
- input path
- output path
- dtype

## GPU compatibility

For Tesla T4-class GPUs, use:

```yaml
dtype: "half"
```

## Outputs

Results are written to the configured output path in `config.yaml`.

## Notes

This is currently the most complete reference architecture in the repository.