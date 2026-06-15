# Troubleshooting

This document captures common issues seen while running Ray workloads on LSF.

## Conda environment creation fails

### Symptoms
- dependency resolution errors
- package conflicts
- pip metadata generation failures

### Notes
The GPU environment may be sensitive to version alignment between:
- `torch`
- `torchvision`
- `torchaudio`
- `vllm`

Use the repository-provided environment files as the starting point.

## vLLM fails on Tesla T4 with bfloat16 errors

### Symptom
A failure similar to:

```text
Bfloat16 is only supported on GPUs with compute capability of at least 8.0.
```

### Resolution
Use `dtype: "half"` in the workload configuration for T4-class GPUs.

## Prompt loading fails with JSON decode errors

### Possible causes
- comments in JSONL files
- trailing blank lines
- malformed JSON objects

### Resolution
Use strict JSONL input and ensure loaders skip blank lines safely.

## Ray cluster starts but hostname resolution warnings appear

### Symptom
Messages involving missing `host` command or hostname lookup fallback.

### Resolution
This may be non-fatal if the startup script falls back correctly.
If needed, update the cluster startup logic to avoid depending on `host`.

## Output quality is poor even though the job succeeds

### Explanation
This is usually a workload or model-quality issue, not an LSF or Ray integration failure.

Possible causes:
- unsuitable model choice
- generation settings
- prompt mismatch with the selected model

## Job output file is missing while the job is still running

### Explanation
LSF may not write the final output file until the job completes.

### Useful commands
```bash
bpeek <jobid>
bjobs -l <jobid>
bhist -l <jobid>
```

## GPU jobs do not schedule

### Check
- queue name
- GPU resource syntax
- task count
- memory requests
- cluster host availability

Useful commands:
```bash
bhosts
lshosts
bjobs -l <jobid>