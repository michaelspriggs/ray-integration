# LSF Integration

This document describes how the repository expects to interact with IBM Spectrum LSF.

## Submission model

Reference architectures are designed to submit through:

```bash
bsub < submit_lsf.sh
```

Each architecture owns its own submission script so resource requests remain local to the workload.

## Ray cluster lifecycle

The shared platform layer provides:

- `common/start_ray_cluster.sh`
- `common/stop_ray_cluster.sh`

The start script is responsible for:
- reading LSF allocation metadata
- selecting a Ray head node
- starting Ray on the head node
- joining worker nodes to the cluster

## Common LSF concepts

### `#BSUB -n`
Number of LSF tasks or slots requested.

### `#BSUB -gpu "num=1/task:j_exclusive=yes"`
Requests one GPU per task and prevents GPU sharing.

### `#BSUB -q`
Queue selection.

### `#BSUB -M`
Memory limit.

### `#BSUB -R "rusage[mem=...]" `
Memory reservation.

### `#BSUB -W`
Wall clock limit.

## GPU visibility

LSF typically sets `CUDA_VISIBLE_DEVICES` for GPU-isolated jobs.
The Ray startup layer and workloads should rely on that rather than hard-coding device IDs.

## Multi-node behavior

For multi-node jobs:
- one host becomes the Ray head
- remaining hosts join as workers
- workloads connect with `ray.init(address="auto")`

## Recommended validation commands

Cluster-side commands often used during debugging:

```bash
lsid
bhosts
lshosts
bjobs -l <jobid>
bhist -l <jobid>
bpeek <jobid>
```

## Repository conventions

- platform scripts live in `common/`
- workload submission scripts live beside each architecture
- workload code should not embed cluster-specific assumptions unless documented

## Known environment-specific caveats

Some clusters may not have the `host` utility installed.
The shared startup script should tolerate that and fall back safely.