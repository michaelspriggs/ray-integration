# Ray on LSF: Batch-Inference Reference Architecture

## 1. Introduction

This reference architecture demonstrates how to run **distributed batch inference on an LSF cluster using Ray**.

It bridges traditional HPC scheduling (LSF) with modern distributed execution (Ray), enabling scalable inference across multiple hosts and GPUs.

This repository provides:

- A **YAML-driven configuration model**
- Scripts to **submit and run jobs on LSF**
- Two execution modes:
  - **Actors (scale-out)**
  - **Ray Data (scale-up + scale-out)**

---

## 2. Prerequisites

### 2.1 LSF Cluster

Requires an operational LSF cluster, ideally with GPUs. Though it is possible to test CPU only inferencing, with a large slowdown. The `res` daemon should be running on all nodes of the cluster, to support launching processes with LSF's `blaunch` command.

There should be a shared `$HOME` directory that is accessible from all hosts of the cluster.

**Recommended:**

- Enable CPU affinity:
  
    `AFFINITY=Y`  (in `lsb.hosts`)
  
    This is required for `cpus_per_worker > 1`.

- Enable GPU enforcement, by setting in `lsf.conf`:

    `LSB_RESOURCE_ENFORCE="gpu"`

---

### 2.2 Python / Ray Environment

Use the provided environment to install a conda environment (as non-root user):
```
sample_conda_env/
```
Example setup:
```
conda env create -f sample_conda_env/environment.yaml
conda activate ray
```
Ensure the environment is available on all nodes.

---

## 3. Quickstart

### Step 1 — Choose a Configuration

The repository provides ready-to-use configs:

| Name                         | Config File                                      | CPUs | GPUs | Use Case |
|------------------------------|--------------------------------------------------|------|------|----------|
| CPU (Ray Data, single host)  | cpu_ray_data_single_host.yaml                    | 1    | 0    | CPU-only inference |
| GPU Actors (single host)     | gpu_actors_single_host.yaml                      | 4    | 1    | Baseline GPU |
| GPU Actors (multi-host)      | gpu_actors_multi_host.yaml                       | 8    | 2    | Distributed throughput |
| GPU Actors (interactive)     | gpu_actors_interactive.yaml                      | 4    | 2    | Debug / testing |
| GPU Ray Data (multi-host)    | gpu_ray_data_multi_host.yaml                     | 8    | 4    | Multi-GPU models |

---

If unsure, start with:

- **gpu_actors_multi_host.yaml** → general use  
- **gpu_ray_data_multi_host.yaml** → multi-GPU models  

---

### Step 2 — Submit

```
./submit_lsf.sh --config path/to/config.yaml
```

Optional:
```
./submit_lsf.sh --config path/to/config.yaml --dry-run
```

---

### Step 3 — Monitor

```
bjobs
bpeek <job_id>
```
---

## 4. Architecture

### Overview

    LSF → allocates resources
       ↓
    Ray → builds distributed cluster
       ↓
    Workload → runs inference

---

### LSF

- Allocates **tasks** across hosts
- Each task represents:

    CPUs = cpus_per_worker  
    GPUs = gpus_per_worker  

- Multi-host execution via `blaunch`

---

### Ray

- One Ray node per host
- Aggregates all CPUs/GPUs on that host
- Schedules actors or tasks based on resource requests

---

### Execution Flow

1. Job submitted via `bsub`
2. `run.sh` starts Ray cluster
3. Workers launched via `blaunch`
4. Workload executes
5. Results written to output

---

## 5. Execution Modes

Two modes are supported:

### Actors

- Persistent workers (1 GPU per actor)
- High-throughput inference
- Model loaded once per worker

Constraint:

    tensor_parallel_size = 1

---

### Ray Data

- Parallel batch processing
- Supports multi-GPU tasks

Requirement:

    tensor_parallel_size = gpus_per_worker

---

### Summary

| Mode      | Use Case                    |
|----------|----------------------------|
| actors   | throughput, small models    |
| ray_data | large models, TP > 1        |

---

## 6. Configuration Format

All workloads are configured via YAML.

### Structure

    lsf:
    execution:
    model:
    data:

---

### LSF

Defines how resources are allocated by LSF.

    lsf:
      num_workers: 4
      cpus_per_worker: 2
      gpus_per_worker: 1
      memory_per_worker: "8GB"
      use_affinity: true
      interactive: false

---

#### Fields

- `num_workers`  
  Number of LSF tasks (logical workers)

- `cpus_per_worker`  
  CPU cores per task  
  If `use_affinity=false`, must be 1

- `gpus_per_worker`  
  GPUs allocated per task

- `memory_per_worker`  
  Memory per task (LSF `rusage[mem=...]`)

- `use_affinity`  
  Enables CPU affinity (`affinity[core(N)]`)

- `interactive`  
  If true, runs with `bsub -Is` (interactive job)

---

---

### Execution

Defines how inference is executed.

    execution:
      mode: actors | ray_data
      device: cpu | gpu
      batch_size: 32

---

#### Fields

- `mode`  
  Execution model:
  - `actors` → scale-out workers  
  - `ray_data` → parallel data pipeline  

- `device`  
  `cpu` or `gpu`

- `batch_size`  
  Number of inputs processed per batch

---

---

### Model

Defines model behavior.

    model:
      name: <model>
      tensor_parallel_size: N

---

#### Fields

- `name`  
  Model identifier (e.g. Hugging Face or vLLM model)

- `tensor_parallel_size`  
  Number of GPUs per model instance

---

#### Constraint

    tensor_parallel_size = gpus_per_worker

---

---

### Data

Defines inputs and outputs.

    data:
      input_path: ...
      output_dir: ...

---

#### Fields

- `input_path`  
  Input dataset (JSONL)

- `output_dir`  
  Output location

---

Supports:

- `{repo_root}`
- `{job_id}`

---

### Key Rules

- CPU affinity disabled → `cpus_per_worker = 1`
- GPU usage → `tensor_parallel_size = gpus_per_worker`
- Actors mode → `tensor_parallel_size = 1`

## 7. Running Workloads

Submit:

    ./submit_lsf.sh --config config.yaml

Dry-run:

    ./submit_lsf.sh --config config.yaml --dry-run

Stop job:

    bkill <job_id>

---

### Runtime Behavior

- Ray cluster starts across hosts
- GPUs assigned automatically
- Workloads scheduled based on config

---

## 8. Troubleshooting

### Ray cluster not forming

- Check `res` daemon
- Ensure multi-node allocation

---

### GPU not detected

- Verify:

      gpus_per_worker > 0
      LSB_RESOURCE_ENFORCE="gpu"

---

### Config errors

- Check:

      tensor_parallel_size = gpus_per_worker
      cpus_per_worker = 1 when no affinity

---

### General issues

- Inspect logs:

      bpeek <job_id>

---
