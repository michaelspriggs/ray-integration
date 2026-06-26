# Ray on LSF: Batch Inference Reference Architecture

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

Requires an operational LSF cluster with:

- Multi-node scheduling
- `blaunch` support
- CPU / memory / GPU resource allocation

**Required:**

- `res` daemon running on all nodes (for `blaunch`)

**Recommended:**

- Enable CPU affinity:

    AFFINITY=Y  (in lsb.hosts)

    This is required for `cpus_per_worker > 1`.

- Enable GPU enforcement:

    LSB_RESOURCE_ENFORCE="gpu"

---

### 2.2 Python / Ray Environment

Use the provided environment:

    sample_conda_env/

Example setup:

    conda env create -f sample_conda_env/environment.yaml
    conda activate ray

Ensure the environment is available on all nodes.

---

## 3. Quickstart

### Step 1 — Choose a Config

#### GPU Actors (default)

    lsf:
      num_workers: 4
      cpus_per_worker: 2
      gpus_per_worker: 1
      use_affinity: true

    execution:
      mode: actors
      device: gpu

    model:
      tensor_parallel_size: 1

**Resources:** 4 GPUs, 8 CPUs  
**Use for:** high throughput, independent requests

---

#### Multi-GPU Model (Ray Data)

    lsf:
      num_workers: 2
      cpus_per_worker: 2
      gpus_per_worker: 2

    execution:
      mode: ray_data
      device: gpu

    model:
      tensor_parallel_size: 2

**Resources:** 4 GPUs, 4 CPUs  
**Use for:** large models (TP > 1)

---

#### No-Affinity Mode

    lsf:
      num_workers: 2
      cpus_per_worker: 1
      gpus_per_worker: 1
      use_affinity: false

    execution:
      mode: actors
      device: gpu

**Resources:** 2 GPUs, 2 CPUs  

---

### Step 2 — Submit

    ./submit_lsf.sh --config path/to/config.yaml

Optional:

    ./submit_lsf.sh --config path/to/config.yaml --dry-run

---

### Step 3 — Monitor

    bjobs
    bpeek <job_id>

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

### Structure

    lsf:
    execution:
    model:
    data:

---

### LSF

    lsf:
      num_workers: 4
      cpus_per_worker: 2
      gpus_per_worker: 1
      memory_per_worker: "8GB"
      use_affinity: true

Rules:

- If `use_affinity=false`, then:

      cpus_per_worker = 1

---

### Execution

    execution:
      mode: actors | ray_data
      device: cpu | gpu
      batch_size: 32

---

### Model

    model:
      name: <model>
      tensor_parallel_size: N

Constraint:

    tensor_parallel_size = gpus_per_worker

---

### Data

    data:
      input_path: ...
      output_dir: ...

Supports:

- `{repo_root}`
- `{job_id}`

---

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
