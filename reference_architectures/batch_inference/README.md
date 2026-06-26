# Ray on LSF: Batch Inference Reference Architectures

## 1. Introduction

This reference architecture demonstrates how to perform **scalable batch inference on an LSF-managed cluster using Ray**. It is designed to bridge the gap between traditional HPC schedulers (LSF) and modern distributed execution frameworks (Ray), enabling users to efficiently run large inference workloads across multiple hosts.

The goal of this project is to provide a **clean, reproducible template** for deploying inference workloads that:

- Scale across **multiple nodes and GPUs**
- Support both **high-throughput (scale-out)** and **large-model (scale-up)** execution patterns
- Integrate naturally with **LSF resource allocation**
- Leverage Ray for **distributed scheduling and orchestration**

---

### Key Concepts

This architecture follows a simple and well-defined separation of responsibilities:

- **LSF**: Allocates compute resources (CPUs, GPUs, memory) across the cluster  
- **Ray**: Builds a distributed execution layer on top of those resources  
- **Workloads**: Define how inference is performed (actors or Ray Data)

---

### What This Reference Provides

This repository includes:

- Predefined **batch inference workloads**
- A flexible **YAML-based configuration system**
- Scripts to **submit and run jobs on LSF**
- Support for both:
  - **Actor-based execution** (many independent workers)
  - **Ray Data pipelines** (parallel batch processing)

---

### When to Use This Architecture

This approach is particularly useful when:

- Running **large-scale inference jobs** over datasets  
- Deploying **LLMs or transformer models** across multiple GPUs  
- Moving from **single-node inference** to distributed execution  
- Integrating Ray into an **existing LSF-based HPC environment**  

---

### Design Philosophy

The architecture is built around a few core principles:

- **Separation of concerns**: LSF handles allocation, Ray handles execution  
- **Configurability**: All behavior is driven via YAML configuration  
- **Portability**: Works across clusters with different capabilities (with or without CPU affinity)  
- **Scalability**: Supports both horizontal and vertical scaling  

---

This document walks through how to configure, run, and understand batch inference workloads using this model, starting with prerequisites and progressing through execution details.

## 2. Prerequisites

## 2. Prerequisites

Before running batch inference workloads, ensure your environment satisfies the following requirements.

---

### 2.1 LSF Cluster Environment

You must have access to an operational **LSF (Load Sharing Facility) cluster**.

This architecture uses:

- **LSF** for resource allocation  
- **blaunch** for launching processes across hosts  
- **Ray** for distributed execution  

---

#### ✅ Required Capabilities

The LSF environment must support:

- Job submission via `bsub`
- Multi-node job allocation
- Remote execution via `blaunch`
- CPU and memory scheduling
- GPU scheduling (for GPU workloads)

---

#### ✅ `res` Daemon (Required for `blaunch`)

To support multi-node execution, the LSF **RES daemon (`res`) must be running on all compute nodes**.

This enables:

- Remote process execution
- Distributed job startup

---

#### ✅ CPU Affinity (Recommended)

CPU affinity ensures that each worker receives a well-defined set of cores.

Enable affinity by setting:

    AFFINITY=Y

in the `lsb.hosts` configuration:

    Begin Host
    HOST_NAME MXJ   r1m     pg    ls    tmp  DISPATCH_WINDOW  AFFINITY
    default    !    ()      ()    ()     ()     ()            (Y)
    End Host

---

##### Behavior

| use_affinity | cpus_per_worker |
|-------------|----------------|
| true        | ≥ 1            |
| false       | must be 1      |

---

> Without affinity, CPU locality is not guaranteed.

---

#### ✅ GPU Resource Enforcement (Recommended)

To ensure correct GPU allocation and isolation:

    LSB_RESOURCE_ENFORCE="gpu"

This ensures:

- Jobs only use assigned GPUs
- Proper `CUDA_VISIBLE_DEVICES` assignment
- No GPU contention between tasks

---

---

### 2.2 Python / Ray Environment

A consistent Python environment must be available on all nodes.

This repository provides a sample environment in:

    sample_conda_env/

---

#### ✅ Setup

Create and activate the environment:

    conda env create -f sample_conda_env/environment.yaml
    conda activate ray

---

#### ✅ Requirements

Ensure:

- The environment is available on all compute nodes  
- The same environment is used across all hosts  
- `ray` is available in `PATH`  

---

#### ✅ Included Dependencies

Core:

    ray
    pyyaml
    tqdm

Optional (depending on workload):

    vllm          # GPU inference
    transformers  # Hugging Face models
    torch         # backend

---

#### ⚠️ Notes

- GPU workloads require compatible CUDA and drivers  
- vLLM requires GPU-enabled nodes  
- Environment inconsistencies can cause runtime failures  

---

With these prerequisites satisfied, you are ready to run distributed batch inference workloads.

## 3. Quickstart

This section provides a minimal set of working examples to help you get started quickly. Choose an example based on your workload, submit the job, and verify that it runs successfully.

---

### Step 1 — Choose a Configuration

Below are common example configurations for typical use cases.

---

#### 🔹 Example 1: GPU Actors (Recommended Default)

High-throughput inference using one GPU per worker.

    lsf:
      num_workers: 4
      cpus_per_worker: 2
      gpus_per_worker: 1
      use_affinity: true

    execution:
      mode: actors
      device: gpu

    model:
      name: facebook/opt-1.3b
      tensor_parallel_size: 1

---

**What this does:**

- Launches 4 workers across the cluster  
- Each worker loads the model independently  
- Maximizes throughput for many small requests  

**Required resources:**

- 4 GPUs  
- 8 CPUs total  

---

#### 🔹 Example 2: Multi-GPU Model (Ray Data)

Run a larger model using multiple GPUs per task.

    lsf:
      num_workers: 2
      cpus_per_worker: 2
      gpus_per_worker: 2
      use_affinity: true

    execution:
      mode: ray_data
      device: gpu

    model:
      name: facebook/opt-1.3b
      tensor_parallel_size: 2

---

**What this does:**

- Each worker uses 2 GPUs  
- Enables tensor parallelism for larger models  
- Suitable for memory-constrained workloads  

**Required resources:**

- 4 GPUs total  
- 4 CPUs total  

---

#### 🔹 Example 3: CPU-only Inference

Run inference without GPUs.

    lsf:
      num_workers: 2
      cpus_per_worker: 1
      gpus_per_worker: 0
      use_affinity: false

    execution:
      mode: actors
      device: cpu

    model:
      name: facebook/opt-1.3b
      tensor_parallel_size: 1

---

**What this does:**

- Runs inference entirely on CPU  
- Useful for environments without GPU access  

**Required resources:**

- 2 CPUs  

---

#### 🔹 Example 4: No Affinity (Portable Mode)

For clusters where CPU affinity is not enabled.

    lsf:
      num_workers: 2
      cpus_per_worker: 1
      gpus_per_worker: 1
      use_affinity: false

    execution:
      mode: actors
      device: gpu

    model:
      tensor_parallel_size: 1

---

**What this does:**

- Works on clusters without CPU affinity support  
- Uses minimal CPU allocation per worker  

**Required resources:**

- 2 GPUs  
- 2 CPUs  

---

### Step 2 — Submit the Job

Run:

    ./submit_lsf.sh --config path/to/config.yaml

---

### Step 3 — (Optional) Preview Before Submitting

To validate the configuration without submitting:

    ./submit_lsf.sh --config path/to/config.yaml --dry-run

---

### Step 4 — Monitor the Job

Use standard LSF commands:

    bjobs
    bpeek <job_id>

---

### Step 5 — Verify Output

During execution, the system will:

- Start a Ray cluster across allocated hosts  
- Initialize workers  
- Run inference tasks  
- Write outputs to the configured output directory  

---

### ✅ Next Steps

Once you have successfully run a basic example:

- Modify configurations for your workload  
- Explore execution modes (Actors vs Ray Data)  
- Tune resource parameters for performance  

---

If you are unsure which configuration to use:

- Use **Actors mode** for most workloads  
- Use **Ray Data mode** for multi-GPU models (TP > 1)  

## 4. Architecture

This reference architecture combines **LSF for resource allocation** with **Ray for distributed execution**, creating a clean separation between infrastructure management and workload execution.

---

### 🧠 High-Level Architecture

The system is composed of three primary layers:

    LSF → Resource Allocation Layer
    Ray → Distributed Execution Layer
    Workload → Inference Logic Layer

---

### 4.1 LSF: Resource Allocation

LSF is responsible for **allocating compute resources across the cluster**.

Each submitted job requests:

- A number of workers (`num_workers`)
- CPU cores per worker (`cpus_per_worker`)
- GPUs per worker (`gpus_per_worker`)
- Memory per worker (`memory_per_worker`)

---

#### ✅ Task-Based Resource Model

This architecture uses a **task-oriented scheduling model**:

    1 LSF task = 1 resource bundle

        CPUs = cpus_per_worker  
        GPUs = gpus_per_worker  

---

LSF assigns these tasks across available hosts. A single host may receive multiple tasks depending on resource availability.

---

#### ✅ Multi-Host Execution

Once resources are allocated:

- The primary process runs on the first execution host  
- Additional processes are launched on other hosts using:

    blaunch

This enables the system to:

- Start distributed processes across nodes  
- Build a multi-node Ray cluster  

---

---

### 4.2 Ray: Distributed Execution

Ray runs on top of LSF-allocated resources and provides:

- Distributed task scheduling  
- Actor lifecycle management  
- Resource-aware placement  

---

#### ✅ One Ray Node per Host

Each host in the allocation runs exactly one Ray node:

    host → 1 Ray node

---

Ray aggregates all resources visible on that host:

- All CPUs allocated to the job  
- All GPUs assigned via LSF  

---

#### ✅ Cluster Formation

The Ray cluster is formed as follows:

1. A **head node** is started on the primary host  
2. Worker nodes are started on all other hosts (via `blaunch`)  
3. Workers connect to the head node using a shared address  

---

#### ✅ Resource Scheduling

Ray schedules work based on declared resource requirements:

- Actors: `num_cpus`, `num_gpus` per actor  
- Ray Data tasks: `num_cpus`, `num_gpus` per batch  

---

Ray automatically:

- Assigns GPUs via `CUDA_VISIBLE_DEVICES`  
- Packs tasks efficiently across hosts  
- Ensures resource isolation  

---

---

### 4.3 Workload Execution Layer

The workload layer defines **how inference is executed**.

Two execution models are supported:

---

#### ✅ Actors (Persistent Workers)

    Driver → creates actors → actors process batches

- Each actor loads the model once  
- Actors remain alive for the duration of the job  
- Ideal for high-throughput workloads  

---

---

#### ✅ Ray Data (Distributed Pipeline)

    Dataset → split into batches → processed in parallel

- Work is split into dynamic tasks  
- Tasks are scheduled across the Ray cluster  
- Supports multi-GPU inference  

---

---

### 4.4 End-to-End Execution Flow

The full workflow proceeds as follows:

---

#### Step 1 — Submission

    submit_lsf.sh → bsub

- LSF allocates resources across hosts  

---

#### Step 2 — Cluster Bootstrap

    run.sh

- Head node starts Ray  
- Worker nodes started via `blaunch`  
- Ray cluster is formed  

---

#### Step 3 — Workload Execution

    workload.py or workload_actors.py

- Ray schedules tasks or actors  
- Models are initialized  
- Batches are processed  

---

#### Step 4 — Output

- Results are written to the configured output directory  
- Logs and metrics are emitted during execution  

---

---

### 4.5 Design Principles

This architecture is built on the following principles:

---

#### ✅ Separation of Concerns

- LSF → resource allocation  
- Ray → execution and scheduling  
- Workload → business logic  

---

#### ✅ Resource Abstraction

- Users specify resource requirements in config  
- Ray maps these onto actual hardware  

---

#### ✅ Scalability

- Scale out → increase `num_workers`  
- Scale up → increase `gpus_per_worker`  

---

#### ✅ Portability

- Works across clusters with or without affinity  
- Supports CPU-only and GPU environments  

---

---

### ✅ Summary

This architecture provides a clean and scalable approach to distributed inference:

- LSF allocates compute resources across the cluster  
- Ray builds a distributed execution layer on top  
- Workloads define how inference is executed  

---

This separation allows users to **scale inference workloads efficiently** while maintaining clear control over resource usage and execution behavior.

## 5. Execution Modes

This architecture supports two execution modes for batch inference:

- **Actors mode** (`mode: actors`)
- **Ray Data mode** (`mode: ray_data`)

Each mode represents a different scaling strategy and is suited to different types of workloads.

---

### 5.1 Overview

| Mode       | Scaling Type      | Best For                          | Multi-GPU Support |
|-----------|------------------|-----------------------------------|-------------------|
| actors    | scale-out        | many independent requests          | ❌ (TP = 1 only)  |
| ray_data  | scale-out + up   | large batches, large models        | ✅ (TP > 1)       |

---

### 5.2 Actors Mode (`mode: actors`)

Actors mode is designed for **high-throughput inference** using many independent workers.

---

#### ✅ Execution Model

    Driver → creates actors → actors process requests

- Each actor runs as a **long-lived worker**
- Each actor loads the model once and reuses it
- Work is distributed manually across actors

---

#### ✅ Resource Model

Each actor:

- Uses **1 GPU** (typical case)
- Uses `cpus_per_worker` CPUs

Example:

    gpus_per_worker: 1
    tensor_parallel_size: 1

---

#### ✅ Strengths

- High throughput for many small requests  
- Efficient model reuse (load once per actor)  
- Predictable performance  
- Simple mental model  

---

#### ⚠️ Limitations

Actors mode does **not support tensor parallelism**:

    tensor_parallel_size must be 1

This means:

- Multi-GPU models (TP > 1) are not supported  
- Each actor must fit the model on a single GPU  

---

#### ✅ When to Use Actors

Use actors mode when:

- Running inference on many independent prompts  
- Model fits on a single GPU  
- Maximizing throughput is the priority  

---

---

### 5.3 Ray Data Mode (`mode: ray_data`)

Ray Data mode is designed for **large-scale batch processing** and **multi-GPU inference**.

---

#### ✅ Execution Model

    Dataset → partitioned into batches → processed in parallel tasks

- Work is broken into **independent tasks**
- Tasks are scheduled dynamically by Ray
- Tasks may use multiple GPUs

---

#### ✅ Resource Model

Each task requests:

- `num_gpus = gpus_per_worker`
- `num_cpus = cpus_per_worker`

Example:

    gpus_per_worker: 2
    tensor_parallel_size: 2

---

#### ✅ Multi-GPU Support

Ray Data supports tensor parallel models:

    tensor_parallel_size = gpus_per_worker

- Each task gets multiple GPUs on the same host  
- Suitable for large models that do not fit on a single GPU  

---

#### ✅ Strengths

- Supports large models via tensor parallelism  
- Efficient dataset processing  
- Flexible scaling (both out and up)  
- Better for long-running batch jobs  

---

#### ⚠️ Considerations

- Model initialization occurs per worker process  
- Slightly higher overhead compared to actors  
- Requires careful tuning of batch size  

---

#### ✅ When to Use Ray Data

Use Ray Data mode when:

- Running inference over large datasets  
- Using multi-GPU models  
- Model does not fit on a single GPU  
- Batch-oriented processing is desired  

---

---

### 5.4 Choosing the Right Mode

If you are unsure which mode to start with:

---

#### ✅ Use Actors Mode if:

- You want **maximum throughput**
- Your model fits on a single GPU
- You are processing many independent requests

---

#### ✅ Use Ray Data Mode if:

- You need **multi-GPU inference (TP > 1)**
- You are processing large datasets
- You want scalable batch processing

---

---

### 5.5 Key Rules

---

#### ✅ Actors Mode

    gpus_per_worker: 1
    tensor_parallel_size: 1

---

#### ✅ Ray Data Mode

    gpus_per_worker = tensor_parallel_size

---

---

### ✅ Summary

- **Actors mode** → many small, independent workers (scale-out)
- **Ray Data mode** → fewer, larger tasks (scale-up + scale-out)

---

Choosing the correct execution mode is the most important decision when configuring your workload, as it directly impacts performance, scalability, and resource utilization.

## 6. Configuration Format

All behavior in this architecture is controlled via a YAML configuration file. This allows you to define resource requirements, execution mode, model settings, and data inputs in a consistent and reproducible way.

---

### 6.1 Top-Level Structure

A typical configuration file contains the following sections:

    lsf:
    execution:
    model:
    data:

---

### 6.2 LSF Section

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
  Number of CPU cores per worker  
  If `use_affinity=false`, this must be 1

- `gpus_per_worker`  
  Number of GPUs allocated per worker

- `memory_per_worker`  
  Memory allocation per worker (LSF `rusage[mem=...]`)

- `use_affinity`  
  Enables CPU affinity (`affinity[core(N)]`)

- `interactive`  
  If true, runs job in interactive mode (`bsub -Is`)

---

---

### 6.3 Execution Section

Defines how the workload is executed.

    execution:
      mode: actors
      device: gpu
      batch_size: 32

---

#### Fields

- `mode`  
  Execution mode:
  - `actors`
  - `ray_data`

- `device`  
  `cpu` or `gpu`

- `batch_size`  
  Number of inputs processed per batch

---

---

### 6.4 Model Section

Defines model configuration.

    model:
      name: facebook/opt-1.3b
      tensor_parallel_size: 1

---

#### Fields

- `name`  
  Model identifier

- `tensor_parallel_size`  
  Number of GPUs used per model instance

---

#### ✅ Constraint

    tensor_parallel_size = gpus_per_worker

---

---

### 6.5 Data Section

Defines input and output locations.

    data:
      input_path: "{repo_root}/data/prompts.jsonl"
      output_dir: "{repo_root}/outputs/{job_id}"

---

#### Fields

- `input_path`  
  Path to input data

- `output_dir`  
  Location for results

---

#### ✅ Supported templates

- `{repo_root}` → repository root  
- `{job_id}` → LSF job ID  

---

---

### 6.6 Validation Rules

The following constraints are enforced:

- `cpus_per_worker = 1` when `use_affinity=false`
- `tensor_parallel_size = gpus_per_worker`
- `execution.device=gpu` requires `gpus_per_worker > 0`

---

---

## 7. Running Workloads

This section describes how to submit and monitor jobs.

---

### 7.1 Submit a Job

Run:

    ./submit_lsf.sh --config path/to/config.yaml

---

This will:

- Generate a submission script  
- Submit the job via `bsub`  
- Launch processes across hosts  
- Start a Ray cluster  
- Execute the workload  

---

---

### 7.2 Dry Run (Recommended First)

To validate without submitting:

    ./submit_lsf.sh --config path/to/config.yaml --dry-run

---

This shows:

- Generated LSF directives  
- Resource selection  
- Execution commands  

---

---

### 7.3 Monitor the Job

Use LSF commands:

    bjobs
    bpeek <job_id>

---

---

### 7.4 Runtime Behavior

During execution:

- Ray cluster is created across allocated hosts  
- Workers are launched via `blaunch`  
- GPUs are assigned automatically  
- Workload runs according to selected mode  

---

---

### 7.5 Output

Outputs are written to:

    data.output_dir

Includes:

- Model outputs  
- Logs  
- Intermediate results  

---

---

### 7.6 Stopping Jobs

Stop a job using:

    bkill <job_id>

---

---

## 8. Troubleshooting

This section highlights common issues and how to resolve them.

---

### 8.1 Ray Cluster Does Not Start

**Symptoms:**

- Workers fail to connect  
- Job appears stuck  

---

**Possible causes:**

- `res` daemon not running  
- `blaunch` failing  
- Incorrect host allocation  

---

**Fix:**

- Verify `res` is running on all nodes  
- Check LSF logs (`bpeek`)  
- Ensure multiple hosts were allocated  

---

---

### 8.2 GPU Not Detected

**Symptoms:**

- Job runs on CPU instead of GPU  
- Errors during model initialization  

---

**Possible causes:**

- GPU not allocated by LSF  
- GPU enforcement not enabled  

---

**Fix:**

- Ensure:

    gpus_per_worker > 0

- Confirm:

    LSB_RESOURCE_ENFORCE="gpu"

- Check `CUDA_VISIBLE_DEVICES` in logs  

---

---

### 8.3 Invalid Configuration Errors

**Symptoms:**

- Job fails immediately with validation error  

---

**Common issues:**

- `tensor_parallel_size != gpus_per_worker`
- `cpus_per_worker > 1` with `use_affinity=false`

---

**Fix:**

- Adjust configuration to match required constraints  

---

---

### 8.4 Poor Performance

**Possible causes:**

- Incorrect batch size  
- CPU over/under allocation  
- Inefficient execution mode  

---

**Fix:**

- Increase or decrease `batch_size`  
- Adjust `num_workers`  
- Use actors for throughput, Ray Data for large models  

---

---

### 8.5 Ray Scheduling Issues

**Symptoms:**

- Tasks not starting  
- Actors stuck in pending  

---

**Possible causes:**

- Insufficient resources  
- Incorrect CPU/GPU requests  

---

**Fix:**

- Verify resource totals:

    total GPUs = num_workers × gpus_per_worker  
    total CPUs = num_workers × cpus_per_worker  

- Ensure cluster has sufficient capacity  

---

---

### ✅ Summary

Most issues fall into one of three categories:

- LSF configuration issues  
- Resource mismatches  
- Execution mode misuse  

---

Carefully reviewing configuration and LSF job output will resolve the majority of problems.

