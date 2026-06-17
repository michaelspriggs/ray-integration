# 🚀 Batch Inference with Ray + vLLM on LSF

This reference architecture demonstrates distributed batch inference for large language models using:

- Ray 2.x for distributed execution  
- vLLM for high-performance inference  
- LSF (IBM Spectrum Computing) for cluster resource management  

It supports multiple Ray execution models while maintaining a **consistent and explicit resource model**.

---

# 🧠 Architecture Overview

## Core Resource Model

This system is built around a simple, deterministic model:

    1 LSF task = 1 Ray worker

Each worker is allocated:

    - gpus_per_worker GPUs
    - memory_per_worker memory
    - cpus_per_worker CPUs (enforced by Ray)

---

## Resource Mapping

| Layer | Responsibility |
|------|----------------|
| LSF | Allocates CPUs, GPUs, and memory |
| Ray | Schedules distributed execution |
| Workload | Consumes resources |

---

## Total Resources

    Total GPUs = num_workers × gpus_per_worker

If using tensor parallelism:

    GPUs per worker = tensor_parallel_size

Constraint:

    num_workers × tensor_parallel_size ≤ total GPUs

---

# ⚙️ Execution Flow

    submit_lsf.sh
        ↓
    run.sh
        ↓
    start_ray_cluster.sh
        ↓
    Ray cluster starts
        ↓
    workload.py OR workload_actors.py runs
        ↓
    stop_ray_cluster.sh (automatic cleanup)

---

# 📦 Configuration

All behavior is controlled via `config.yaml`.

---

## LSF (Resource Allocation)

```yaml
lsf:
  num_workers: 8
  gpus_per_worker: 1
  memory_per_worker: "8GB"
  restrict_to_single_host: false
```

---

## Execution (Workload Behavior)

```yaml
execution:
  mode: "actors"        # or "ray_data"
  num_workers: "auto"   # uses lsf.num_workers
  cpus_per_worker: 4
  device: "gpu"         # gpu | cpu
  batch_size: 16
```

---

## Model

```yaml
model:
  name: "ibm-granite/granite-3b-code-base"
  tensor_parallel_size: 1
```

---

# 🧠 Choosing an Execution Model

This architecture supports two execution models:

- Ray Actors (`mode: actors`)
- Ray Data (`mode: ray_data`)

Both use the same resource model, but differ in how execution is handled.

---

## ✅ Ray Actors (`mode: actors`)

Best for: Maximum control and performance tuning

### Characteristics

- Stateful workers (model loaded once per worker)
- Explicit scheduling of work
- Fine-grained control over execution
- Manual batching and distribution

### Use when

- You want maximum GPU utilization
- You need tight control over scheduling
- You are optimizing performance
- You need backpressure / custom logic

### Trade-offs

- More complex implementation
- Manual scheduling required

---

## ✅ Ray Data (`mode: ray_data`)

Best for: Simplicity and large datasets

### Characteristics

- Declarative pipeline using `map_batches`
- Automatic batching and parallelism
- Built-in fault tolerance
- Handles dataset streaming

### Use when

- You have large datasets
- You want minimal code
- You prefer automatic scaling

### Trade-offs

- Less control over execution details
- Harder to fine-tune performance

---

## ⚖️ Comparison

| Feature | Actors | Ray Data |
|--------|--------|----------|
| Control | High | Low |
| Simplicity | Medium | High |
| Scheduling | Manual | Automatic |
| Fault tolerance | Manual | Built-in |
| Best for | Optimization | Large-scale data |

---

## ✅ Recommended Workflow

- Start with Ray Data
- Switch to Actors for performance tuning

---

## 🧠 Mental Model

    Ray Actors = "I control execution"
    Ray Data   = "Ray controls execution"

---

# 🚀 Running the Example

## Submit to LSF

```bash
./submit_lsf.sh
```

---

## Run locally (debug)

```bash
CONFIG_PATH=... ./run.sh
```

---

## Dry run

```bash
DRY_RUN=true ./submit_lsf.sh
```

---

# ⚙️ Scripts

| Script | Role |
|--------|------|
| submit_lsf.sh | Submit job to LSF |
| run.sh | Orchestrates execution |
| start_ray_cluster.sh | Starts Ray cluster |
| stop_ray_cluster.sh | Stops Ray cluster |
| workload_actors.py | Actor-based execution |
| workload.py | Ray Data execution |

---

# ⚙️ Key Design Principles

## ✅ Explicit Resource Model

- LSF controls GPUs and memory  
- Ray controls CPU usage and scheduling  

---

## ✅ Separation of Concerns

| Layer | Responsibility |
|------|----------------|
| LSF | Allocation |
| Ray | Execution |
| Workload | Computation |

---

## ✅ Deterministic Scaling

- No hidden autoscaling
- Scaling is fully controlled by LSF

---

# ⚠️ Common Pitfalls

## GPU mismatch

    num_workers × tensor_parallel_size > total GPUs

→ causes startup failure

---

## Memory pressure

- Ensure memory_per_worker is sufficient
- Avoid very large batch sizes

---

## CPU oversubscription

- CPUs are enforced by Ray, not LSF
- Set cpus_per_worker appropriately

---

# 🧪 When to Use This Architecture

Use this when:

- Running large-scale batch inference
- Using an LSF-managed cluster
- You need explicit resource control

---

# ✅ Summary

This architecture provides:

- Clear, consistent resource modeling
- Two execution paradigms (Actors and Ray Data)
- Scalable, production-ready design
- Clean separation of concerns

---

# 💬 Final Takeaway

    LSF allocates resources
    Ray executes the workload
    The application consumes them
