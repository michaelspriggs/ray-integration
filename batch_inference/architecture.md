# Architecture and Design Rationale

This document explains the design decisions behind the batch inference reference implementation.

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         LSF Scheduler                        │
│  - Resource allocation (CPUs, GPUs, memory)                 │
│  - Sets CUDA_VISIBLE_DEVICES for GPU isolation              │
│  - Job management and monitoring                            │
└────────────────┬────────────────────────────────────────────┘
                 │
                 │ bsub command
                 ▼
┌─────────────────────────────────────────────────────────────┐
│              ray_launch_cluster.sh                           │
│  - Reads LSF allocation (hosts, CPUs, GPUs)                 │
│  - Starts Ray head node                                      │
│  - Starts Ray worker nodes                                   │
│  - Executes user workload                                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 │ Ray cluster
                 ▼
┌─────────────────────────────────────────────────────────────┐
│                      Ray Cluster                             │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Head Node   │  │ Worker Node  │  │ Worker Node  │     │
│  │              │  │              │  │              │     │
│  │ - Scheduler  │  │ - Executors  │  │ - Executors  │     │
│  │ - Dashboard  │  │ - GPUs       │  │ - GPUs       │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
└────────────────┬────────────────────────────────────────────┘
                 │
                 │ Workload execution
                 ▼
┌─────────────────────────────────────────────────────────────┐
│              Batch Inference Application                     │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              vLLM Worker Actors                       │  │
│  │                                                        │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐ │  │
│  │  │ Actor 1 │  │ Actor 2 │  │ Actor 3 │  │ Actor 4 │ │  │
│  │  │ GPU 0   │  │ GPU 1   │  │ GPU 2   │  │ GPU 3   │ │  │
│  │  │ vLLM    │  │ vLLM    │  │ vLLM    │  │ vLLM    │ │  │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘ │  │
│  │                                                        │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  Input: JSONL prompts → Batching → Inference → Output       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Why Ray + vLLM?

**Ray provides:**
- Distributed execution framework
- Resource management (CPU, GPU, memory)
- Fault tolerance and retries
- Monitoring via dashboard
- Flexible actor model

**vLLM provides:**
- Optimized LLM inference engine
- PagedAttention for efficient memory usage
- Continuous batching
- Tensor parallelism support
- High throughput

**Together:** Best-in-class distributed batch inference for LLMs

### 2. Why LSF Integration?

**LSF advantages:**
- Enterprise-grade job scheduling
- Multi-tenancy support
- Resource quotas and fairshare
- GPU isolation via `j_exclusive=yes`
- Automatic `CUDA_VISIBLE_DEVICES` management
- Integration with existing HPC infrastructure

**Alternative schedulers** (Slurm, PBS) could be supported with similar patterns.

### 3. Actor-Based vs Ray Data

We provide both implementations for different use cases:

#### Actor-Based Approach (`batch_infer_vllm_actors.py`)

**Advantages:**
- Fine-grained control over resource allocation
- Explicit actor placement
- Better for heterogeneous clusters
- Easier to debug and monitor
- More flexible for complex workflows

**Use when:**
- You need precise GPU allocation
- You have mixed hardware (different GPU types)
- You want to optimize for specific patterns
- You need custom actor logic

#### Ray Data Approach (`batch_infer_ray_data.py`)

**Advantages:**
- Simpler code (less boilerplate)
- Automatic data partitioning
- Built-in fault tolerance
- Better for streaming workloads
- Automatic backpressure handling

**Use when:**
- You have large datasets
- You want simpler code
- You need automatic fault tolerance
- You're processing streaming data

**Recommendation:** Start with actors for production, use Ray Data for simpler use cases.

### 4. GPU Isolation Strategy

**LSF's `j_exclusive=yes` flag:**
- Ensures GPUs are not shared between jobs
- Prevents interference and performance degradation
- Critical for multi-tenant environments
- LSF automatically sets `CUDA_VISIBLE_DEVICES`

**Ray's GPU detection:**
- Reads `CUDA_VISIBLE_DEVICES` from environment
- Automatically discovers available GPUs
- No manual GPU specification needed
- Works seamlessly with LSF allocation

**Why this matters:**
- Predictable performance
- No GPU contention
- Simplified configuration
- Works with heterogeneous clusters

### 5. Flexible vs Uniform GPU Allocation

**Our approach: Flexible allocation**

**Advantages:**
- Works with any cluster topology
- Handles heterogeneous hardware naturally
- Better resource utilization
- Simpler LSF submission commands
- No need for CSM-specific features

**How it works:**
1. LSF allocates resources based on request
2. Ray discovers actual topology
3. vLLM actors adapt to available GPUs
4. Automatic load balancing

**Example scenarios:**

**Scenario 1: Uniform cluster**
```
Host 1: 4 GPUs → 4 actors (1 GPU each)
Host 2: 4 GPUs → 4 actors (1 GPU each)
Total: 8 actors
```

**Scenario 2: Heterogeneous cluster**
```
Host 1: 8 GPUs → 8 actors (1 GPU each)
Host 2: 4 GPUs → 4 actors (1 GPU each)
Host 3: 2 GPUs → 2 actors (1 GPU each)
Total: 14 actors (automatically balanced)
```

**Scenario 3: Large model with tensor parallelism**
```
Host 1: 8 GPUs → 1 actor (8 GPUs for TP)
Host 2: 8 GPUs → 1 actor (8 GPUs for TP)
Total: 2 actors, each using 8 GPUs
```

### 6. Configuration Design

**YAML-based configuration:**
- Human-readable and editable
- Supports comments and documentation
- Easy to version control
- Can be overridden via CLI

**"auto" values:**
- Automatically adapt to available resources
- Reduce configuration burden
- Work across different cluster sizes
- Sensible defaults for common cases

**Example:**
```yaml
execution:
  num_workers: "auto"  # Creates one actor per GPU
  batch_size: 16       # Fixed value
```

### 7. Two-Pattern LSF Submission

**Pattern 1: CPU-only**
```bash
bsub -n 8 -o output.%J ./ray_launch_cluster.sh ...
```
- Simple, no GPU specification
- For testing and development
- Uses small models (gpt2)

**Pattern 2: GPU with exclusive access**
```bash
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J ./ray_launch_cluster.sh ...
```
- Production pattern
- GPU isolation guaranteed
- Predictable performance

**Why only two patterns?**
- Covers 95% of use cases
- Easy to remember and document
- Reduces complexity
- Other options available via LSF flags

### 8. Fault Tolerance Strategy

**Ray-level:**
- Actor restarts on failure
- Task retries (configurable)
- Automatic rescheduling

**Application-level:**
- Batch-level retries
- Progress tracking
- Checkpoint support (future)

**LSF-level:**
- Job requeue on node failure
- Resource reallocation

### 9. Monitoring and Observability

**Ray Dashboard:**
- Real-time cluster status
- Resource utilization
- Task execution timeline
- Actor placement visualization

**Application logs:**
- Progress tracking
- Throughput metrics
- Error reporting
- Performance statistics

**LSF integration:**
- Job output files
- Resource usage reports
- Accounting data

## Performance Considerations

### Throughput Optimization

**Key factors:**
1. **Batch size:** Larger batches = better GPU utilization
2. **Tensor parallelism:** For models that don't fit on single GPU
3. **GPU memory utilization:** Higher = more KV cache = better throughput
4. **Number of workers:** More workers = more parallelism

**Typical throughput:**
- Small models (< 7B): 50-100 prompts/second
- Medium models (7-13B): 20-50 prompts/second
- Large models (70B+): 5-15 prompts/second

### Memory Management

**GPU memory breakdown:**
1. Model weights (fixed)
2. KV cache (dynamic, controlled by `gpu_memory_utilization`)
3. Activation memory (depends on batch size)

**Optimization strategies:**
- Use quantization (AWQ, GPTQ) to reduce model size
- Adjust `gpu_memory_utilization` to balance cache vs headroom
- Use tensor parallelism for large models
- Monitor with `nvidia-smi`

### Scaling Patterns

**Horizontal scaling:**
- Add more GPUs → More workers → Higher throughput
- Linear scaling up to network/storage bottlenecks

**Vertical scaling:**
- Larger batches → Better GPU utilization
- Limited by GPU memory

**Optimal configuration:**
- Depends on model size, prompt length, and hardware
- Requires experimentation and profiling

## Comparison with Alternatives

### vs. Standalone vLLM

**Our implementation:**
- ✅ Distributed across multiple nodes
- ✅ LSF integration for resource management
- ✅ Fault tolerance via Ray
- ✅ Monitoring and observability

**Standalone vLLM:**
- ✅ Simpler for single-node
- ❌ No distributed execution
- ❌ Manual resource management

### vs. Ray Serve

**Batch inference (this):**
- ✅ Optimized for throughput
- ✅ Simpler for offline workloads
- ✅ Better resource utilization for batches
- ❌ Not for real-time serving

**Ray Serve:**
- ✅ Online serving with HTTP
- ✅ Auto-scaling
- ✅ Request routing
- ❌ More overhead for batch workloads

### vs. KServe/TorchServe

**Batch inference (this):**
- ✅ Higher throughput for offline
- ✅ Simpler deployment
- ✅ LSF integration
- ❌ No REST API

**KServe/TorchServe:**
- ✅ Production serving platform
- ✅ REST/gRPC APIs
- ✅ Model versioning
- ❌ More complex setup
- ❌ Lower throughput for batches

## Future Enhancements

Potential improvements for future versions:

1. **Checkpointing:** Save progress for long-running jobs
2. **Streaming output:** Write results as they're generated
3. **Multi-model support:** Run different models in parallel
4. **Dynamic batching:** Adjust batch size based on prompt length
5. **Cost tracking:** Monitor GPU hours and costs
6. **A/B testing:** Compare different models or configurations
7. **Prometheus metrics:** Export metrics for monitoring
8. **Grafana dashboards:** Visualize performance metrics

## Conclusion

This architecture provides:
- ✅ Production-ready batch inference
- ✅ Flexible resource allocation
- ✅ Simple LSF integration
- ✅ High throughput and efficiency
- ✅ Easy to understand and modify
- ✅ Scalable from single node to large clusters

The design prioritizes:
1. **Simplicity:** Two clear patterns, minimal configuration
2. **Flexibility:** Works with any cluster topology
3. **Performance:** Optimized for throughput
4. **Reliability:** Fault tolerance and monitoring
5. **Maintainability:** Clear code, good documentation