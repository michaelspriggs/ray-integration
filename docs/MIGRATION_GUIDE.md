# Migration Guide: Ray 1.x to Ray 2.x

This guide helps you migrate existing Ray 1.x code to Ray 2.x.

## Overview

Ray 2.x introduced significant API changes and improvements. This guide covers the most common migration scenarios.

## Environment Setup

### Python Version

**Ray 1.x:**
```yaml
python=3.7
```

**Ray 2.x:**
```yaml
python=3.10  # or 3.11
```

### Dependencies

**Ray 1.x:**
```yaml
- ray==1.3.0
- pytorch=1.8.1
- cudatoolkit=10.2
```

**Ray 2.x:**
```yaml
- ray[default]==2.40.0
- torch==2.2.0
- cudatoolkit=11.8
```

## API Changes

### 1. Ray Train (formerly Ray SGD)

#### TorchTrainer

**Ray 1.x:**
```python
from ray.util.sgd.torch import TorchTrainer, TrainingOperator
from ray.util.sgd.utils import BATCH_SIZE, override

class MyTrainingOperator(TrainingOperator):
    @override(TrainingOperator)
    def setup(self, config):
        # Setup code
        model = MyModel()
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        self.model, self.optimizer = self.register(
            models=model, optimizers=optimizer
        )

trainer = TorchTrainer(
    training_operator_cls=MyTrainingOperator,
    num_workers=4,
    use_gpu=True,
    config={"lr": 0.01, BATCH_SIZE: 128}
)

for i in range(10):
    trainer.train()
```

**Ray 2.x:**
```python
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

def train_loop_per_worker(config):
    import torch
    from torch import nn
    
    # Setup code
    model = MyModel()
    optimizer = torch.optim.SGD(model.parameters(), lr=config["lr"])
    
    # Wrap with Ray Train
    model = ray.train.torch.prepare_model(model)
    
    # Training loop
    for epoch in range(config["num_epochs"]):
        for batch in train_loader:
            # Training step
            loss = model(batch)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
        
        # Report metrics
        ray.train.report({"loss": loss.item()})

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=ScalingConfig(
        num_workers=4,
        use_gpu=True,
    ),
    train_loop_config={"lr": 0.01, "num_epochs": 10}
)

result = trainer.fit()
```

### 2. Ray Data (formerly Ray Datasets)

**Ray 1.x:**
```python
import ray

ds = ray.data.read_csv("data.csv")
ds = ds.map(lambda x: {"value": x["value"] * 2})
ds.write_parquet("output")
```

**Ray 2.x:**
```python
import ray

# API is mostly the same, but with improvements
ds = ray.data.read_csv("data.csv")
ds = ds.map(lambda x: {"value": x["value"] * 2})
ds.write_parquet("output")

# New: Better batching support
ds = ds.map_batches(
    lambda batch: {"value": batch["value"] * 2},
    batch_size=100
)
```

### 3. Ray Core

#### Runtime Context

**Ray 1.x:**
```python
import ray

@ray.remote
def get_node_id():
    return ray.get_runtime_context().node_id.hex()
```

**Ray 2.x:**
```python
import ray

@ray.remote
def get_node_id():
    return ray.get_runtime_context().get_node_id()
```

#### Actor Options

**Ray 1.x:**
```python
@ray.remote(num_gpus=1)
class MyActor:
    pass

actor = MyActor.remote()
```

**Ray 2.x:**
```python
# Same syntax, but more options available
@ray.remote(num_gpus=1, num_cpus=2)
class MyActor:
    pass

actor = MyActor.remote()

# Or use options() for dynamic configuration
actor = MyActor.options(num_gpus=2).remote()
```

### 4. Ray Serve

**Ray 1.x:**
```python
from ray import serve

@serve.deployment
class MyModel:
    def __call__(self, request):
        return {"result": "prediction"}

MyModel.deploy()
```

**Ray 2.x:**
```python
from ray import serve
from fastapi import FastAPI

app = FastAPI()

@serve.deployment
@serve.ingress(app)
class MyModel:
    @app.post("/predict")
    def predict(self, data: dict):
        return {"result": "prediction"}

serve.run(MyModel.bind())
```

## LSF Integration Changes

### Ray 1.x Pattern

```bash
bsub -n 2 -R "span[ptile=1]" -gpu "num=2" \
  ./ray_launch_cluster.sh \
  -c "python workload.py" \
  -n ray \
  -m 20000000000
```

### Ray 2.x Pattern

**CPU-only:**
```bash
bsub -n 8 -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_cpu \
  -c "python workload.py" \
  -m 20000000000
```

**GPU with exclusive access:**
```bash
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_gpu \
  -c "python workload.py" \
  -m 20000000000
```

**Key changes:**
- Removed unnecessary `span[ptile=1]` restriction
- Added `j_exclusive=yes` for GPU isolation
- Simplified patterns (only two: CPU and GPU)

## Common Migration Issues

### Issue 1: Import Errors

**Error:**
```
ImportError: cannot import name 'TorchTrainer' from 'ray.util.sgd.torch'
```

**Solution:**
```python
# Old
from ray.util.sgd.torch import TorchTrainer

# New
from ray.train.torch import TorchTrainer
```

### Issue 2: TrainingOperator Not Found

**Error:**
```
ImportError: cannot import name 'TrainingOperator'
```

**Solution:**
Replace `TrainingOperator` class with `train_loop_per_worker` function. See TorchTrainer example above.

### Issue 3: BATCH_SIZE Constant

**Error:**
```
ImportError: cannot import name 'BATCH_SIZE' from 'ray.util.sgd.utils'
```

**Solution:**
```python
# Old
from ray.util.sgd.utils import BATCH_SIZE
config = {BATCH_SIZE: 128}

# New
config = {"batch_size": 128}  # Use regular dict key
```

### Issue 4: GPU Detection

**Ray 1.x:**
```python
# Manual GPU specification
ray.init(num_gpus=4)
```

**Ray 2.x:**
```python
# Auto-detect from CUDA_VISIBLE_DEVICES (set by LSF)
ray.init(address="auto")  # When using ray_launch_cluster.sh
```

## Step-by-Step Migration Process

### 1. Update Environment

```bash
# Create new Ray 2.x environment
conda env create -f sample_conda_env/ray_2x_gpu.yml
conda activate ray_gpu
```

### 2. Update Imports

Replace all Ray 1.x imports with Ray 2.x equivalents:

```python
# Ray 1.x
from ray.util.sgd.torch import TorchTrainer, TrainingOperator
from ray.util.sgd.utils import BATCH_SIZE

# Ray 2.x
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig
```

### 3. Refactor Training Code

Convert `TrainingOperator` classes to `train_loop_per_worker` functions.

### 4. Update LSF Submission

Use the new simplified patterns:
- CPU-only: `bsub -n N`
- GPU: `bsub -n N -gpu "num=1/task:j_exclusive=yes"`

### 5. Test

Start with CPU-only testing:
```bash
bsub -n 4 -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_cpu \
  -c "python your_workload.py" \
  -m 10000000000
```

### 6. Deploy to GPU

Once CPU testing passes:
```bash
bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J \
  ./ray_launch_cluster.sh \
  -n ray_gpu \
  -c "python your_workload.py" \
  -m 20000000000
```

## Testing Your Migration

### Minimal Test Script

```python
import ray

# Test Ray initialization
ray.init(address="auto")

# Test basic functionality
@ray.remote
def test_function():
    return "Ray 2.x is working!"

result = ray.get(test_function.remote())
print(result)

# Test GPU detection
resources = ray.cluster_resources()
print(f"Available GPUs: {resources.get('GPU', 0)}")

ray.shutdown()
```

### Run Test

```bash
bsub -n 2 -gpu "num=1/task:j_exclusive=yes" -o test.%J \
  ./ray_launch_cluster.sh \
  -n ray_gpu \
  -c "python test_ray2.py" \
  -m 5000000000
```

## Resources

- **Ray 2.x Documentation**: https://docs.ray.io/en/latest/
- **Ray Train Guide**: https://docs.ray.io/en/latest/train/train.html
- **Ray Migration Guide**: https://docs.ray.io/en/latest/ray-overview/migration-guide.html
- **This Repository**: See `batch_inference/` for Ray 2.x examples

## Getting Help

If you encounter issues:

1. Check the [Ray 2.x documentation](https://docs.ray.io/)
2. Review the `batch_inference/` examples in this repository
3. Check Ray GitHub issues: https://github.com/ray-project/ray/issues
4. Ask on Ray Discourse: https://discuss.ray.io/

## Summary

Key takeaways:
- ✅ Ray 2.x requires Python 3.10+
- ✅ `TrainingOperator` → `train_loop_per_worker` function
- ✅ Simplified LSF patterns (CPU-only and GPU)
- ✅ Automatic GPU detection via `CUDA_VISIBLE_DEVICES`
- ✅ Better performance and new features
- ✅ Backward compatibility for Ray Core APIs (mostly)

The migration effort is worthwhile for:
- Better performance
- Modern Python support
- New features (vLLM, improved Ray Train, etc.)
- Long-term support