#!/bin/bash
#BSUB -n 4
#BSUB -gpu "num=1/task:j_exclusive=yes"
#BSUB -o output.%J
#BSUB -J ray_batch_inference
#BSUB -q normal
#BSUB -W 4:00
#BSUB -M 200GB
#BSUB -R "rusage[mem=20GB]"

./common/start_ray_cluster.sh \
  -n ray_batch_inference \
  -c "python reference_architectures/batch_inference_ray_data/workload_actors.py --config reference_architectures/batch_inference_ray_data/config.yaml" \
  -m 20000000000
