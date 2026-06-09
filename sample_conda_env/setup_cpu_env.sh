#!/bin/bash
# Setup script for Ray 2.x CPU environment
# This script creates the conda environment and installs PyTorch CPU version

set -e

echo "=== Setting up Ray 2.x CPU Environment ==="
echo ""

# Create conda environment
echo "Step 1: Creating conda environment..."
conda env create -f ray_2x_cpu.yml

echo ""
echo "Step 2: Activating environment..."
eval "$(conda shell.bash hook)"
conda activate ray_cpu

echo ""
echo "Step 3: Installing PyTorch CPU version..."
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu

echo ""
echo "Step 4: Verifying installation..."
python -c "import ray; print(f'Ray version: {ray.__version__}')"
python -c "import torch; print(f'PyTorch version: {torch.__version__}')"
python -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "To activate the environment, run:"
echo "  conda activate ray_cpu"
echo ""
echo "To test the setup, run:"
echo "  python -c 'import ray; ray.init(); print(\"Ray is working!\"); ray.shutdown()'"

# Made with Bob
