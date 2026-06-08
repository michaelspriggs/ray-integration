#!/bin/bash
# Wrapper script for running batch inference with vLLM on Ray + LSF
#
# This script simplifies running batch inference by providing sensible defaults
# and handling common use cases.

set -e

# Default values
CONFIG="batch_inference/config.yaml"
SCRIPT="batch_inference/batch_infer_vllm_actors.py"
CPU_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG="$2"
            shift 2
            ;;
        --cpu-only)
            CPU_ONLY=true
            shift
            ;;
        --ray-data)
            SCRIPT="batch_inference/batch_infer_ray_data.py"
            shift
            ;;
        --model)
            MODEL="$2"
            shift 2
            ;;
        --input)
            INPUT="$2"
            shift 2
            ;;
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --config PATH       Path to config file (default: batch_inference/config.yaml)"
            echo "  --cpu-only          Run in CPU-only mode"
            echo "  --ray-data          Use Ray Data implementation instead of actors"
            echo "  --model NAME        Override model name"
            echo "  --input PATH        Override input file path"
            echo "  --output PATH       Override output file path"
            echo "  --help              Show this help message"
            echo ""
            echo "Examples:"
            echo "  # CPU-only testing"
            echo "  $0 --cpu-only --model gpt2"
            echo ""
            echo "  # GPU inference with custom config"
            echo "  $0 --config my_config.yaml"
            echo ""
            echo "  # Use Ray Data pipeline"
            echo "  $0 --ray-data"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build command
CMD="python $SCRIPT --config $CONFIG"

if [ "$CPU_ONLY" = true ]; then
    CMD="$CMD --cpu-only"
fi

if [ ! -z "$MODEL" ]; then
    CMD="$CMD --model $MODEL"
fi

if [ ! -z "$INPUT" ]; then
    CMD="$CMD --input $INPUT"
fi

if [ ! -z "$OUTPUT" ]; then
    CMD="$CMD --output $OUTPUT"
fi

echo "Running batch inference..."
echo "Command: $CMD"
echo ""

# Execute
eval $CMD

# Made with Bob
