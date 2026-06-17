#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# --------------------------------------------
# Parse command-line arguments
# --------------------------------------------
CONFIG_FILE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--config CONFIG_FILE]"
      echo ""
      echo "Options:"
      echo "  --config CONFIG_FILE    Path to configuration file (default: config.yaml in current directory)"
      echo "  -h, --help             Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Use --help for usage information" >&2
      exit 1
      ;;
  esac
done

# --------------------------------------------
# Determine config path
# --------------------------------------------
if [[ -n "$CONFIG_FILE" ]]; then
  # User specified --config option
  CONFIG_PATH="$CONFIG_FILE"
elif [[ -n "${CONFIG_PATH:-}" ]]; then
  # CONFIG_PATH environment variable is set
  CONFIG_PATH="$CONFIG_PATH"
else
  # Default: config.yaml in script directory
  CONFIG_PATH="${SCRIPT_DIR}/config.yaml"
fi

# Resolve to absolute path
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "ERROR: Configuration file not found: $CONFIG_PATH" >&2
  exit 1
fi

echo "Using configuration file: $CONFIG_PATH"

cd "${REPO_ROOT}"

# --------------------------------------------
# Parse config.yaml using Python + PyYAML
# --------------------------------------------
eval "$(
python3 - "$CONFIG_PATH" <<'PY' 2>/dev/null || echo "# Python validation skipped"
import sys
try:
    import yaml
except ImportError:
    # PyYAML not available, skip validation
    sys.exit(0)
import shlex

config_path = sys.argv[1]

with open(config_path) as f:
    cfg = yaml.safe_load(f)

lsf = cfg.get("lsf", {})

# ------------------------
# Validation
# ------------------------
required = ["num_workers", "gpus_per_worker"]
for key in required:
    if key not in lsf:
        raise ValueError(f"Missing required field: lsf.{key}")

num_workers = int(lsf["num_workers"])
gpus_per_worker = int(lsf["gpus_per_worker"])

if num_workers <= 0:
    raise ValueError("lsf.num_workers must be > 0")

if gpus_per_worker <= 0:
    raise ValueError("lsf.gpus_per_worker must be > 0")

# ------------------------
# Helpers
# ------------------------
def emit(name, value):
    print(f"{name}={shlex.quote(str(value))}")

# ------------------------
# Emit environment
# ------------------------
emit("TOTAL_WORKERS", num_workers)
emit("GPUS_PER_WORKER", gpus_per_worker)
emit("RESTRICT_TO_SINGLE_HOST", str(lsf.get("restrict_to_single_host", False)).lower())
emit("INTERACTIVE", str(lsf.get("interactive", False)).lower())

if "queue" in lsf:
    emit("QUEUE", lsf["queue"])

if "job_name" in lsf:
    emit("JOB_NAME", lsf["job_name"])

if "output_log" in lsf:
    emit("OUTPUT_LOG", lsf["output_log"])

if "memory_per_worker" in lsf:
    emit("MEMORY_PER_WORKER", lsf["memory_per_worker"])
PY
)"

# --------------------------------------------
# Fallback: Parse config with grep/sed if Python failed
# --------------------------------------------
if [[ -z "${TOTAL_WORKERS:-}" ]]; then
  echo "Warning: Python config parsing failed, using grep/sed fallback" >&2
  
  # Extract values using grep and sed (|| true to prevent failures on missing fields)
  TOTAL_WORKERS=$(grep -E '^\s*num_workers:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  GPUS_PER_WORKER=$(grep -E '^\s*gpus_per_worker:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' || true)
  QUEUE=$(grep -E '^\s*queue:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  JOB_NAME=$(grep -E '^\s*job_name:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  OUTPUT_LOG=$(grep -E '^\s*output_log:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  MEMORY_PER_WORKER=$(grep -E '^\s*memory_per_worker:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  RESTRICT_TO_SINGLE_HOST=$(grep -E '^\s*restrict_to_single_host:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  INTERACTIVE=$(grep -E '^\s*interactive:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  
  # Convert boolean strings
  [[ "$RESTRICT_TO_SINGLE_HOST" == "true" ]] && RESTRICT_TO_SINGLE_HOST="true" || RESTRICT_TO_SINGLE_HOST="false"
  [[ "$INTERACTIVE" == "true" ]] && INTERACTIVE="true" || INTERACTIVE="false"
fi

# --------------------------------------------
# Build bsub command
# --------------------------------------------
bsub_args=()

# Interactive mode
if [[ "${INTERACTIVE:-false}" == "true" ]]; then
  bsub_args+=(-Is)
fi

# Basic options
[[ -n "${JOB_NAME:-}" ]] && bsub_args+=(-J "${JOB_NAME}")
[[ -n "${QUEUE:-}" ]] && bsub_args+=(-q "${QUEUE}")
[[ -n "${TOTAL_WORKERS:-}" ]] && bsub_args+=(-n "${TOTAL_WORKERS}")

# Output log (skip if interactive mode)
if [[ "${INTERACTIVE:-false}" != "true" ]] && [[ -n "${OUTPUT_LOG:-}" ]]; then
  bsub_args+=(-o "${OUTPUT_LOG}")
fi

# GPU
if [[ -n "${GPUS_PER_WORKER:-}" ]]; then
  bsub_args+=(-gpu "num=${GPUS_PER_WORKER}/task:j_exclusive=yes")
fi

# Resource requirements
resource_requirements=()

if [[ -n "${MEMORY_PER_WORKER:-}" ]]; then
  resource_requirements+=("rusage[mem=${MEMORY_PER_WORKER}/task]")
fi

if [[ "${RESTRICT_TO_SINGLE_HOST:-false}" == "true" ]]; then
  resource_requirements+=("span[hosts=1]")
fi

# Combine -R into single argument
if [[ ${#resource_requirements[@]} -gt 0 ]]; then
  combined_requirements="$(printf "%s " "${resource_requirements[@]}")"
  combined_requirements="${combined_requirements% }"
  bsub_args+=(-R "${combined_requirements}")
fi

# --------------------------------------------
# Debug / Dry-run
# --------------------------------------------
# Build the command to execute
EXEC_CMD="export CONFIG_PATH='${CONFIG_PATH}'; ${SCRIPT_DIR}/run.sh"

echo "Submitting job with command:"
printf ' %q' bsub "${bsub_args[@]}" "${EXEC_CMD}"
echo

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "Dry-run enabled. Not submitting."
  exit 0
fi

# --------------------------------------------
# Submit job
# --------------------------------------------
bsub "${bsub_args[@]}" "${EXEC_CMD}"

