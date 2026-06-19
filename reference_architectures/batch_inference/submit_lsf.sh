#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# --------------------------------------------
# Parse command-line arguments
# --------------------------------------------
CONFIG_PATH=""
DRY_RUN="${DRY_RUN:-false}"

while [[ $# -gt 0 ]]; do
  case $1 in
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    -h|--help)
      echo "Usage: $0 --config CONFIG_FILE [OPTIONS]"
      echo ""
      echo "Required arguments:"
      echo "  --config CONFIG_FILE    Path to configuration file"
      echo ""
      echo "Optional arguments:"
      echo "  --dry-run              Preview the bsub command without submitting"
      echo "  -h, --help             Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0 --config config/config.yaml"
      echo "  $0 --config config/config.yaml --dry-run"
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
# Validate required arguments
# --------------------------------------------
if [[ -z "$CONFIG_PATH" ]]; then
  echo "ERROR: --config is required" >&2
  echo "Use --help for usage information" >&2
  exit 1
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
python3 - "$CONFIG_PATH" "$REPO_ROOT" <<'PY' 2>/dev/null || echo "# Python validation skipped"
import sys
try:
    import yaml
except ImportError:
    # PyYAML not available, skip validation
    sys.exit(0)
import shlex

config_path = sys.argv[1]
repo_root = sys.argv[2]

with open(config_path) as f:
    cfg = yaml.safe_load(f)

lsf = cfg.get("lsf", {})

# ------------------------
# Validation
# ------------------------
required = ["num_workers", "cpus_per_worker", "gpus_per_worker"]
for key in required:
    if key not in lsf:
        raise ValueError(f"Missing required field: lsf.{key}")

# Handle "auto" for num_workers - will be resolved later
num_workers_raw = lsf["num_workers"]
if num_workers_raw == "auto":
    num_workers = "auto"
else:
    num_workers = int(num_workers_raw)
    if num_workers <= 0:
        raise ValueError("lsf.num_workers must be > 0")

cpus_per_worker = int(lsf["cpus_per_worker"])
if cpus_per_worker <= 0:
    raise ValueError("lsf.cpus_per_worker must be > 0")

gpus_per_worker = int(lsf["gpus_per_worker"])
if gpus_per_worker < 0:
    raise ValueError("lsf.gpus_per_worker must be >= 0")

# Calculate total slots needed
if num_workers != "auto":
    total_slots = num_workers * cpus_per_worker
else:
    total_slots = "auto"

# ------------------------
# Helpers
# ------------------------
def emit(name, value):
    print(f"{name}={shlex.quote(str(value))}")

# ------------------------
# Emit environment
# ------------------------
emit("TOTAL_WORKERS", num_workers)
emit("CPUS_PER_WORKER", cpus_per_worker)
emit("GPUS_PER_WORKER", gpus_per_worker)
emit("TOTAL_SLOTS", total_slots)
emit("INTERACTIVE", str(lsf.get("interactive", False)).lower())

if "queue" in lsf:
    emit("QUEUE", lsf["queue"])

if "job_name" in lsf:
    emit("JOB_NAME", lsf["job_name"])

if "output_log" in lsf:
    # Resolve {repo_root} template and convert {job_id} to %J for LSF
    output_log = lsf["output_log"]
    output_log = output_log.replace("{repo_root}", repo_root)
    output_log = output_log.replace("{job_id}", "%J")
    emit("OUTPUT_LOG", output_log)

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
  TOTAL_WORKERS=$(grep -E '^\s*num_workers:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" | tr -d ' ' || true)
  CPUS_PER_WORKER=$(grep -E '^\s*cpus_per_worker:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' || true)
  GPUS_PER_WORKER=$(grep -E '^\s*gpus_per_worker:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' || true)
  QUEUE=$(grep -E '^\s*queue:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  JOB_NAME=$(grep -E '^\s*job_name:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  OUTPUT_LOG=$(grep -E '^\s*output_log:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  # Resolve {repo_root} template and convert {job_id} to %J for LSF
  if [[ -n "$OUTPUT_LOG" ]]; then
    OUTPUT_LOG="${OUTPUT_LOG//\{repo_root\}/$REPO_ROOT}"
    OUTPUT_LOG="${OUTPUT_LOG//\{job_id\}/%J}"
  fi
  MEMORY_PER_WORKER=$(grep -E '^\s*memory_per_worker:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  INTERACTIVE=$(grep -E '^\s*interactive:' "$CONFIG_PATH" 2>/dev/null | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  
  # Calculate total slots
  if [[ "$TOTAL_WORKERS" != "auto" ]] && [[ -n "$CPUS_PER_WORKER" ]]; then
    TOTAL_SLOTS=$((TOTAL_WORKERS * CPUS_PER_WORKER))
  else
    TOTAL_SLOTS="auto"
  fi
  
  # Convert boolean strings
  [[ "$INTERACTIVE" == "true" ]] && INTERACTIVE="true" || INTERACTIVE="false"
fi

# --------------------------------------------
# Handle defaults and "auto" for num_workers
# --------------------------------------------
# Set defaults if not set
TOTAL_WORKERS="${TOTAL_WORKERS:-1}"
CPUS_PER_WORKER="${CPUS_PER_WORKER:-1}"

# Handle "auto" for num_workers
if [[ "${TOTAL_WORKERS}" == "auto" ]]; then
  # When execution.num_workers is "auto", use lsf.num_workers
  # This should already be set from the Python parsing, but fallback to 1
  TOTAL_WORKERS="1"
fi

# Calculate total slots
TOTAL_SLOTS=$((TOTAL_WORKERS * CPUS_PER_WORKER))

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

# Number of slots (total CPUs needed)
if [[ -n "${TOTAL_SLOTS:-}" ]]; then
  bsub_args+=(-n "${TOTAL_SLOTS}")
fi

# Output log (skip if interactive mode)
if [[ "${INTERACTIVE:-false}" != "true" ]] && [[ -n "${OUTPUT_LOG:-}" ]]; then
  # Ensure the parent directory exists (LSF won't create nested directories)
  # Note: %J will be replaced by LSF with the actual job ID
  # We need to create the directory structure up to but not including the %J part
  LOG_DIR="$(dirname "${OUTPUT_LOG}")"
  
  # If the path contains %J, create parent directory up to that point
  if [[ "$LOG_DIR" == *"%J"* ]]; then
    # Extract the part before %J
    PARENT_DIR="${LOG_DIR%%/%J*}"
    mkdir -p "${PARENT_DIR}" 2>/dev/null || true
  else
    # No %J in path, create the full directory
    mkdir -p "${LOG_DIR}" 2>/dev/null || true
  fi
  
  bsub_args+=(-o "${OUTPUT_LOG}")
fi

# GPU allocation - always use per-host allocation
# Skip -gpu option if gpus_per_worker is 0 (LSF doesn't allow -gpu num=0)
if [[ -n "${GPUS_PER_WORKER:-}" ]] && [[ "${GPUS_PER_WORKER}" -gt 0 ]]; then
  bsub_args+=(-gpu "num=${GPUS_PER_WORKER}/host:j_exclusive=yes")
fi

# Resource requirements
resource_requirements=()

# Memory reservation - always per host (1 worker per host with ptile)
if [[ -n "${MEMORY_PER_WORKER:-}" ]]; then
  resource_requirements+=("rusage[mem=${MEMORY_PER_WORKER}/host]")
fi

# Span configuration - always use ptile to distribute workers across hosts
if [[ -n "${CPUS_PER_WORKER:-}" ]]; then
  resource_requirements+=("span[ptile=${CPUS_PER_WORKER}]")
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
EXEC_CMD="${REPO_ROOT}/common/run.sh --config '${CONFIG_PATH}' --workload-dir '${SCRIPT_DIR}'"

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

