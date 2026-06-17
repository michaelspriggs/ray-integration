#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-${SCRIPT_DIR}/config.yaml}"

cd "${REPO_ROOT}"

# --------------------------------------------
# Parse config.yaml using Python + PyYAML
# --------------------------------------------
eval "$(
python3 - "$CONFIG_PATH" <<'PY'
import sys
import yaml
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
# Build bsub command
# --------------------------------------------
bsub_args=()

# Basic options
[[ -n "${JOB_NAME:-}" ]] && bsub_args+=(-J "${JOB_NAME}")
[[ -n "${QUEUE:-}" ]] && bsub_args+=(-q "${QUEUE}")
[[ -n "${TOTAL_WORKERS:-}" ]] && bsub_args+=(-n "${TOTAL_WORKERS}")
[[ -n "${OUTPUT_LOG:-}" ]] && bsub_args+=(-o "${OUTPUT_LOG}")

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
echo "Submitting job with command:"
printf ' %q' bsub "${bsub_args[@]}" "${SCRIPT_DIR}/run.sh"
echo

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "Dry-run enabled. Not submitting."
  exit 0
fi

# --------------------------------------------
# Submit job
# --------------------------------------------
bsub "${bsub_args[@]}" "${SCRIPT_DIR}/run.sh"

