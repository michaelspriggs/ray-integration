#!/bin/bash

set -euo pipefail

# --------------------------------------------
# Parse arguments
# --------------------------------------------
CONFIG_PATH=""
WORKLOAD_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --workload-dir)
      WORKLOAD_DIR="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 --config CONFIG_FILE --workload-dir WORKLOAD_DIR"
      exit 0
      ;;
    *)
      echo "ERROR: Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

[[ -z "${CONFIG_PATH}" ]] && { echo "Missing --config"; exit 1; }
[[ -z "${WORKLOAD_DIR}" ]] && { echo "Missing --workload-dir"; exit 1; }

CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"
WORKLOAD_DIR="$(cd "$WORKLOAD_DIR" && pwd)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

# --------------------------------------------
# Debug mode
# --------------------------------------------
[[ "${DEBUG:-false}" == "true" ]] && set -x

# --------------------------------------------
# Parse config
# --------------------------------------------
eval "$(
python3 - "$CONFIG_PATH" "$WORKLOAD_DIR" "$REPO_ROOT" <<'PY'
import sys, yaml, shlex, os
from pathlib import Path

cfg = yaml.safe_load(open(sys.argv[1]))
workload_dir = Path(sys.argv[2])
repo_root = sys.argv[3]

execution = cfg.get("execution", {})
data = cfg.get("data", {})
ray_cfg = cfg.get("ray", {})

mode = execution["mode"]

workload_map = {
    "actors": "workload_actors.py",
    "ray_data": "workload.py",
}

if mode not in workload_map:
    raise SystemExit(f"Invalid execution.mode: {mode}")

workload_path = (workload_dir / workload_map[mode]).resolve()

def emit(k, v):
    print(f"{k}={shlex.quote(str(v or ''))}")

emit("WORKLOAD_PATH", workload_path)
emit("CONFIG_PATH_VALUE", sys.argv[1])
emit("MODE", mode)
emit("RAY_OBJ_STORE", ray_cfg.get("object_store_memory_bytes", ""))

# Resolve output_dir
output_dir = data.get("output_dir", "")
if output_dir:
    output_dir = output_dir.replace("{repo_root}", repo_root)
    output_dir = output_dir.replace("{job_id}", os.environ.get("LSB_JOBID", "local"))
    emit("OUTPUT_DIR", output_dir)

PY
)"

# --------------------------------------------
# Output dir
# --------------------------------------------
if [[ -n "${OUTPUT_DIR:-}" ]]; then
  echo "Output directory: ${OUTPUT_DIR}"
  mkdir -p "${OUTPUT_DIR}"
fi

# --------------------------------------------
# Pre-flight
# --------------------------------------------
command -v ray >/dev/null || { echo "ray not found"; exit 1; }

echo ""
echo "=== Run Configuration ==="
echo "Mode:      ${MODE}"
echo "Workload:  ${WORKLOAD_PATH}"
echo "Config:    ${CONFIG_PATH_VALUE}"
echo "CPUs/task: ${CPUS_PER_WORKER:-unknown}"
echo "GPUs/task: ${GPUS_PER_WORKER:-unknown}"
[[ -n "${OUTPUT_DIR:-}" ]] && echo "Output:    ${OUTPUT_DIR}"
echo ""

# --------------------------------------------
# Cleanup
# --------------------------------------------
cleanup() {
  set +e
  echo ""
  echo "Stopping Ray..."
  ray stop || true
}
trap cleanup EXIT

# --------------------------------------------
# Start Ray cluster
# --------------------------------------------
echo "=== Starting Ray cluster ==="

# Optional object store config
[[ -n "${RAY_OBJ_STORE:-}" ]] && export RAY_OBJECT_STORE_MEMORY_BYTES="${RAY_OBJ_STORE}"

# IMPORTANT: limit CPUs per node
RAY_CPUS="${CPUS_PER_WORKER:-1}"

# Determine nodes via rankfile
RANKFILE="${LSB_DJOB_RANKFILE:-}"

if [[ -z "${RANKFILE}" || ! -f "${RANKFILE}" ]]; then
  echo "ERROR: LSB_DJOB_RANKFILE not found"
  exit 1
fi

mapfile -t HOSTS < "${RANKFILE}"

HEAD_NODE="${HOSTS[0]}"

echo "Head node: ${HEAD_NODE}"
echo "Workers: ${#HOSTS[@]}"

PORT=6379

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY RUN] Would start Ray cluster"
else
  # Start head
  ssh "${HEAD_NODE}" "
    ray start --head \
      --port=${PORT} \
      --num-cpus=${RAY_CPUS}
  "

  # Start workers
  for host in "${HOSTS[@]:1}"; do
    ssh "${host}" "
      ray start --address=${HEAD_NODE}:${PORT} \
        --num-cpus=${RAY_CPUS}
    "
  done
fi

RAY_ADDRESS="${HEAD_NODE}:${PORT}"
export RAY_ADDRESS

echo "Ray cluster ready: ${RAY_ADDRESS}"

# --------------------------------------------
# Environment
# --------------------------------------------
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"
export REPO_ROOT
export LSB_JOBID="${LSB_JOBID:-local}"

# --------------------------------------------
# Run workload
# --------------------------------------------
WORKLOAD_CMD=(python "${WORKLOAD_PATH}" --config "${CONFIG_PATH_VALUE}")

echo ""
echo "=== Running Workload ==="
printf ' %q' "${WORKLOAD_CMD[@]}"
echo ""

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  exit 0
fi

"${WORKLOAD_CMD[@]}"

echo ""
echo "=== Completed successfully ==="

