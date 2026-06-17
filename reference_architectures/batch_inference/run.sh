#!/bin/bash

set -euo pipefail

# --------------------------------------------
# Paths / config
# --------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-${SCRIPT_DIR}/config.yaml}"

cd "${REPO_ROOT}"

# --------------------------------------------
# Optional debug mode
# --------------------------------------------
if [[ "${DEBUG:-false}" == "true" ]]; then
  set -x
fi

# --------------------------------------------
# Parse config via Python (robust)
# --------------------------------------------
eval "$(
python3 - "$CONFIG_PATH" "$SCRIPT_DIR" <<'PY'
import sys, yaml, shlex
from pathlib import Path

config_path = Path(sys.argv[1]).resolve()
script_dir = Path(sys.argv[2]).resolve()

with open(config_path) as f:
    cfg = yaml.safe_load(f)

execution = cfg.get("execution", {})
ray_cfg = cfg.get("ray", {})

def require(section, key):
    if key not in section or section[key] in (None, ""):
        raise SystemExit(f"Missing required config value: execution.{key}")
    return section[key]

mode = require(execution, "mode")

workloads = {
    "ray_data": "workload.py",
    "actors": "workload_actors.py",
}

if mode not in workloads:
    valid = ", ".join(sorted(workloads))
    raise SystemExit(f"Unsupported execution.mode '{mode}'. Expected: {valid}")

workload_path = (script_dir / workloads[mode]).resolve()

def emit(name, value):
    if value is None:
        value = ""
    print(f"{name}={shlex.quote(str(value))}")

emit("WORKLOAD_PATH", workload_path)
emit("CONFIG_PATH_VALUE", config_path)
emit("RAY_OBJ_STORE", ray_cfg.get("object_store_memory_bytes", ""))
PY
)"

# --------------------------------------------
# Pre-flight checks
# --------------------------------------------
if ! command -v ray >/dev/null 2>&1; then
  echo "ERROR: 'ray' not found in PATH." >&2
  echo "Activate environment with Ray installed." >&2
  exit 1
fi

echo "=== Run Configuration ==="
echo "Python: $(command -v python)"
echo "Ray:    $(command -v ray)"
echo "Config: ${CONFIG_PATH_VALUE}"
echo "Mode:   $(basename "$WORKLOAD_PATH")"
echo ""

# --------------------------------------------
# Cleanup (ALWAYS runs)
# --------------------------------------------
cleanup() {
  set +e
  echo ""
  echo "=== Cleaning up Ray cluster ==="
  "${REPO_ROOT}/common/stop_ray_cluster.sh"
}
trap cleanup EXIT

# --------------------------------------------
# Start cluster
# --------------------------------------------
echo "=== Starting Ray cluster ==="

if [[ -n "${RAY_OBJ_STORE:-}" ]]; then
  export RAY_OBJECT_STORE_MEMORY_BYTES="${RAY_OBJ_STORE}"
fi

# Dry-run support
if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY RUN] Cluster start command:"
  echo "  ${REPO_ROOT}/common/start_ray_cluster.sh"
  echo
else
  # Start the cluster
  "${REPO_ROOT}/common/start_ray_cluster.sh"
  
  # Read RAY_ADDRESS from file
  RAY_INFO_FILE="/tmp/ray-${USER}-${LSB_JOBID}.env"
  
  if [[ ! -f "$RAY_INFO_FILE" ]]; then
    echo "ERROR: Ray info file not found: $RAY_INFO_FILE" >&2
    exit 1
  fi
  
  source "$RAY_INFO_FILE"
  
  if [[ -z "${RAY_ADDRESS:-}" ]]; then
    echo "ERROR: RAY_ADDRESS not set" >&2
    exit 1
  fi
  
  echo "RAY_ADDRESS=${RAY_ADDRESS}"
  echo ""
fi

echo "=== Ray Cluster Status ==="
ray status --address "${RAY_ADDRESS}" || true
echo ""

# --------------------------------------------
# Build workload command safely
# --------------------------------------------
# Add repo root to PYTHONPATH so common module can be imported
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"

WORKLOAD_CMD=(python "${WORKLOAD_PATH}" --config "${CONFIG_PATH_VALUE}")

echo "=== Running Workload ==="
printf ' %q' "${WORKLOAD_CMD[@]}"
echo ""

# Dry-run support
if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "[DRY RUN] Skipping workload execution."
  exit 0
fi

# --------------------------------------------
# Execute workload
# --------------------------------------------
"${WORKLOAD_CMD[@]}"

echo ""
echo "=== Workload completed successfully ==="
