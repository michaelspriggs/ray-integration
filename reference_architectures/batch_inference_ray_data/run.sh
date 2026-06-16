#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-${SCRIPT_DIR}/config.yaml}"

cd "${REPO_ROOT}"

eval "$(
python - <<'PY' "${CONFIG_PATH}" "${SCRIPT_DIR}"
import shlex
import sys
from pathlib import Path

config_path = Path(sys.argv[1]).resolve()
script_dir = Path(sys.argv[2]).resolve()

lsf = {}
in_lsf = False

with open(config_path, "r") as f:
    for raw_line in f:
        line = raw_line.rstrip("\n")
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        if not line.startswith(" "):
            in_lsf = stripped == "lsf:"
            continue

        if not in_lsf:
            continue

        if not line.startswith("  "):
            continue

        entry = stripped
        if ":" not in entry:
            continue

        key, value = entry.split(":", 1)
        key = key.strip()
        value = value.strip()

        if not value:
            parsed = ""
        elif value[0] in ("'", '"') and value[-1] == value[0]:
            parsed = value[1:-1]
        else:
            parsed = value

        lsf[key] = parsed

def require(name):
    value = lsf.get(name)
    if value in (None, ""):
        raise SystemExit(f"Missing required config value: lsf.{name}")
    return value

workload_script = require("workload_script")
ray_object_store_memory_bytes = int(require("ray_object_store_memory_bytes"))

workload_path = (script_dir / workload_script).resolve()

def emit(name, value):
    print(f"{name}={shlex.quote(str(value))}")

emit("WORKLOAD_PATH", workload_path)
emit("CONFIG_PATH_VALUE", config_path)
emit("RAY_OBJECT_STORE_MEMORY_BYTES", ray_object_store_memory_bytes)
PY
)"

if ! command -v ray >/dev/null 2>&1; then
  echo "ERROR: 'ray' is not available in PATH." >&2
  echo "Run this workflow from an already-activated environment that contains Ray and the workload dependencies." >&2
  exit 1
fi

echo "Using python: $(command -v python)"
echo "Using ray: $(command -v ray)"

echo "Starting Ray cluster and running workload..."
"${REPO_ROOT}/common/start_ray_cluster.sh" \
  -c "python ${WORKLOAD_PATH} --config ${CONFIG_PATH_VALUE}" \
  -m "${RAY_OBJECT_STORE_MEMORY_BYTES}"

