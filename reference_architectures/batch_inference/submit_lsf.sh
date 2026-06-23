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
      echo "  --dry-run              Preview submission script"
      echo "  -h, --help             Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# --------------------------------------------
# Validate required arguments
# --------------------------------------------
if [[ -z "$CONFIG_PATH" ]]; then
  echo "ERROR: --config is required" >&2
  exit 1
fi

CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "ERROR: Configuration file not found: $CONFIG_PATH" >&2
  exit 1
fi

echo "Using configuration file: $CONFIG_PATH"

cd "${REPO_ROOT}"

# --------------------------------------------
# Parse config via Python
# --------------------------------------------
eval "$(
python3 - "$CONFIG_PATH" "$REPO_ROOT" <<'PY'
import sys, yaml, shlex, os

config_path = sys.argv[1]
repo_root = sys.argv[2]

with open(config_path) as f:
    cfg = yaml.safe_load(f)

# ------------------------
# Required sections
# ------------------------
if "lsf" not in cfg:
    raise ValueError("Missing required section: lsf")
if "model" not in cfg:
    raise ValueError("Missing required section: model")
if "data" not in cfg:
    raise ValueError("Missing required section: data")
if "execution" not in cfg:
    raise ValueError("Missing required section: execution")

lsf = cfg["lsf"]
model = cfg["model"]
data = cfg["data"]
execution = cfg["execution"]

# ------------------------
# LSF validation
# ------------------------
required = ["num_workers", "cpus_per_worker", "gpus_per_worker"]
for k in required:
    if k not in lsf:
        raise ValueError(f"Missing lsf.{k}")

num_workers = int(lsf["num_workers"])
cpus_per_worker = int(lsf["cpus_per_worker"])
gpus_per_worker = int(lsf["gpus_per_worker"])

if num_workers <= 0:
    raise ValueError("lsf.num_workers must be > 0")

if cpus_per_worker <= 0:
    raise ValueError("lsf.cpus_per_worker must be > 0")

if gpus_per_worker < 0:
    raise ValueError("lsf.gpus_per_worker must be >= 0")

# ------------------------
# Device validation
# ------------------------
device = execution.get("device", "cpu")

if device == "gpu" and gpus_per_worker == 0:
    raise ValueError("execution.device=gpu but gpus_per_worker=0")

if device == "cpu" and gpus_per_worker > 0:
    print("# WARNING: GPUs allocated but execution.device=cpu", file=sys.stderr)

# ------------------------
# Tensor parallel validation
# ------------------------
tp = int(model.get("tensor_parallel_size", 1))

if gpus_per_worker > 0 and tp != gpus_per_worker:
    raise ValueError(
        f"tensor_parallel_size ({tp}) must equal gpus_per_worker ({gpus_per_worker})"
    )

# ------------------------
# Data validation
# ------------------------
if "output_dir" not in data:
    raise ValueError("data.output_dir is required")

output_dir = data["output_dir"]
output_dir = output_dir.replace("{repo_root}", repo_root)
output_dir = output_dir.replace("{job_id}", "%J")

# Optional: validate input file
input_path = data.get("input_path")
if input_path:
    resolved_input = input_path.replace("{repo_root}", repo_root)
    if not os.path.exists(resolved_input):
        raise FileNotFoundError(f"Input file not found: {resolved_input}")

# ------------------------
# Emit values
# ------------------------
def emit(name, val):
    print(f"{name}={shlex.quote(str(val))}")

emit("NUM_WORKERS", num_workers)
emit("CPUS_PER_WORKER", cpus_per_worker)
emit("GPUS_PER_WORKER", gpus_per_worker)
emit("INTERACTIVE", str(lsf.get("interactive", False)).lower())

if "queue" in lsf:
    emit("QUEUE", lsf["queue"])

if "job_name" in lsf:
    emit("JOB_NAME", lsf["job_name"])

if "memory_per_worker" in lsf:
    emit("MEMORY_PER_WORKER", lsf["memory_per_worker"])

emit("OUTPUT_DIR", output_dir)

PY
)"

if [[ -z "${NUM_WORKERS:-}" ]]; then
  echo "ERROR: Failed to parse config (PyYAML required)" >&2
  exit 1
fi

# --------------------------------------------
# Derived variables
# --------------------------------------------
if [[ -n "${OUTPUT_DIR:-}" ]]; then
  OUTPUT_LOG="${OUTPUT_DIR}/logs/lsf.log"
  mkdir -p "$(dirname "${OUTPUT_LOG}")"
fi

echo "Workers: ${NUM_WORKERS}"
echo "CPUs/worker: ${CPUS_PER_WORKER}"
echo "GPUs/worker: ${GPUS_PER_WORKER}"

# --------------------------------------------
# Generate submission script
# --------------------------------------------
SUBMIT_SCRIPT="/tmp/ray_submit_${USER}_$$.sh"

cat > "${SUBMIT_SCRIPT}" <<EOF
#!/bin/bash
EOF

[[ -n "${JOB_NAME:-}" ]] && echo "#BSUB -J ${JOB_NAME}" >> "${SUBMIT_SCRIPT}"
[[ -n "${QUEUE:-}" ]] && echo "#BSUB -q ${QUEUE}" >> "${SUBMIT_SCRIPT}"
[[ -n "${NUM_WORKERS:-}" ]] && echo "#BSUB -n ${NUM_WORKERS}" >> "${SUBMIT_SCRIPT}"

if [[ "${INTERACTIVE:-false}" != "true" && -n "${OUTPUT_LOG:-}" ]]; then
  echo "#BSUB -o ${OUTPUT_LOG}" >> "${SUBMIT_SCRIPT}"
fi

if [[ -n "${GPUS_PER_WORKER}" && "${GPUS_PER_WORKER}" -gt 0 ]]; then
  echo "#BSUB -gpu \"num=${GPUS_PER_WORKER}/task:j_exclusive=yes\"" >> "${SUBMIT_SCRIPT}"
fi

if [[ -n "${MEMORY_PER_WORKER:-}" ]]; then
  echo "#BSUB -R \"rusage[mem=${MEMORY_PER_WORKER}/task]\"" >> "${SUBMIT_SCRIPT}"
fi

if [[ -n "${CPUS_PER_WORKER:-}" ]]; then
  echo "#BSUB -R \"affinity[core(${CPUS_PER_WORKER})]\"" >> "${SUBMIT_SCRIPT}"
fi

echo "" >> "${SUBMIT_SCRIPT}"

cat >> "${SUBMIT_SCRIPT}" <<EOF

# Pass resource info to Ray
export CPUS_PER_WORKER=${CPUS_PER_WORKER}
export GPUS_PER_WORKER=${GPUS_PER_WORKER}

${REPO_ROOT}/common/run.sh --config '${CONFIG_PATH}' --workload-dir '${SCRIPT_DIR}'
EOF

chmod +x "${SUBMIT_SCRIPT}"

# --------------------------------------------
# Dry-run
# --------------------------------------------
if [[ "${DRY_RUN}" == "true" ]]; then
  echo "=== Dry-run: Submission script ==="
  cat "${SUBMIT_SCRIPT}"
  rm -f "${SUBMIT_SCRIPT}"
  exit 0
fi

# --------------------------------------------
# Submit job
# --------------------------------------------
echo "Submitting job..."
echo ""

if [[ "${INTERACTIVE:-false}" == "true" ]]; then
  TEMP_OUTPUT="/tmp/bsub_${USER}_$$.txt"
  bsub -Is < "${SUBMIT_SCRIPT}" 2>&1 | tee "${TEMP_OUTPUT}"
  JOB_RESULT=${PIPESTATUS[0]}
  JOB_ID=$(grep -oE 'Job <[0-9]+>' "${TEMP_OUTPUT}" | grep -oE '[0-9]+' | head -1)
  rm -f "${TEMP_OUTPUT}"
else
  SUBMIT_OUTPUT=$(bsub < "${SUBMIT_SCRIPT}" 2>&1)
  JOB_RESULT=$?
  echo "${SUBMIT_OUTPUT}"
  JOB_ID=$(echo "${SUBMIT_OUTPUT}" | grep -oE 'Job <[0-9]+>' | grep -oE '[0-9]+')
fi

# --------------------------------------------
# Post-submission handling (UNIFIED)
# --------------------------------------------
if [[ "${JOB_RESULT}" -eq 0 && -n "${JOB_ID:-}" && -n "${OUTPUT_DIR:-}" ]]; then
  ACTUAL_OUTPUT_DIR="${OUTPUT_DIR//%J/${JOB_ID}}"

  mkdir -p "${ACTUAL_OUTPUT_DIR}"
  mv "${SUBMIT_SCRIPT}" "${ACTUAL_OUTPUT_DIR}/submit.sh"

  echo ""
  echo "Job ID: ${JOB_ID}"
  echo "Output directory: ${ACTUAL_OUTPUT_DIR}"
else
  rm -f "${SUBMIT_SCRIPT}"
fi

exit ${JOB_RESULT}

