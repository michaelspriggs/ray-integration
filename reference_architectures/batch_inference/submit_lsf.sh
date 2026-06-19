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

# Get output_dir from data section for LSF log file
data = cfg.get("data", {})
if "output_dir" in data:
    output_dir = data["output_dir"]
    output_dir = output_dir.replace("{repo_root}", repo_root)
    output_dir = output_dir.replace("{job_id}", "%J")
    output_log = f"{output_dir}/lsf.log"
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
  
  # Get output_dir from data section for LSF log file
  OUTPUT_DIR=$(grep -A 10 '^data:' "$CONFIG_PATH" 2>/dev/null | grep -E '^\s*output_dir:' | tail -1 | sed 's/.*:\s*//' | tr -d '"' | tr -d "'" || true)
  if [[ -n "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="${OUTPUT_DIR//\{repo_root\}/$REPO_ROOT}"
    OUTPUT_DIR="${OUTPUT_DIR//\{job_id\}/%J}"
    OUTPUT_LOG="${OUTPUT_DIR}/lsf.log"
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
# Generate LSF submission script
# --------------------------------------------
SUBMIT_SCRIPT="/tmp/ray_submit_${USER}_$$.sh"

cat > "${SUBMIT_SCRIPT}" <<'EOF'
#!/bin/bash
EOF

# Add LSF directives (always add them to the script)
[[ -n "${JOB_NAME:-}" ]] && echo "#BSUB -J ${JOB_NAME}" >> "${SUBMIT_SCRIPT}"
[[ -n "${QUEUE:-}" ]] && echo "#BSUB -q ${QUEUE}" >> "${SUBMIT_SCRIPT}"
[[ -n "${TOTAL_SLOTS:-}" ]] && echo "#BSUB -n ${TOTAL_SLOTS}" >> "${SUBMIT_SCRIPT}"

# Output log (skip if interactive mode)
if [[ "${INTERACTIVE:-false}" != "true" ]] && [[ -n "${OUTPUT_LOG:-}" ]]; then
  echo "#BSUB -o ${OUTPUT_LOG}" >> "${SUBMIT_SCRIPT}"
fi

# GPU allocation
if [[ -n "${GPUS_PER_WORKER:-}" ]] && [[ "${GPUS_PER_WORKER}" -gt 0 ]]; then
  echo "#BSUB -gpu \"num=${GPUS_PER_WORKER}/host:j_exclusive=yes\"" >> "${SUBMIT_SCRIPT}"
fi

# Resource requirements
resource_requirements=()
[[ -n "${MEMORY_PER_WORKER:-}" ]] && resource_requirements+=("rusage[mem=${MEMORY_PER_WORKER}/host]")
[[ -n "${CPUS_PER_WORKER:-}" ]] && resource_requirements+=("span[ptile=${CPUS_PER_WORKER}]")

if [[ ${#resource_requirements[@]} -gt 0 ]]; then
  combined_requirements="$(printf "%s " "${resource_requirements[@]}")"
  combined_requirements="${combined_requirements% }"
  echo "#BSUB -R \"${combined_requirements}\"" >> "${SUBMIT_SCRIPT}"
fi

# Add blank line after directives
echo "" >> "${SUBMIT_SCRIPT}"

# Add the execution command
cat >> "${SUBMIT_SCRIPT}" <<EOF
# Execute workload
${REPO_ROOT}/common/run.sh --config '${CONFIG_PATH}' --workload-dir '${SCRIPT_DIR}'
EOF

chmod +x "${SUBMIT_SCRIPT}"

# --------------------------------------------
# Handle dry-run or submission
# --------------------------------------------
if [[ "${DRY_RUN:-false}" == "true" ]]; then
  echo "=== Dry-run mode: Generated submission script ==="
  echo ""
  cat "${SUBMIT_SCRIPT}"
  echo ""
  echo "=== End of submission script ==="
  rm -f "${SUBMIT_SCRIPT}"
  exit 0
fi

# --------------------------------------------
# Submit job
# --------------------------------------------
echo "Submitting job from script: ${SUBMIT_SCRIPT}"
echo ""

if [[ "${INTERACTIVE:-false}" == "true" ]]; then
  # Interactive mode: use bsub -Is with the script
  bsub -Is < "${SUBMIT_SCRIPT}"
  JOB_RESULT=$?
else
  # Batch mode: submit and capture job ID
  SUBMIT_OUTPUT=$(bsub < "${SUBMIT_SCRIPT}" 2>&1)
  JOB_RESULT=$?
  echo "${SUBMIT_OUTPUT}"
  
  # Extract job ID from output (format: "Job <12345> is submitted to queue <normal>.")
  if [[ $JOB_RESULT -eq 0 ]]; then
    JOB_ID=$(echo "${SUBMIT_OUTPUT}" | grep -oP 'Job <\K[0-9]+(?=>)')
    
    if [[ -n "${JOB_ID}" ]] && [[ -n "${OUTPUT_LOG:-}" ]]; then
      # Resolve output directory (replace %J with actual job ID)
      ACTUAL_OUTPUT_DIR="${OUTPUT_LOG//%J/${JOB_ID}}"
      ACTUAL_OUTPUT_DIR="$(dirname "${ACTUAL_OUTPUT_DIR}")"
      
      # Create output directory and move submission script
      mkdir -p "${ACTUAL_OUTPUT_DIR}"
      mv "${SUBMIT_SCRIPT}" "${ACTUAL_OUTPUT_DIR}/submit.sh"
      echo ""
      echo "Submission script saved to: ${ACTUAL_OUTPUT_DIR}/submit.sh"
    else
      # Cleanup if we couldn't determine job ID or output dir
      rm -f "${SUBMIT_SCRIPT}"
    fi
  else
    # Cleanup on failure
    rm -f "${SUBMIT_SCRIPT}"
  fi
fi

exit $JOB_RESULT

