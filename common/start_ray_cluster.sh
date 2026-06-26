#!/bin/bash
set -euo pipefail

echo "=== Ray Cluster Bootstrap (LSF) ==="

# --------------------------------------------
# Environment + temp dir
# --------------------------------------------
export RAY_TMPDIR="/tmp/ray-$USER-$LSB_JOBID"
mkdir -p "$RAY_TMPDIR"
echo "RAY_TMPDIR=$RAY_TMPDIR"

echo ""
echo "LSB_DJOB_HOSTFILE=$LSB_DJOB_HOSTFILE"
cat "$LSB_DJOB_HOSTFILE"
echo ""

# --------------------------------------------
# Resource configuration from LSF
# --------------------------------------------
CPUS_PER_WORKER="${CPUS_PER_WORKER:-1}"
GPUS_PER_WORKER="${GPUS_PER_WORKER:-0}"

echo "Resource allocation:"
echo "  CPUs per worker: ${CPUS_PER_WORKER}"
echo "  GPUs per worker: ${GPUS_PER_WORKER}"
echo ""

# --------------------------------------------
# Build host list
# --------------------------------------------
# Get unique hosts
mapfile -t hosts < <(sort "$LSB_DJOB_HOSTFILE" | uniq)
echo "Unique hosts: ${hosts[*]}"

# Count total tasks (CPU slots) per host
declare -A cores_per_host=()

while read -r h; do
  cores_per_host["$h"]=$(( ${cores_per_host["$h"]:-0} + 1 ))
done < "$LSB_DJOB_HOSTFILE"

echo "Tasks per host:"
for h in "${!cores_per_host[@]}"; do
  echo "  $h: ${cores_per_host[$h]} tasks"
done

# --------------------------------------------
# Select head node
# --------------------------------------------
head_node="${hosts[0]}"

# Resolve IPv4
head_node_ip=$(getent hosts "$head_node" | awk '{print $1}' | grep -E '^[0-9]+\.' | head -1 || true)
if [[ -z "$head_node_ip" ]]; then
  head_node_ip=$(hostname -I | awk '{print $1}')
fi

echo "Head node: $head_node ($head_node_ip)"

# --------------------------------------------
# Ports
# --------------------------------------------
get_free_port() {
  while true; do
    port=$((RANDOM % 40000 + 20000))
    ! ss -ltn | grep -q ":$port " && break
  done
  echo "$port"
}

port=$(get_free_port)
dashboard_port=$(get_free_port)

echo "Using ports: $port (dashboard: $dashboard_port)"

# --------------------------------------------
# Object store memory
# --------------------------------------------
object_store_mem="${RAY_OBJECT_STORE_MEMORY_BYTES:-4000000000}"
echo "Object store memory: $object_store_mem"

# --------------------------------------------
# Start head
# --------------------------------------------
# Calculate total CPUs: workers on this host × CPUs per worker
num_workers_head="${cores_per_host[$head_node]}"
num_cpu_head=$(( num_workers_head * CPUS_PER_WORKER ))

echo "Starting Ray head on ${head_node}..."
echo "  Workers: ${num_workers_head}"
echo "  Total CPUs: ${num_cpu_head}"

# Build Ray start command
ray_head_cmd="ray start --head \
  --port $port \
  --dashboard-port $dashboard_port \
  --num-cpus $num_cpu_head \
  --object-store-memory $object_store_mem \
  --dashboard-host 0.0.0.0 \
  --node-ip-address $head_node_ip"

# Add GPU configuration if GPUs are allocated
if [[ $GPUS_PER_WORKER -gt 0 ]]; then
  # Count GPUs from CUDA_VISIBLE_DEVICES (set by LSF)
  # This will be evaluated on the remote host via blaunch
  ray_head_cmd="export CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-}; \
    num_gpus=\$(echo \"\${CUDA_VISIBLE_DEVICES}\" | grep -o ',' | wc -l); \
    num_gpus=\$((num_gpus + 1)); \
    [[ -z \"\${CUDA_VISIBLE_DEVICES}\" ]] && num_gpus=0; \
    echo \"  GPUs detected: \${num_gpus}\"; \
    ${ray_head_cmd} --num-gpus \${num_gpus}"
fi

blaunch -z "$head_node" "${ray_head_cmd}" &

sleep 3

# Wait for head
echo "Waiting for Ray head..."
until ray status --address "$head_node_ip:$port" >/dev/null 2>&1; do
  sleep 2
done

echo "Head ready ✔"

# --------------------------------------------
# Start workers
# --------------------------------------------
workers=("${hosts[@]:1}")

for host in "${workers[@]}"; do
  # Calculate total CPUs: workers on this host × CPUs per worker
  num_workers_host="${cores_per_host[$host]}"
  num_cpu=$(( num_workers_host * CPUS_PER_WORKER ))

  echo "Starting worker on ${host}..."
  echo "  Workers: ${num_workers_host}"
  echo "  Total CPUs: ${num_cpu}"

  # Build Ray start command
  ray_worker_cmd="ray start \
    --address $head_node_ip:$port \
    --num-cpus $num_cpu \
    --object-store-memory $object_store_mem"

  # Add GPU configuration if GPUs are allocated
  if [[ $GPUS_PER_WORKER -gt 0 ]]; then
    # Count GPUs from CUDA_VISIBLE_DEVICES (set by LSF)
    ray_worker_cmd="export CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-}; \
      num_gpus=\$(echo \"\${CUDA_VISIBLE_DEVICES}\" | grep -o ',' | wc -l); \
      num_gpus=\$((num_gpus + 1)); \
      [[ -z \"\${CUDA_VISIBLE_DEVICES}\" ]] && num_gpus=0; \
      echo \"  GPUs detected: \${num_gpus}\"; \
      ${ray_worker_cmd} --num-gpus \${num_gpus}"
  fi

  blaunch -z "$host" "${ray_worker_cmd}" &

  # Wait for worker
  sleep 3
  until blaunch -z "$host" ray status --address "$head_node_ip:$port" >/dev/null 2>&1; do
    echo "Waiting for worker $host..."
    sleep 2
  done

  echo "Worker $host joined ✔"
done

# --------------------------------------------
# Write connection info to file for parent script
# --------------------------------------------
RAY_ADDRESS="$head_node_ip:$port"
RAY_INFO_FILE="/tmp/ray-${USER}-${LSB_JOBID}.env"

echo "RAY_ADDRESS=${RAY_ADDRESS}" > "$RAY_INFO_FILE"

echo ""
echo "=== Ray Cluster Ready ==="
ray status --address "$RAY_ADDRESS"
echo ""
echo "Ray address written to: $RAY_INFO_FILE"

# Return control (IMPORTANT: no workload here!)
