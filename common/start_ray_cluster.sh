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
# Build host list
# --------------------------------------------
mapfile -t hosts < <(sort "$LSB_DJOB_HOSTFILE" | uniq)
echo "Hosts: ${hosts[*]}"

# Count cores per host
declare -A cores_per_host
while read -r h; do
  ((cores_per_host[$h]++))
done < "$LSB_DJOB_HOSTFILE"

for h in "${!cores_per_host[@]}"; do
  echo "Host $h has ${cores_per_host[$h]} CPU slots"
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
num_cpu_head="${cores_per_host[$head_node]}"

echo "Starting Ray head..."
blaunch -z "$head_node" \
  ray start --head \
    --port "$port" \
    --dashboard-port "$dashboard_port" \
    --num-cpus "$num_cpu_head" \
    --object-store-memory "$object_store_mem" \
    --dashboard-host 0.0.0.0 \
    --node-ip-address "$head_node_ip" \
  &

sleep 10

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
  echo "Starting worker on $host"

  num_cpu="${cores_per_host[$host]}"

  blaunch -z "$host" \
    ray start \
      --address "$head_node_ip:$port" \
      --num-cpus "$num_cpu" \
      --object-store-memory "$object_store_mem" \
    &

  # Wait for worker
  until blaunch -z "$host" ray status --address "$head_node_ip:$port" >/dev/null 2>&1; do
    echo "Waiting for worker $host..."
    sleep 3
  done

  echo "Worker $host joined ✔"
done

# --------------------------------------------
# Export connection info
# --------------------------------------------
export RAY_ADDRESS="$head_node_ip:$port"
echo "RAY_ADDRESS=$RAY_ADDRESS"

echo ""
echo "=== Ray Cluster Ready ==="
ray status --address "$RAY_ADDRESS"
echo ""

# Return control (IMPORTANT: no workload here!)
``
