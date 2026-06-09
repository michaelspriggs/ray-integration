#!/bin/bash
#Run Ray 2.x on LSF.
#
# Architecture:
#   - One Ray process (head or worker) per host allocated by LSF
#   - Each process uses the number of CPU cores allocated to that host
#   - Core counts determined from LSB_DJOB_HOSTFILE (one line per core)
#   - GPUs auto-detected via CUDA_VISIBLE_DEVICES set by LSF
#   - All processes launched with blaunch for proper LSF tracking
#
#Examples:
# CPU-only:
#   bsub -n 8 -o output.%J ./ray_launch_cluster.sh -n ray_cpu -c "python workload.py" -m 20000000000
#
# GPU with exclusive access:
#   bsub -n 8 -gpu "num=1/task:j_exclusive=yes" -o output.%J ./ray_launch_cluster.sh -n ray_gpu -c "python workload.py" -m 20000000000
#
# Optional LSF parameters:
#   -q queue_name          # Specify queue
#   -M 100GB               # Memory limit
#   -W 2:00                # Wall time limit
#   -R "rusage[mem=10GB]"  # Memory reservation
echo "=== Ray 2.x on LSF Cluster Setup ==="
echo "LSB_MCPU_HOSTS=$LSB_MCPU_HOSTS"
echo "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"
echo ""
echo "---- LSB_DJOB_HOSTFILE=$LSB_DJOB_HOSTFILE"
cat $LSB_DJOB_HOSTFILE
echo "---- End of LSB_DJOB_HOSTFILE"
echo ""

# Use user and job-specific temporary folder to avoid conflicts
# when same user runs multiple Ray clusters
export RAY_TMPDIR="/tmp/ray-$USER-$LSB_JOBID"
echo "RAY_TMPDIR=$RAY_TMPDIR"
mkdir -p $RAY_TMPDIR

#bias to selection of higher range ports
function getfreeport()
{
    CHECK="do while"
    while [[ ! -z $CHECK ]]; do
        port=$(( ( RANDOM % 40000 )  + 20000 ))
        CHECK=$(netstat -a | grep $port)
    done
    echo $port
}

while getopts ":c:n:m:" option;do
    case "${option}" in
    c) c=${OPTARG}
        user_command=$c
    ;;
    n) n=${OPTARG}
        conda_env=$n
    ;;
    m) m=${OPTARG}
        object_store_mem=$m
    ;;
    *) echo "Did not supply the correct arguments"
    ;;
    esac
    done



#use bash -i to activate conda env when the script is launched
#or use the below syntax.
if [ -z "$conda_env" ]
then
    echo "No conda env provided, is ray installed?"
else

    eval "$(conda shell.bash hook)"
    conda activate $conda_env
fi

hosts=()
for host in `cat $LSB_DJOB_HOSTFILE | uniq`
do
        echo "Adding host: $host"
        hosts+=($host)
done

echo "The host list is: ${hosts[@]}"

# Compute number of cores allocated to hosts from LSB_DJOB_HOSTFILE
# Each line in the file represents one slot (CPU core) allocated to a host
declare -A associative

for host in `cat $LSB_DJOB_HOSTFILE | uniq`
do
    num_slots=`grep -c "^$host$" $LSB_DJOB_HOSTFILE`
    associative[$host]=$num_slots
done

for host in ${!associative[@]}; do
    echo "host=$host cores=${associative[$host]}"
done

#Assumption only one head node and more than one
#workers will connect to head node

head_node=${hosts[0]}

export head_node

echo "Starting Ray head node on: ${hosts[0]}"

if [ -z $object_store_mem ]
then
    echo "Using default object store memory of 4GB"
    object_store_mem=4000000000
else
    echo "Object store memory set to: $object_store_mem bytes"
fi

num_cpu_for_head=${associative[$head_node]}

# Ray 2.x will automatically detect GPUs via CUDA_VISIBLE_DEVICES set by LSF
# No need to manually specify --num-gpus
echo "Ray will auto-detect GPUs from CUDA_VISIBLE_DEVICES"
echo "Head node CPUs: $num_cpu_for_head"

# Retry loop for starting Ray head node with port selection
max_retries=5
retry_count=0
ray_started=false

while [ "$ray_started" = false ] && [ $retry_count -lt $max_retries ]; do
    retry_count=$((retry_count + 1))
    
    # Select new ports for each attempt
    port=$(getfreeport)
    dashboard_port=$(getfreeport)
    
    echo "Attempt $retry_count/$max_retries: Trying port $port (dashboard: $dashboard_port)"
    
    # Ray 2.x command with updated flags
    command_launch="blaunch -z ${hosts[0]} ray start --head --port $port --dashboard-port $dashboard_port --num-cpus $num_cpu_for_head --object-store-memory $object_store_mem --include-dashboard true --dashboard-host 0.0.0.0"
    
    echo "Launching Ray head node..."
    $command_launch &
    
    sleep 20
    
    # Check if Ray head is ready
    command_check_up="ray status --address $head_node:$port"
    check_attempts=0
    max_check_attempts=3
    
    while [ $check_attempts -lt $max_check_attempts ]; do
        if $command_check_up 2>/dev/null; then
            echo "Ray head node is ready on port $port!"
            ray_started=true
            export port
            break
        fi
        check_attempts=$((check_attempts + 1))
        echo "Checking Ray status (attempt $check_attempts/$max_check_attempts)..."
        sleep 3
    done
    
    if [ "$ray_started" = false ]; then
        echo "Failed to start Ray on port $port, will retry with new port..."
        # Stop any partially started Ray processes
        ray stop 2>/dev/null || true
        sleep 5
    fi
done

if [ "$ray_started" = false ]; then
    echo "ERROR: Failed to start Ray head node after $max_retries attempts"
    echo "Check for port conflicts or other issues in the output above."
    exit 1
fi



workers=("${hosts[@]:1}")

echo "adding the workers to head node: ${workers[*]}"
# Run ray on worker nodes and connect to head
for host in "${workers[@]}"
do
    echo "Starting worker on: $host, connecting to head node: $head_node"

    sleep 10
    num_cpu=${associative[$host]}
    
    # Ray 2.x worker command - GPUs auto-detected via CUDA_VISIBLE_DEVICES
    command_for_worker="blaunch -z $host ray start --address $head_node:$port --num-cpus $num_cpu --object-store-memory $object_store_mem"
    
    echo "Worker command: $command_for_worker"
    $command_for_worker &
    
    sleep 10
    command_check_up_worker="blaunch -z $host ray status --address $head_node:$port"
    while ! $command_check_up_worker
    do
        echo "Waiting for worker $host to join cluster..."
        sleep 3
    done
    echo "Worker $host successfully joined the cluster"
done

# Display cluster status before running workload
echo ""
echo "=== Ray Cluster Status ==="
ray status --address $head_node:$port
echo ""

# Run user workload with blaunch for LSF tracking
echo "Running user workload: $user_command"
echo ""
blaunch -z $head_node $user_command

exit_code=$?

if [ $exit_code != 0 ]; then
    echo ""
    echo "ERROR: Workload failed with exit code: $exit_code"
    echo "Shutting down Ray cluster..."
    ray stop
    exit $exit_code
else
    echo ""
    echo "SUCCESS: Workload completed successfully"
    echo "Shutting down Ray cluster..."
    ray stop
    echo "Job complete"
fi
