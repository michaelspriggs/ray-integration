#!/bin/bash
#Run Ray 2.x on LSF.
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

# Use user specific temporary folder for multi-tenancy environment
export RAY_TMPDIR="/tmp/ray-$USER"
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

port=$(getfreeport)
echo "Head node will use port: $port"

export port

dashboard_port=$(getfreeport)
echo "Dashboard will use port: $dashboard_port"

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

# Ray 2.x command with updated flags
command_launch="blaunch -z ${hosts[0]} ray start --head --port $port --dashboard-port $dashboard_port --num-cpus $num_cpu_for_head --object-store-memory $object_store_mem --include-dashboard true --dashboard-host 0.0.0.0"

$command_launch &



sleep 20

command_check_up="ray status --address $head_node:$port"

while ! $command_check_up
do
    sleep 3
done



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

# Run user workload
echo "Running user workload: $user_command"
echo ""
$user_command

exit_code=$?

if [ $exit_code != 0 ]; then
    echo ""
    echo "ERROR: Workload failed with exit code: $exit_code"
    echo "Shutting down Ray cluster..."
    ray stop --address $head_node:$port
    exit $exit_code
else
    echo ""
    echo "SUCCESS: Workload completed successfully"
    echo "Shutting down Ray cluster..."
    ray stop --address $head_node:$port
    echo "Job complete"
    bkill $LSB_JOBID
fi
