#!/bin/bash
compute_A="dell@192.168.6.2"
compute_B="dell@192.168.6.3"
compute_C="dell@192.168.6.5"
compute_D="dell@192.168.6.4"

# workload = $1, machine_number = $2, thread_number = $3, coro_number = $4
ex() {
    echo "" >> ${1}_trace.log
    echo ./mem_config.sh "$1" "$2" >> ${1}_trace.log
    echo ./single.sh "$1" "$2" "$3" "$4" "$5" >> ${1}_trace.log
    echo "-------------------------------" >> ${1}_trace.log
}

# machine_number=(1 2 4)
machine_number=(4 2 1)
# thread_number=(1 1 1 2 4 8 16)
thread_number=(16 8 4 2 1 1 1)
# coro_number=(2 3 5 5 5 5 5)
coro_number=(5 5 5 5 5 3 2)
load_type=("zipfian" "uniform")

for arg in "$@" 
do
    echo "------------${arg}--------------------"
    for type in "${load_type[@]}" 
    do
        echo "----------${type}-----------------"
        for machine in "${machine_number[@]}"
        do
            if [ $machine -lt 4 ]; then
                ex "$arg" "$machine" 1 2 "$type"
            else 
                for i in {0..6}
                do
                    ex "${arg}" "${machine}" "${thread_number[$i]}" "${coro_number[$i]}" "${type}"
                done
            fi
        done
    done
done