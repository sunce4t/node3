#!/bin/bash

machine_number=( 2 1)

thread_number=(16 8 4 2 1 1 1)

coro_number=(4 4 4 4 4 2 1)

load_type=("uniform" "zipfian")
ycsb_type_arr=("a" "b" "d" "f") #"d" swo race需预留空间 
# ycsb_type_arr=("a")

# a W95R5 b W20R80 d W80R20 f W5R95 d-bak W50R50

echo 'now run ex2'
for type in "${load_type[@]}" 
do
    for machine in "${machine_number[@]}"
    do
        if [ $machine -lt 4 ]; then
            for ycsb_type_type in "${ycsb_type_arr[@]}"
            do
                echo 'Now Run: autoRun_SAndC "$machine" 1 2 "$type" "$ycsb_type_type"'
                ./autoRun_SAndC.sh "$machine" 1 2 "$type" "$ycsb_type_type"
            done
        else 
            for num_index in {0..6}
            do
                for ycsb_now_type in "${ycsb_type_arr[@]}"
                do
                    echo 'Now Run: autoRun_SAndC "${machine}" "${thread_number[$num_index]}" "${coro_number[$num_index]}" "${type}" "${ycsb_now_type}'
                    ./autoRun_SAndC.sh "${machine}" "${thread_number[$num_index]}" "${coro_number[$num_index]}" "${type}" "${ycsb_now_type}"
                done
            done
        fi
    done
done