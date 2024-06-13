# /bin/bash
# usage: 
#       server: ../ser_cli.sh server
#       client: ../ser_cli.sh machine_id num_cli num_coro num_machine
#       client_0: ../ser_cli.sh 0 1 1 2
#       client_1: ../ser_cli.sh 1 1 1 2
#       num_cli : 0~4
#       num_coro : 1~4

machine_num="$1"
thread_num="$2"
coro_num="$3"
ycsb_workload="$4"
dist_type="$5"
machine_id="$6"

ycsb_load="/home/dell/yzx/ycsb_output/$machine_num-node/$dist_type/ycsb_load"
ycsb_run="/home/dell/yzx/ycsb_output/$machine_num-node/$dist_type/$ycsb_workload/ycsb" 
for num_cli in `seq $thread_num $thread_num`;do
    for num_coro in `seq $coro_num $coro_num`;do
        for load_num in 100000000;do
            echo "num_cli" $num_cli "num_coro" $num_coro "load_num" $load_num
            out_file=${dist_type}_${ycsb_workload}_${machine_num}_${thread_num}_${coro_num}.txt;

            pkill -9 ycsb_test

            ./ycsb_test \
            --server_ip 10.26.43.56 --num_machine $machine_num --num_cli $num_cli --num_coro $num_coro \
            --gid_idx 1 \
            --max_coro 256 --cq_size 64 \
            --machine_id $machine_id  \
            --load_num $load_num \
            --num_op 100000000 \
            --pattern_type 1 \
            --insert_frac 0.1 \
            --read_frac   0.9 \
            --update_frac  0.0 \
            --delete_frac  0.0 \
            --read_size     64 \
            --ycsb_load $ycsb_load \
            --ycsb_run $ycsb_run >> $out_file
        done 
    done
done
