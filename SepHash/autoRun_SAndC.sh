# /bin/bash
# usage: 
#       server: ../ser_cli.sh server
#       client: ../ser_cli.sh machine_id num_cli num_coro num_machine
#       client_0: ../ser_cli.sh 0 1 1 2
#       client_1: ../ser_cli.sh 1 1 1 2
#       num_cli : 0~4
#       num_coro : 1~4

memory_start() {
    ./kill.sh  2>&1 | sed '//d'
    # 启动Server
    echo "server"
    # ./ser_cli_var_kv --server \
    nohup ./build/ycsb_test --server \
    --gid_idx 1 \
    --max_coro 256 --cq_size 64 \
    --mem_size 182536110080 > temp.log &
    # 确保内存成功启动
    sleep 60
}

machine_num="$1"
thread_num="$2"
coro_num="$3"
ycsb_workload="$4"
dist_type="$5"

#启动内存并且关闭其他计算节点的client
memory_start

#启动各个计算节点
python3 ./my_ycsb.py $machine_num client $thread_num $coro_num $ycsb_workload $dist_type