#!/bin/bash
compute_A="dell@192.168.6.2"
compute_B="dell@192.168.6.3"
compute_C="dell@192.168.6.5"
compute_D="dell@192.168.6.4"

workload = $1, machine_number=$2
# mem_start() {
#     mem_shutdown
#     echo "----------mem_start-------------"
#     local workload=$1
#     cd /home/dell/dgy/${workload}
#     cp config_${2}/memory_node_config_${workload}.json config/memory_node_config.json
#     cd /home/dell/dgy/${workload}/build/memory_pool/server/
#     #nohup ./mem_pool >> ${workload}.log &!
#     # 确保memory已经成功启动
#     #sleep 30
# }

# 关闭内存节点
mem_shutdown() {
    echo "---------mem_shutdown------------"
    # 获取第一个行的第一个字段（进程ID）
    # echo $process_id
    # 使用kill命令关闭进程    
    for i in {1..3}
    do
        process_id=$(ps -eT | grep mem_pool | awk 'END {print $1}')
        echo ps -eT|grep mem_pool | awk END
         # 使用kill命令关闭进程
        kill -9 $process_id  # 可以使用-15或-9，分别表示SIGTERM和SIGKILL信号
    done
    
    echo "---------compute_shutdown------------"
    compute_shutdown $compute_A
    compute_shutdown $compute_B
    compute_shutdown $compute_C
    compute_shutdown $compute_D
}

# server = $1, workload = $2, machine_number=$3,thread_number = $4,coro_number=$5,load_type=$6,which_machine=$7
compute_start() {
    echo "------now_run----$2 $4 $5 > ${2}_${3}_${4}_${5}_${6}_${7}.log-----"
    echo $1 "cd /home/dell/dgy/$2/ && 
            cp config_$3/${7}/${6}/${2}_config.json config/${2}_config.json && 
            cp config_$3/${7}/compute_node_config.json config/compute_node_config.json && 
            cd build/compute_pool/run &&
            nohup ./hashrun ${2} ${4} ${5} >> ${2}_${3}_${4}_${5}_${6}_${7}.log &!
            exit 
            "

    ssh -n -f $1 "cd /home/dell/dgy/$2/;
        cp config_$3/${7}/${6}/${2}_config.json config/${2}_config.json;
        cp config_$3/${7}/compute_node_config.json config/compute_node_config.json;
        cd build/compute_pool/run;
        nohup ./hashrun ${2} ${4} ${5} >> /home/dell/log/${2}_${3}_${4}_${5}_${6}_${7}.log 2>&1 &
        exit
        "
}

compute_shutdown() {
    ssh $1 "process_id=\$(ps -eT | grep hashrun | awk 'NR==1 {print \$1}') && kill -9 \$process_id exit"
}

compute_finish() {
    # 踩进去
    ssh $1 "process_id=\$(ps -eT | grep hashrun | awk 'NR==1 {print \$1}')
             while [[ -n \$process_id ]]; do
                 # 每30秒检查一下是否完成
                 sleep 30
                 process_id=\$(ps -eT | grep hashrun | awk 'NR==1 {print \$1}')
             done
             exit"
}

# workload = $1, machine_number = $2, thread_number = $3, coro_number = $4, type=$5 zifpian or uniform
echo ex "$1" "$2" "$3" "$4" "$5"
# 开启内存
# mem_shutdown
# mem_start "${1}" "${2}"
if [ $2 -eq 1 ]; then
    compute_start "$compute_A" "$1" "$2" "$3" "$4" "$5" "a"
    compute_finish "$compute_A"
elif [ $2 -eq 2 ]; then
    compute_start "$compute_A" "$1" "$2" "$3" "$4" "$5" "a"
    compute_start "$compute_B" "$1" "$2" "$3" "$4" "$5" "b"

    compute_finish "$compute_A"
    compute_finish "$compute_B"
else
    compute_start "$compute_A" "$1" "$2" "$3" "$4" "$5" "a"
    compute_start "$compute_B" "$1" "$2" "$3" "$4" "$5" "b"
    compute_start "$compute_C" "$1" "$2" "$3" "$4" "$5" "c"
    compute_start "$compute_D" "$1" "$2" "$3" "$4" "$5" "d"

    compute_finish "$compute_A"
    compute_finish "$compute_B"
    compute_finish "$compute_C"
    compute_finish "$compute_D"
fi
# 关闭内存
mem_shutdown