#!/bin/bash

cd /home/dell/sqs/race_share/build/compute_pool/run

# 启动 hashrun 进程并获取其进程ID
./hashrun race 32 5 &
hashrun_pid=$(ps -eT | awk '/hashrun/{print $1}')


cd /home/dell/sqs

# 使用 perf 对 hashrun 进程进行性能分析
perf record -g -p $hashrun_pid

perf script -i perf.data | ./stackcollapse-perf.pl | ./flamegraph.pl > perf.svg
# 其他的 perf 命令和分析步骤...

echo "finished..."