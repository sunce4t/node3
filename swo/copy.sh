#!/bin/bash
compute_A="dell@192.168.6.2"
compute_B="dell@192.168.6.3"
compute_C="dell@192.168.6.5"
compute_D="dell@192.168.6.4"

# 输出所有传递给脚本的参数
for arg in "$@"; do
    ssh ${compute_A} "rm -rf /home/dell/dgy/${arg} exit"
    scp -r /home/dell/dgy/${arg} ${compute_A}:/home/dell/dgy/
    ssh ${compute_A} "cd /home/dell/dgy/${arg} && rm -rf build && ./debug.sh exit"

    ssh ${compute_B} "rm -rf /home/dell/dgy/${arg} exit"
    scp -r /home/dell/dgy/${arg} ${compute_B}:/home/dell/dgy/
    ssh ${compute_B} "cd /home/dell/dgy/${arg} && rm -rf build && ./debug.sh exit"

    ssh ${compute_C} "rm -rf /home/dell/dgy/${arg} exit"
    scp -r /home/dell/dgy/${arg} ${compute_C}:/home/dell/dgy/
    ssh ${compute_C} "cd /home/dell/dgy/${arg} && rm -rf build && ./debug.sh exit"

    ssh ${compute_D} "rm -rf /home/dell/dgy/${arg} exit"
    scp -r /home/dell/dgy/${arg} ${compute_D}:/home/dell/dgy/
    ssh ${compute_D} "cd /home/dell/dgy/${arg} && rm -rf build && ./debug.sh exit"
done