    for i in {1..3}
    do
        process_id=$(ps -eT | grep mem_pool | awk 'END {print $1}')
        echo ps -eT|grep mem_pool | awk END
         # 使用kill命令关闭进程
        kill -9 $process_id  # 可以使用-15或-9，分别表示SIGTERM和SIGKILL信号
    done 