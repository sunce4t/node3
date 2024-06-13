# usage: 
#       ./sync.sh out num_server
#       ./sync.sh in num_server cli_num(1,2,4,8,16) coro_num(1,2,3,4)
clis=("10.26.43.62" "10.26.43.63" "10.26.43.64" "10.26.43.52")
if [ "$1" = "out" ]
then
    make ser_cli_var_kv
    make ser_cli
    make ycsb_test 
    make my_test
    # $2 run nodes
    port=22
    user=dell
    for cli in ${clis[@]:0:$2}
    do 
        if [ "$cli" = "10.26.43.52" ]; then
            port=4
            user=dell101
        fi
        echo "cli" $cli
        scp -P $port ./ser_cli $user@$cli:/home/$user/dgy
        scp -P $port ./ser_cli_var_kv $user@$cli:/home/$user/dgy
        scp -P $port ./ycsb_test $user@$cli:/home/$user/dgy
        scp -P $port ./my_test $user@$cli:/home/$user/dgy
        scp -P $port ../ser_cli.sh $user@$cli:/home/$user/dgy
        scp -P $port ../run.sh $user@$cli:/home/$user/dgy
        scp -P $port ../ycsb.sh $user@$cli:/home/$user/dgy
        scp -P $port ../*.sh ../*.py $user@$cli:/home/$user/dgy
    done
else
    cnt=1
    rm -f ./insert_lat*192.168*.txt
    rm -f ./search_lat*192.168*.txt
    for cli in ${clis[@]:0:$2}
    do 
        echo "cli" $cli
        rm -f ./out$cli.txt
        scp dell@$cli:/home/dell/dgy/out.txt ./out$cli.txt
        cli_num=$3
        coro_num=$4
        for ((cli_id=0; cli_id<cli_num; cli_id++)); do
            for ((coro_id=0; coro_id<coro_num; coro_id++)); do
                # 设置要匹配的文件名
                filename="insert_lat${cli_id}${coro_id}.txt"
                echo $filename
                # 执行SCP命令，从远程服务器复制文件到本地并修改文件名
               scp "dell@$cli:/home/dell/dgy/$filename" "./insert_lat${cli_id}${coro_id}$cli.txt"
                
                filename="search_lat${cli_id}${coro_id}.txt"
                echo $filename
                # 执行SCP命令，从远程服务器复制文件到本地并修改文件名
                scp "dell@$cli:/home/dgy/$filename" "./search_lat${cli_id}${coro_id}$cli.txt"
            done
        done
        ((cnt += 1))
    done
fi
