# -*- coding: utf-8 -*-

import threading
from fabric import Connection
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("num_servers", type=int, help="Number of servers to run the command on")
parser.add_argument("command_type", choices=["server", "client"], help="Command type to run")
parser.add_argument('cli_num', type=int, help='client数量')
parser.add_argument('coro_num', type=int, help='coro数量')
parser.add_argument('dist_type', type=str, help='分布方式')
parser.add_argument('ycsb_workload', type=str, help='ycsb负载类型')


args = parser.parse_args()

# 然后将 num_servers 用作运行命令的服务器数量
num_servers = args.num_servers
command_type = args.command_type
cli_num = args.cli_num
coro_num = args.coro_num
ycsb_workload = args.ycsb_workload
dist_type = args.dist_type

# Define the connection information for each server
servers = [
    {'host': '10.26.43.62', 'user': 'dell', 'password': '204msjcllm', 'port' : 22},
    {'host': '10.26.43.63', 'user': 'dell', 'password': '204msjcllm', 'port' : 22},
    {'host': '10.26.43.64', 'user': 'dell', 'password': '204msjcllm', 'port' : 22},
    {'host': '10.26.43.52', 'user': 'dell101', 'password': '204msjcllm101', 'port' : 4}
    # Add more servers if needed
]

# Create a list of Connection objects for each server
connections = [Connection(host=server['host'], user=server['user'], connect_kwargs={"password": server['password']}, port=server['port']) for server in servers]

# Define a task to run on all servers
def server_command(i):
    conn = connections[i]
    print(f"server: {conn.host}")
    # conn.run('killall multi_rdma', warn=True)
    result = conn.run(f'cd server && ./ser_cli.sh server {i}') 

def client_command(i):
    conn = connections[i]
    print(f"client: {conn.host}")
    # conn.run('killall ser_cli_var_kv', warn=True)
    # conn.run('free -h', warn=True)
    result = conn.run(f'rm -f insert*.txt search*.txt out.txt core') 
    print(f'cd /home/dell/dgy/ && /home/dell/dgy/my_ycsb_auto.sh {num_servers}  {cli_num} {coro_num} {ycsb_workload} {dist_type} {i}')
    result = conn.run(f'cd /home/dell/dgy/ && /home/dell/dgy/my_ycsb_auto.sh {num_servers}  {cli_num} {coro_num} {ycsb_workload} {dist_type} {i}') 

# Execute the task on all servers
threads = []
for i in range(num_servers):
    if command_type == "server":
        thread = threading.Thread(target=server_command, args=(i,))
    elif command_type == "client":
        thread = threading.Thread(target=client_command, args=(i,))
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()
