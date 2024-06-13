# -*- coding: utf-8 -*-

import threading
from fabric import Connection
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("num_servers", type=int, help="Number of servers to run the command on")
parser.add_argument("command_type", choices=["server", "client"], help="Command type to run")
parser.add_argument('cli_num', type=int, help='client数量')
parser.add_argument('coro_num', type=int, help='coro数量')

args = parser.parse_args()

# 然后将 num_servers 用作运行命令的服务器数量
num_servers = args.num_servers
command_type = args.command_type
cli_num = args.cli_num
coro_num = args.coro_num

# Define the connection information for each server
servers = [
    {'host': '10.26.43.62', 'user': 'dell', 'password': '204msjcllm'},
    {'host': '10.26.43.63', 'user': 'dell', 'password': '204msjcllm'},
    {'host': '10.26.43.55', 'user': 'dell', 'password': '204msjcllm'},
    {'host': '10.26.43.64', 'user': 'dell', 'password': '204msjcllm'}
    # Add more servers if needed
]

# Create a list of Connection objects for each server
connections = [Connection(host=server['host'], user=server['user'], connect_kwargs={"password": server['password']}) for server in servers]

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
    result = conn.run(f'cd /home/dell/dgy/ && /home/dell/dgy/run.sh {i} {cli_num} {coro_num} {num_servers} {}') 

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
