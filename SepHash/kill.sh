#!/bin/bash

echo "关闭本机server"
pkill -9 ycsb_test

echo "关闭计算节点"
ssh dell@10.26.43.62 "pkill -9 ycsb_test && exit"

ssh dell@10.26.43.63 "pkill -9 ycsb_test && exit"

ssh dell@10.26.43.64 "pkill -9 ycsb_test && exit"

ssh dell@10.26.43.55 "pkill -9 ycsb_test && exit"

