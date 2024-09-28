#! /bin/bash
 
host_num=32
host_address=192.168.1.          
host_hostname=node     
host_username=root                 
self_ip="$1"

tc qdisc add dev eth0 root handle 1:0 htb default 2

tc class add dev eth0 parent 1:0 classid 1:1 htb rate 200Mbit ceil 200Mbit burst 0
for((i=1;i<=32;i++)); do
    host_ip=${host_address}${i}
    tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst ${host_ip} classid 1:1

done
wait
