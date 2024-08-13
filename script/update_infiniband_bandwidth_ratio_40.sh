#!/bin/bash


for (( i=1; i<=32; i++ )); do

        if(( i >= 2 && i <= 11 )); then
                continue
        fi

        if (( i >= 1 && i <= 9 )); then
                node="node0$i"
        else
                node="node$i"
        fi

        ssh $node "tc class add dev eth0 parent 1:0 classid 1:10 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.9 classid 1:10"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:11 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.3 classid 1:11"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:12 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.4 classid 1:12"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:13 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.5 classid 1:13"
        # ssh $node "tc class add dev eth0 parent 1:0 classid 1:14 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.6 classid 1:14"

done

for (( i=1; i<=32; i++ )); do

        if(( i >= 12 && i <= 21 )); then
                continue
        fi

        if (( i >= 1 && i <= 9 )); then
                node="node0$i"
        else
                node="node$i"
        fi

        ssh $node "tc class add dev eth0 parent 1:0 classid 1:15 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.12 classid 1:15"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:16 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.13 classid 1:16"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:17 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.14 classid 1:17"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:18 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.15 classid 1:18"
        # ssh $node "tc class add dev eth0 parent 1:0 classid 1:19 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.16 classid 1:19"

done

for (( i=1; i<=32; i++ )); do

        if(( i >= 22 && i <= 31 )); then
                continue
        fi

        if (( i >= 1 && i <= 9 )); then
                node="node0$i"
        else
                node="node$i"
        fi
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:20 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.23 classid 1:20"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:21 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.24 classid 1:21"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:22 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.25 classid 1:22"
        ssh $node "tc class add dev eth0 parent 1:0 classid 1:23 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.25 classid 1:23"
        # ssh $node "tc class add dev eth0 parent 1:0 classid 1:24 htb rate 250Mbit ceil 250Mbit burst 0; tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst 192.168.0.26 classid 1:24"


done

echo "update bandwidth ratio done"