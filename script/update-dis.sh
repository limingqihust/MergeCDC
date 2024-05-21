#!/bin/bash

# parameters
host_num=41
NODE_NAME=node
USER=root
HOME=/home
host_address=192.168.0.          
CDC_DIR=$HOME/Coded-TeraSort-Node

# update distribution
for((i=1;i<$host_num;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}-0${i}
	else
		host=${NODE_NAME}-$i
	fi

    scp $CDC_DIR/Distribution/combination_my $USER@$host:$CDC_DIR/Distribution
    scp $CDC_DIR/Distribution/Distribution $USER@$host:$CDC_DIR/Distribution
} &
done 
wait

for ((i=1;i<$host_num;i++));do
    host_ip=${host_address}$((i + 1))
	scp $CDC_DIR/tc.sh $USER@$host_ip:$CDC_DIR
    ssh $USER@${host_ip} "tc qdisc delete dev eth0 root;cd $CDC_DIR;chmod +x ./tc.sh; ./tc.sh ${host_ip}"
done
wait
