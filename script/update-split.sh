#!/bin/bash

# parameters
NODE_NUM=41
NODE_NAME=node
USER=root
HOME=/home
CDC_DIR=$HOME/Coded-TeraSort-Node

# compile
make clean
make

# dispatch
for((i=1;i<$NODE_NUM;i++))
do
{
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}-0${i}
	else
		host=${NODE_NAME}-$i
	fi

    scp $CDC_DIR/Splitter $USER@$host:$CDC_DIR/
    scp $CDC_DIR/TeraSort $USER@$host:$CDC_DIR/
    scp $CDC_DIR/CodedTeraSort $USER@$host:$CDC_DIR/
} &
done

wait

# split
for((i=0;i<$NODE_NUM;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}-0${i}
	else
		host=${NODE_NAME}-$i
	fi

    ssh $USER@$host "cd $CDC_DIR; ./Splitter"
} &
done 

wait