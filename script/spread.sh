#! /bin/bash
 
host_num=41
host_address=192.168.0.          
host_hostname=node     
host_username=root                  
CDC_DIR=/home/Coded-TeraSort-Node

# install expect locally
expect -v &> /dev/null
if [ `echo $?` -ne 0 ];then
	echo "install expect:"
	sudo apt-get update
    sudo apt install -y expect
fi

# apply ssh.sh to each nodes
for ((i=1;i<$host_num;i++));do
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${host_hostname}-0${i}
	else
		host=${host_hostname}-$i
	fi
    host_ip=${host_address}$((i + 1))
	scp $CDC_DIR/install/ssh.sh $host_username@$host_ip:/home
    ssh ${host_username}@${host_ip} "cd /home;chmod +x ./ssh.sh; ./ssh.sh"
done
wait