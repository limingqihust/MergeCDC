#! /bin/bash
 
host_num=41
host_address=192.168.0.          # 客户端们的IP地址
host_hostname=node     # 客户端们的域名
host_username=root                  # ssh连接的用户，控制端的用户为root
CDC_DIR=/home/Coded-TeraSort-Node

for ((i=0;i<$host_num;i++));do
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${host_hostname}-0${i}
	else
		host=${host_hostname}-$i
	fi
    host_ip=${host_address}$((i + 1))
	echo "${host_ip} ${host} ${host}">>/etc/hosts
done
wait

for ((i=1;i<$host_num;i++));do
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${host_hostname}-0${i}
	else
		host=${host_hostname}-$i
	fi
    host_ip=${host_address}$((i + 1))
	scp /etc/hosts $host_username@$host_ip:/etc
done
wait

# add to 
for ((i=0;i<$host_num;i++));do
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${host_hostname}-0${i}
	else
		host=${host_hostname}-$i
	fi
    host_ip=${host_address}$((i + 1))
/usr/bin/expect << EOF
# 设置捕获字符串后，期待回复的超时时间
set timeout 10

spawn ssh ${host_username}@${host} -o ConnectTimeout=5 "exit"

## 开始进连续捕获
expect	{
        "yes/no" { send "yes\n";  exp_continue }
}
EOF
done

wait
