#! /bin/bash
 
host_num=41
host_address=192.168.0.          
host_hostname=node     
host_username=root                  
host_passwd="$1"                 
 
# install expect locally
expect -v &> /dev/null
if [ `echo $?` -ne 0 ];then
	echo "install expect:"
	sudo apt-get update
    sudo apt install -y expect
fi
 
# generate ssh public key locally
echo "";echo ""
echo "########################## generate ssh public key locally ##########################"
if [ `test -a ~/.ssh/id_rsa.pub;echo $?` == 0 ];then
	echo "ssh already been generated"
else
	echo "start generating ssh public key"
/usr/bin/expect << EOF
set timeout 10
 
spawn ssh-keygen -t rsa

expect	{
        "yes/no" { send "yes\n";  exp_continue }
        "s password:"          { send "${host_passwd}\n"; exp_continue }
        ".ssh/id_rsa)"         { send "\n";  exp_continue }
        "Overwrite (y/n)?"     { send "y\n"; exp_continue }
        "no passphrase):"      { send "\n";  exp_continue }
        "passphrase again:"    { send "\n";  exp_continue }
}
expect eof
EOF
fi

# generate ssh public key for remote nodes
for ((i=1;i<$host_num;i++));do
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${host_hostname}-0${i}
	else
		host=${host_hostname}-$i
	fi
    host_ip=${host_address}$((i + 1))
	
    /usr/bin/expect << EOF
    spawn ssh ${host_username}@${host_ip} "ssh-keygen -t rsa"

    expect	{
        "yes/no" { send "yes\n";  exp_continue }
        "s password:"          { send "${host_passwd}\n"; exp_continue }
        ".ssh/id_rsa):"         { send "\n";  exp_continue }
        "Overwrite (y/n)?"     { send "y\n"; exp_continue }
        "no passphrase):"      { send "\n";  exp_continue }
        "passphrase again:"    { send "\n";  exp_continue }
    }
    expect eof
    EOF
done

wait
 
# add locally ssh public key to remote nodes
for ((i=0;i<$host_num;i++));do
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${host_hostname}-0${i}
	else
		host=${host_hostname}-$i
	fi
    host_ip=${host_address}$((i + 1))
/usr/bin/expect << EOF
set timeout 10

# spawn ssh ${host_username}@${host_ip} "cat /root/.ssh/id_rsa.pub>>/root/.ssh/authorized_keys"
spawn ssh-copy-id -i /root/.ssh/id_rsa.pub ${host_username}@${host_ip}

expect	{
        "yes/no" { send "yes\n";  exp_continue }
        "s password:"          { send "${host_passwd}\n"; exp_continue }
}
EOF
	echo "############# add to ${host_ip} #############"
	echo "";echo "";echo ""
done
 
 
flag_ssh=0
# test
for ((i=0;i<$host_num;i++));do
    if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${host_hostname}-0${i}
	else
		host=${host_hostname}-$i
	fi
    host_ip=${host_address}$((i + 1))
	if [ `ssh ${host_username}@${host_ip} -o ConnectTimeout=5 "exit";echo $?` == 0 ];then
		echo "Success: ${host_ip} connected"
	else
		echo "Failed:  ${host_ip}"
		flag_ssh=1
	fi
done
echo "";echo "";echo ""
if [ ${flag_ssh} == 1 ];then
	echo "############# exit #############"
	exit
fi

