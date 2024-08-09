NODE_NUM=32
NODE_NAME=node
USER=root
host_address=192.168.0.


for((i=1;i<=$NODE_NUM;i++));
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		host=${NODE_NAME}$i
	fi

    ssh $USER@$host "killall TeraSort; killall CodedTeraSort"
} &
done
wait