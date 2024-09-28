host_num=32
NODE_NAME=node
USER=root
host_address=192.168.1.


for((i=1; i<=$host_num; i++));
do
{
    if [[ $i -ge 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi
    host_ip=${host_address}$i
    ssh $host "tc qdisc delete dev eth0 root"
} &
done
wait


for((i=2;i<=$host_num;i++));
do
{
    if [[ $i -ge 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi
    # scp /root/MergeCDC/hostfile $USER@$host:/root/MergeCDC/
    # scp -r /root/MergeCDC $USER@$host:/root/
    # scp -r /root/MergeCDC/script/tc.sh  $USER@$host:/root/MergeCDC/script/
    # ssh $host "mkdir -p /root/wordcount-exp3/MergeCDC/script"
    # ssh $host "mkdir -p /root/wordcount-exp3"
    # scp -r /root/wordcount-exp3/MergeCDC $USER@$host:/root/wordcount-exp3/
    # scp -r /root/wordcount-exp3/MergeCDC/script/tc.sh $USER@$host:/root/wordcount-exp3/MergeCDC/script/
    scp /root/wordcount-exp3/MergeCDC/TeraSort $USER@$host:/root/wordcount-exp3/MergeCDC/
} &
done
wait


for((i=1; i<=$host_num; i++));
do
{
    if [[ $i -ge 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi
    host_ip=${host_address}$i
    ssh $host "tc qdisc delete dev eth0 root;chmod +x /root/wordcount-exp3/MergeCDC/script/tc.sh; /root/wordcount-exp3/MergeCDC/script/tc.sh ${host_ip}"

}
done

