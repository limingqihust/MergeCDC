host_num=32
NODE_NAME=node
USER=root
host_address=192.168.0.
# update 
for((i=2;i<=$host_num;i++));
do
{
    if [[ $i -ge 0 && $i -lt 10 ]]
    then
            host=${NODE_NAME}0${i}
    else
            host=${NODE_NAME}$i
    fi
    # scp -r /root/exp2 $USER@$host:/root/
    # scp -r /root/MergeCDC/script/tc.sh  $USER@$host:/root/MergeCDC/script/
    scp /root/exp2/MergeCDC/TeraSort $USER@$host:/root/exp2/MergeCDC/
    scp /root/exp2/MergeCDC/CodedTeraSort $USER@$host:/root/exp2/MergeCDC/
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
        ssh $host "tc qdisc delete dev eth0 root;chmod +x /root/exp2/MergeCDC/script/tc.sh; /root/exp2/MergeCDC/script/tc.sh ${host_ip}"

} &
done
wait