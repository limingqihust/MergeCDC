host_num=32
NODE_NAME=node
USER=root
host_address=192.168.0.


for((i=1; i<=1; i++));
do
{
    mpirun -np 32 -allow-run-as-root -hostfile /root/exp2/MergeCDC/hostfile-r /root/exp2/MergeCDC/CodedTeraSort
} &
done
wait