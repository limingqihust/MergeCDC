echo "master mpirun start" >> /root/exp2/MergeCDC/log.out
echo 0 > /root/exp2/MergeCDC/result_flag.out
rm -rf /root/exp2/MergeCDC/result.out 
for ((i = 1; i <= 1; i++));
do
{
    mpirun -np 32 -allow-run-as-root -hostfile /root/exp2/MergeCDC/hostfile-r /root/exp2/MergeCDC/CodedTeraSort
    # mpirun -np 32 -allow-run-as-root -hostfile /root/exp2/MergeCDC/hostfile-r /root/exp2/MergeCDC/CodedTeraSort >> /root/exp2/MergeCDC/result.out 
} &
done
wait
echo "master mpirun done" >> /root/exp2/MergeCDC/log.out
echo 1 > /root/exp2/MergeCDC/result_flag.out