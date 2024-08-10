echo "master mpirun start" >> /root/exp2/MergeCDC/log.out
echo 0 > /root/exp2/MergeCDC/result_flag.out
rm -rf /root/exp2/MergeCDC/result.out 
for ((i = 1; i <= 8; i++));
do
{
    mpirun -np 31 -allow-run-as-root -hostfile /root/exp2/MergeCDC/hostfile /root/exp2/MergeCDC/TeraSort >> /root/exp2/MergeCDC/result.out 
} &
done
wait
echo "master mpirun done" >> /root/exp2/MergeCDC/log.out
echo 1 > /root/exp2/MergeCDC/result_flag.out