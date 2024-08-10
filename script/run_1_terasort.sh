echo "master mpirun start" >> /root/exp3/MergeCDC/log.out
echo 0 > /root/exp3/MergeCDC/result_flag.out
rm -rf /root/exp3/MergeCDC/result.out 
mpirun -np 21  -allow-run-as-root -hostfile /root/exp3/MergeCDC/hostfile /root/exp3/MergeCDC/TeraSort
echo "master mpirun done" >> /root/exp3/MergeCDC/log.out
echo 1 > /root/exp3/MergeCDC/result_flag.out