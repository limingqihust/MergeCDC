echo "master mpirun start" >> /root/MergeCDC/log.out
echo 0 > /root/MergeCDC/result_flag.out
rm -rf /root/MergeCDC/result.out 
mpirun -np 21  -allow-run-as-root -hostfile /root/MergeCDC/hostfile /root/MergeCDC/TeraSort
echo "master mpirun done" >> /root/MergeCDC/log.out
echo 1 > /root/MergeCDC/result_flag.out