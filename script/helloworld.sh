host_num=32
NODE_NAME=node
USER=root
host_address=192.168.0.


# for((i=2;i<=$host_num;i++));
# do
# {
#     if [[ $i -ge 0 && $i -lt 10 ]]
#     then
#             host=${NODE_NAME}0${i}
#     else
#             host=${NODE_NAME}$i
#     fi

#     scp /root/exp2/MergeCDC/script/helloworld $USER@$host:/root/exp2/MergeCDC/script/
# } &
# done
# wait
echo "mpirun start"
# mpirun -np 31 -allow-run-as-root --oversubscribe /root/exp2/MergeCDC/script/helloworld > /root/exp2/MergeCDC/result.out
mpirun -np 31 -allow-run-as-root -hostfile /root/exp2/MergeCDC/hostfile /root/exp2/MergeCDC/script/helloworld > /root/exp2/MergeCDC/result.out
echo "mpirun done"
echo "helloworld done" >> /root/exp2/MergeCDC/helloworld.log
