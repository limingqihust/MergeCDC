bash script/clear.sh
bash script/update.sh
mpirun -np 31 -allow-run-as-root -hostfile /root/wordcount-exp2/MergeCDC/hostfile /root/wordcount-exp2/MergeCDC/TeraSort
mpirun -np 32 -allow-run-as-root -hostfile /root/wordcount-exp2/MergeCDC/hostfile-r /root/wordcount-exp2/MergeCDC/CodedTeraSort