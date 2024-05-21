NODE_NUM=41
NODE_NAME=node
USER=root
HOME=/home
CDC_DIR=$HOME/Coded-TeraSort-Node

# install base dependencies on each node
for((i=0;i<$NODE_NUM;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}-0${i}
	else
		host=${NODE_NAME}-$i
	fi

    ssh $USER@$host "sudo apt-get update;sudo apt install -y gcc automake autoconf libtool make cmake g++ build-essential wget vim"

	ssh $USER@$host "sudo ufw disable"
} &
done 

wait

# copy DPDedup source code to each node
for((i=1;i<$NODE_NUM;i++))
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}-0${i}
	else
		host=${NODE_NAME}-$i
	fi

	scp -r $CDC_DIR $USER@$host:$HOME
} &
done

wait

# install openmpi on each node
for((i=0;i<$NODE_NUM;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}-0${i}
	else
		host=${NODE_NAME}-$i
	fi

	ssh $USER@$host "cd $CDC_DIR/install; tar -zxvf openmpi-4.0.0.tar.gz"
	ssh $USER@$host "cd $CDC_DIR/install/openmpi-4.0.0; ./configure --prefix=/opt/openmpi --enable-mpi-cxx; make && sudo make install"
	ssh $USER@$host "echo "export PATH=/opt/openmpi/bin:\$PATH" >> ~/.bashrc;echo "export LD_LIBRARY_PATH=/opt/openmpi/lib:\$LD_LIBRARY_PATH" >> ~/.bashrc;source ~/.bashrc"
} &
done
wait
