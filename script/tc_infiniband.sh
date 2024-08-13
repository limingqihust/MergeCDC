#! /bin/bash
 
host_num=32
host_address=192.168.0.          
host_hostname=node     
host_username=root                 
self_ip="$1"

tc qdisc add dev eth0 root handle 1:0 htb default 2

for ((i=0;i<$host_num;i++));do
    host_ip=${host_address}$((i + 1))
	if (($i==0 || $i==31))
    then
        rack_id=1
    elif (($i >= 1 && $i <= 10))
    then
        rack_id=2
    elif(($i >= 11 && $i <= 20))
    then
        rack_id=3
    elif(($i >= 21 && $i <= 30))
    then
        rack_id=4
    else
        echo "Invalid IPv4 address: $1"
        exit 1
    fi
    tc filter add dev eth0 parent 1:0 prior 2 protocol ip u32 match ip dst ${host_ip}/32 classid 1:${rack_id}
    echo "tc filter add dev eth0 parent 1:0 prior 2 protocol ip u32 match ip dst ${host_ip}/32 classid 1:${rack_id}"
done
wait

last_octet=$(echo "$1" | awk -F'.' '{print $4}')

# get the rackId of self_ip
if ((last_octet == 1 || last_octet == 32 )) 
then
    self_rack=1
elif ((last_octet >= 2 && last_octet <= 11)) 
then
    self_rack=2
elif ((last_octet >= 12 && last_octet <= 21)) 
then
    self_rack=3
elif ((last_octet >= 22 && last_octet <= 31)) 
then
    self_rack=4
else
    echo "Invalid IPv4 address"
    exit 1
fi

for((i=1;i<=4;i++)); do
    if ((i == self_rack))
    then
        tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 10Gbit ceil 10Gbit burst 0
        echo "${self_ip}: tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 10Gbit"
        tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 4us
        echo "${self_ip}: tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 4us"
    else
        tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 500Mbit ceil 500Mbit burst 0
        echo "${self_ip}: tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 500Mbit"
        tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 80us
        echo "${self_ip}: tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 80us"
    fi
done
wait
