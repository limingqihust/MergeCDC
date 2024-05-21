#! /bin/bash
 
host_num=41
host_address=192.168.0.          
host_hostname=node     
host_username=root                 
self_ip="$1"

tc qdisc add dev eth0 root handle 1:0 htb default 2

for ((i=0;i<$host_num;i++));do
    host_ip=${host_address}$((i + 1))
	if (($i==0 || $i==40))
    then
        rack_id=1
    elif (($i >= 1 && $i <= 7))
    then
        rack_id=2
    elif(($i >= 8 && $i <= 14))
    then
        rack_id=3
    elif(($i >= 15 && $i <= 21))
    then
        rack_id=4
    elif(($i >= 22 && $i <= 28))
    then
        rack_id=5
    elif(($i >= 29 && $i <= 39))
    then
        rack_id=6
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
if ((last_octet == 1 || last_octet == 41 )) 
then
    self_rack=1
elif ((last_octet >= 2 && last_octet <= 8)) 
then
    self_rack=2
elif ((last_octet >= 9 && last_octet <= 15)) 
then
    self_rack=3
elif ((last_octet >= 16 && last_octet <= 22)) 
then
    self_rack=4
elif ((last_octet >= 23 && last_octet <= 29)) 
then
    self_rack=5
elif ((last_octet >= 30 && last_octet <= 40)) 
then
    self_rack=6
else
    echo "Invalid IPv4 address"
    exit 1
fi

for((i=1;i<=3;i++)); do
    if ((i == self_rack))
    then
        tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 4Gbit ceil 4Gbit burst 0
        echo "${self_ip}: tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 4Gbit"
        tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 10us
        echo "${self_ip}: tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 10us"
    else
        tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 200Mbit ceil 200Mbit burst 0
        echo "${self_ip}: tc class add dev eth0 parent 1:0 classid 1:${i} htb rate 200Mbit"
        tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 200us
        echo "${self_ip}: tc qdisc add dev eth0 parent 1:${i} handle $((i+1)):0 netem delay 200us"
    fi
done
wait
