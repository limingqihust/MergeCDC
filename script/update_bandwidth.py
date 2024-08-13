import json
import subprocess

filename = "/root/exp2/MergeCDC/config/exp2.json"
with open(filename) as file:
    conf = json.load(file)
    infiniband = conf["infiniband"]
    bandwidth_ratio = conf["bandwidth_ratio"]

    if infiniband:
        # clear old config
        exec_cmd = "bash /root/exp2/MergeCDC/script/clear.sh"
        ret = subprocess.getstatusoutput(exec_cmd)
        # update bandwidth
        exec_cmd = "bash /root/exp2/MergeCDC/script/update_infiniband.sh"
        ret = subprocess.getstatusoutput(exec_cmd)
        # set bandwidth ratio
        exec_cmd = f"bash /root/exp2/MergeCDC/script/update_infiniband_bandwidth_ratio_{bandwidth_ratio}.sh"
        ret = subprocess.getstatusoutput(exec_cmd)
        print(ret)

    else:
        exec_cmd = f"bash /root/exp2/MergeCDC/script/update_bandwidth_ratio_{bandwidth_ratio}.sh"
        ret = subprocess.getstatusoutput(exec_cmd)
        print(ret)

