import json
import subprocess

filename = "/root/exp2/MergeCDC/config/exp2.json"
with open(filename) as file:
    conf = json.load(file)
    bandwidth_ratio = conf["bandwidth_ratio"]
    exec_cmd = f"bash /root/exp2/MergeCDC/script/update_bandwidth_ratio_{bandwidth_ratio}.sh"
    ret = subprocess.getstatusoutput(exec_cmd)
    print(ret)

