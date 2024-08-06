import json
import subprocess




root = "home/lmq/MergeCDC"


def UpdateBandwidth(ratio):
    # clear old bandwidth config
    filename = f"{root}/script/clear.sh"
    subprocess.run(filename, shell=True)
    
    # run ssh.sh
    filename = f"{root}/script/update_bandwidth_ratio_{ratio}.sh"




# read exp2.json, update Distribution, combination_my
# update Distributon and combination_my in worker nodes
# update bandwidth
# run "mpirun -np 31 --oversubscribe /home/lmq/MergeCDC/TeraSort"
# run "mpirun -np 32 --oversubscribe /home/lmq/MergeCDC/CodedTeraSort"
if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)

    print(config)