#include "fileDIstribution.hh"

using namespace std;

FileDistGenerator::FileDistGenerator(int fn, int onn, int dnn, int r, int rn) : fileNum(fn), originalNodeNum(onn), destNodeNum(dnn), replicaNum(r), rackNum(rn) {
    stdDist.resize(destNodeNum);
    GenerateStandardDistribution();
}

FileDistGenerator::~FileDistGenerator() {

}

void FileDistGenerator::GenerateStandardDistribution() {
    // generate subset of nodes ( K choose r )
    vector<set<int> > list;
    vector<set<int> > nodeSubsets;
    list.push_back(set<int>());
    for(int i = 0; i < destNodeNum; i++) {
        int listNum = list.size();
        for(int j = 0; j < listNum; j++) {
            set<int> tmpList(list[j]);
            tmpList.insert(i);
            if(tmpList.size() == replicaNum) {
                nodeSubsets.push_back(tmpList);
            } else {
                list.push_back(tmpList);
            }
        }
    }
    // cout << "node subsets: ";
    // for(int i = 0; i < nodeSubsets.size(); i++) {
    //     cout << endl;
    //     for(int nid : nodeSubsets[i]) {
    //         cout << nid << " ";
    //     }
    // }
    // cout << endl;
    // assign the corresponding files to each subset of nodes
    if(fileNum % nodeSubsets.size()) {
        cout << "N is not divisible by [K choose R], where N = " << fileNum << ", [K choose R] = " << nodeSubsets.size() << endl;
        assert(false);
    }
    int eta = fileNum / nodeSubsets.size();
    int fileId = 0;
    for(int i = 0; i < nodeSubsets.size(); i++) {
        for(int j = 0; j < eta; j++) {
            for(int nid : nodeSubsets[i]) {
                stdDist[nid].push_back(fileId);
            }
            fileId++;
        }
    }
    // cout << "standard file distribution: ";
    // for(int i = 0; i < destNodeNum; i++) {
    //     cout << "\nnode " << i << "(" << stdDist[i].size() << "):";
    //     for(int fid : stdDist[i]) {
    //         cout << fid << " ";
    //     }
    // }
    // cout << endl;
    return ;
}

vector<vector<int> > FileDistGenerator::GetRandomDistribution() {
    vector<vector<int> > dist(originalNodeNum);
    for(int i = 0; i < replicaNum; i++) {
        for(int fileId = 0; fileId < fileNum; fileId++) {
            int nodeId = rand() % originalNodeNum;
            dist[nodeId].push_back(fileId);
        }
    }
    return dist;
}

vector<vector<int> > FileDistGenerator::GetRandomDistributionFromStandard() {
    vector<vector<int> > dist;
    for(int i = 0; i < destNodeNum; i++) {
        int nodeNum = originalNodeNum / destNodeNum;
        if(i < originalNodeNum % destNodeNum) {
            nodeNum++;
        }
        // for(int j = 1; j < nodeNum; j++) {
        //     dist.emplace_back(stdDist[i].begin() + (j - 1) * (stdDist[i].size() / nodeNum), stdDist[i].begin() + j * (stdDist[i].size() / nodeNum));
        // }
        // dist.emplace_back(stdDist[i].begin() + (nodeNum - 1) * (stdDist[i].size() / nodeNum), stdDist[i].end());
        int distIdx = dist.size();
        for(int j = 0; j < nodeNum; j++) {
            dist.push_back(vector<int>());
        }
        for(int j = 0; j < stdDist[i].size(); j++) {
            int nodeId = rand() % nodeNum;
            dist[distIdx + nodeId].push_back(stdDist[i][j]);
        }
    }
    return dist;
}

double FileDistGenerator::GetSimilarity(vector<int> nodeX, vector<int> nodeY) {
    double maxSimilarity = 0;
    string a = string(fileNum, '0');
    string b = string(fileNum, '0');
    for(int fid : nodeX) {
        a[fid] = '1';
    }
    for(int fid : nodeY) {
        // if(a[fid] == '1') {
        //     // same file
        //     return 0;
        // }
        a[fid] = '1';
    }
    for(int i = 0; i < destNodeNum; i++) {
        for(int fid : stdDist[i]) {
            b[fid] = '1';
        }
        maxSimilarity = max(maxSimilarity, (Dot(a, b) * 1.0) / (GetVectorLength(a) * GetVectorLength(b)));
    }
    return maxSimilarity;
}

int FileDistGenerator::Dot(string &a, string &b) {
    int res = 0;
    assert(a.size() == b.size());
    for(int i = 0; i < a.size(); i++) {
        res += (a[i] - '0') * (b[i] - '0');
    }
    return res;
}

double FileDistGenerator::GetVectorLength(string &s) {
    int res = 0;
    for(int i = 0; i < s.size(); i++) {
        res += pow((s[i] - '0'), 2);
    }
    return sqrt(1.0 * res);
}

vector<int> FileDistGenerator::GenerateRandomNetworkTopology() {
    // vector<int> netTp(originalNodeNum);
    // for(int i = 0; i < originalNodeNum; i++) {
    //     int rackId = rand() % rackNum;
    //     netTp[i] = rackId;
    // }
    vector<int> netTp;
    int nodeNum = originalNodeNum / rackNum, lastNodeNum = originalNodeNum - nodeNum * (rackNum - 1);
    for(int i = 0; i < rackNum - 1; i++) {
        for(int j = 0; j < nodeNum; j++) {
            netTp.push_back(i);
        }
    }
    for(int i = 0; i < lastNodeNum; i++) {
        netTp.push_back(rackNum - 1);
    }
    return netTp;
}

pair<vector<vector<int>>, vector<int>> FileDistGenerator::GetDistributionFromFile(string filePath) {
    vector<vector<int>> originNodeStorage(originalNodeNum);
    vector<int> networkTopology;
    ifstream ifs(filePath);
    if(!ifs.is_open()) {
      cout << "can't open distribution file" << filePath << endl;
      assert(false);
    }
    // read distribution from distributionFile
    for(unsigned int i = 0 ; i < originalNodeNum; i++) {
      string line;
      if(getline(ifs, line)) {
        istringstream iss(line);
        int fileId;
        while(iss >> fileId) {
          originNodeStorage[i].push_back(fileId);
        }
      } else {
        cout << "not enough node in distribution file, only get " << i << "nodes" << endl;
        assert(false);
      }
    }
    // read network topology
    string line;
    if(getline(ifs, line)) {
      istringstream iss(line);
      int rackId;
      while(iss >> rackId) {
        networkTopology.push_back(rackId);
      }
    } else {
      cout << "can't find the network topology info" << endl;
      assert(false);
    }
    assert(networkTopology.size() == originalNodeNum);
    ifs.close();
    return make_pair(originNodeStorage, networkTopology);
}
