#include "combination.hh"
#include "combiner.hh"

using namespace std;

bool compareByWgt(const std::pair<int, int>& a, const std::pair<int, int>& b) {
    return a.first > b.first;
}

void Graph::Convert2Graph() {
    // get file distribution
    // blockInfo = fileDist->GetRandomDistributionFromStandard();
    blockInfo = fileDist->GetRandomDistribution();
    networkTopology = fileDist->GenerateRandomNetworkTopology();
    // auto it = fileDist->GetDistributionFromFile("./Distribution");
    // blockInfo = it.first;
    // networkTopology = it.second;
    // convert block distribution to undirected graph
    int nVertices = blockInfo.size();
    // dump nodeInfo to file
    ofstream ofs("Distribution");
    if(!ofs) {
        cerr << "fail to open file nodeInfo" << endl;
        assert(false);
    }
    for(int i = 0; i < blockInfo.size(); i++) {
        for(int fid : blockInfo[i]) {
            ofs << fid << " ";
        }
        ofs << endl;
    }
    for(int i = 0; i < nVertices; i++) {
        ofs << networkTopology[i] << " ";
    }
    ofs << endl;
    ofs.close();
    cout << "generate random distribution: ";
    for(int i = 0; i < blockInfo.size(); i++) {
        cout << "\nnode " << i << "(" << blockInfo[i].size() << "):";
        for(int fid : blockInfo[i]) {
            cout << fid << " ";
        }
    }
    cout << endl << "generate network topology: " << endl;
    for(int rackId : networkTopology) {
        cout << rackId << " ";
    }
    for(int i = 0; i < nVertices; i++) {
        vwgt.push_back(blockInfo[i].size());
        xadj.push_back(adjncy.size());
        // vector<pair<int, int>> wgts; // (wgt, nodeId)
        for(int j = 0; j < nVertices; j++) {
            if(j == i) {
                continue;
            }
            // wgts.push_back(make_pair(fileDist->GetSimilarity(blockInfo[i], blockInfo[j]) * 100, j));
            int wgt = fileDist->GetSimilarity(blockInfo[i], blockInfo[j]) * 100;
            if(wgt != 0) {
                adjncy.push_back(j);
                adjwgt.push_back(wgt);
            }
        }
        // sort(wgts.begin(), wgts.end(), compareByWgt);
        // for(int j = 0; j < 6; j++) {
        //     adjncy.push_back(wgts[j].second);
        //     adjwgt.push_back(wgts[j].first);
        // }
    }
    xadj.push_back(adjncy.size());
    // cout << "\nxadj:" << endl;
    // for(int i : xadj) {
    //     cout << i << ", ";
    // }
    // cout << "\nvwgt: " << endl;
    // for(int i : vwgt) {
    //     cout << i << ", ";
    // }
    // cout << "\nadjncy: " << endl;
    // for(int i : adjncy) {
    //     cout << i << ", ";
    // }
    // cout << "\nadjwgt: " << endl;
    // for(int i : adjwgt) {
    //     cout << i << ", ";
    // }
    // cout << endl;
}

void Combination::GenRandomDist() {
    combinedDist.clear();
    combinedDist.resize(nParts);
    for(int i = 0; i < graph->blockInfo.size(); i++) {
        int nodeId = rand() % nParts;
        combinedDist[nodeId].insert(combinedDist[nodeId].end(), graph->blockInfo[i].begin(), graph->blockInfo[i].end());
    }
    // for(int i = 0; i < graph->blockInfo.size(); i++) {
    //     for(int fileId : graph->blockInfo[i]) {
    //         int nodeId = rand() % nParts;
    //         combinedDist[nodeId].push_back(fileId);
    //     }
    // }
}

void Combination::GenMyCombinationDist() {
    struct timeval start, end;
    double rTime;
    combinedDist.clear();
    combinedDist.resize(nParts);
    NodeInfo *info = new NodeInfo;
    info->fileDistribution = graph->blockInfo;
    info->networkTopology = graph->networkTopology;
    int fileNum = graph->fileDist->GetFileNum(), nodeNum = graph->xadj.size() - 1;
    Combiner *c = new Combiner(info, nodeNum, nParts, fileNum, replicaNum);
    vector<int> res(nodeNum);
    gettimeofday(&start,NULL);
    c->GetCombinationResult(res);
    gettimeofday(&end,NULL);
    rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
    vector<vector<int>> combRes(nParts);
    for(int i = 0; i < res.size(); i++) {
        combRes[res[i]].push_back(i);
        combinedDist[res[i]].insert(combinedDist[res[i]].end(), graph->blockInfo[i].begin(), graph->blockInfo[i].end());
    }
    ofstream rofs("result", std::ios_base::app);
    // ofstream rofs("result");
    rofs << "heuristic:\n network topology" << endl;
    for(int i = 0; i < nParts; i++) {
        for(int nodeId : combRes[i]) {
            rofs << nodeId << '(' << graph->networkTopology[nodeId] << ") ";
        }
        rofs << endl;
    }
    rofs << "cost " << rTime << "s to get result" << endl;
    rofs.close();
    // dump combination result to file
    ofstream ofs("combination_my");
    if(!ofs) {
        cerr << "fail to open file combination_my" << endl;
        assert(false);
    }
    for(int i = 0; i < nodeNum; i++) {
        ofs << res[i] << " ";
    }
    ofs << endl;
    ofs.close();
    // cout combined file dist
    // cout << "-----node combine res in files" << endl;
    // for(int i = 0; i < nParts; i++) {
    //     for(int nodeId : combRes[i]) {
    //         for(int fileId : graph->blockInfo[nodeId]) {
    //             cout << fileId << ' ';
    //         }
    //         cout << endl;
    //     }
    // }
    // cout << "combine res:" << endl;
    // for(int i = 0; i < nParts; i++) {
    //     cout << combRes[i].size() << ' ';
    // }
    // cout << endl;
    // cout << "network topology: " << endl;
    // for(int i = 0; i < nParts; i++) {
    //     for(int nodeId : combRes[i]) {
    //         cout << info->networkTopology[nodeId] << ' ';
    //     }
    // }
    // cout << endl;
    return ;
}

void Combination::GenDFSCombinationDist() {
    struct timeval start, end;
    double rTime;
    combinedDist.clear();
    combinedDist.resize(nParts);
    NodeInfo *info = new NodeInfo;
    info->fileDistribution = graph->blockInfo;
    info->networkTopology = graph->networkTopology;
    int fileNum = graph->fileDist->GetFileNum(), nodeNum = graph->xadj.size() - 1;
    DFSCombiner *c = new DFSCombiner(info, nodeNum, nParts, fileNum, replicaNum);
    vector<int> res(nodeNum);
    gettimeofday(&start,NULL);
    c->GetCombinationResult(res);
    gettimeofday(&end,NULL);
    rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
    vector<vector<int>> combRes(nParts);
    for(int i = 0; i < res.size(); i++) {
        combRes[res[i]].push_back(i);
        combinedDist[res[i]].insert(combinedDist[res[i]].end(), graph->blockInfo[i].begin(), graph->blockInfo[i].end());
    }
    ofstream rofs("result");
    rofs << "brute force:\n network topology" << endl;
    for(int i = 0; i < nParts; i++) {
        for(int nodeId : combRes[i]) {
            rofs << nodeId << '(' << graph->networkTopology[nodeId] << ") ";
        }
        rofs << endl;
    }
    rofs << "cost " << rTime << "s to get result" << endl;
    rofs.close();
    // dump combination result to file
    ofstream ofs("combination_dfs");
    if(!ofs) {
        cerr << "fail to open file combination_dfs" << endl;
        assert(false);
    }
    for(int i = 0; i < nodeNum; i++) {
        ofs << res[i] << " ";
    }
    ofs << endl;
    ofs.close();
}

void Combination::SearchForGroups() {
    map<set<int>, vector<int>> sameFileTab; // nodeset(size r) -> file
    map<int, set<int>> fileNodeTab; // file -> nodes
    // cout << "\nfile distribution after node combination" << endl;
    for(int i = 0; i < combinedDist.size(); i++) {
        for(int fileId : combinedDist[i]) {
            fileNodeTab[fileId].insert(i);
            // cout << fileId << " ";
        }
        // cout << endl;
    }
    for(auto it : fileNodeTab) {
        sameFileTab[it.second].push_back(it.first);
    }
    // generate broadcast group
    vector<set<int>> list;
    vector<set<int>> bcastGroup; // broadcast group(size r + 1)
    list.push_back(set<int>());
    for(int i = 0; i < nParts; i++) {
        int listNum = list.size();
        for(int j = 0; j < listNum; j++) {
            set<int> tmpNodeSet(list[j]);
            tmpNodeSet.insert(i);
            if(tmpNodeSet.size() == replicaNum + 1) {
                bcastGroup.push_back(tmpNodeSet);
            } else {
                list.push_back(tmpNodeSet);
            }
        }
    }
    // search for broadcast group
    map<int, int> missNum; // miss subset count
    int totFileNum = 0, totFileGroupNum = 0, totInnerNodeTx = 0;
    for(auto it : bcastGroup) {
        int missCnt = 0, fileNum = 0, fileGroupNum = INT_MAX;
        for(int nodeId : it) {
            set<int> subsetR(it);
            subsetR.erase(nodeId);
            if(sameFileTab.find(subsetR) == sameFileTab.end()) {
                missCnt++;
            } else {
                fileNum += sameFileTab[subsetR].size();
                fileGroupNum = min(fileGroupNum, int(sameFileTab[subsetR].size()));
            }
        }
        missNum[missCnt]++;
        if(!missCnt) {
            totFileNum += fileNum;
            totFileGroupNum += fileGroupNum;
        } else {
            continue;
        }
        // for(int i = 0; i < fileGroupNum; i++) {
        //     map<int, vector<int>> codeFileList; // nodeId -> fileId needed for code
        //     for(int nodeId : it) {
        //         set<int> subsetR(it);
        //         subsetR.erase(nodeId);
        //         for(int codeNodeId : subsetR) {
        //             codeFileList[codeNodeId].push_back(sameFileTab[subsetR][i]);
        //         }
        //     }
        //     for(auto node : codeFileList) {
        //         int nodeId = node.first;
        //         map<int, int> realNodeFileCnt; // real node id -> fileCnt
        //         for(int fileId : node.second) {
        //             vector<int> nodeList = fileRealNodeTab[nodeId][fileId];
        //             set<int> nodeSets(nodeList.begin(), nodeList.end());
        //             for(int realNodeId : nodeSets) {
        //                 realNodeFileCnt[realNodeId]++;
        //             }
        //         }
        //         int maxFileCnt = 0;
        //         for(auto realNode : realNodeFileCnt) {
        //             maxFileCnt = max(maxFileCnt, realNode.second);
        //         }
        //         totInnerNodeTx += codeFileList[nodeId].size() - maxFileCnt;
        //     }
        // }
    }
    ofstream rofs("result", std::ios_base::app);
    rofs << missNum[0] << " out of " << bcastGroup.size() << " bcast groups can be coded" << endl;
    int eta = fileNodeTab.size() / CalCombination(nParts, replicaNum);
    int groupNum = eta * bcastGroup.size();
    rofs << totFileGroupNum << " out of " << groupNum << " file groups can be coded" << endl;
    rofs << totFileNum << " out of " << groupNum * (replicaNum + 1) << " files can be coded" << endl;
    rofs.close();
    // cout << "need extra " << totInnerNodeTx << " intermediate value" << endl;
    return ;
}

void Combination::ShowResult() {
    vector<vector<int> > combinedNodes(nParts);
    for(int i = 0; i < parts.size(); i++) {
        combinedNodes[parts[i]].push_back(i);
    }
    cout << "\nnew nodes: ";
    for(int i = 0; i < nParts; i++) {
        cout << "\nnode " << i << ":";
        for(int nid : combinedNodes[i]) {
            cout << nid << " ";
        }
    }
    cout << endl;

    cout << "\nafter combination: ";
    for(int i = 0; i < nParts; i++) {
        cout << "\nnode " << i << "(" << combinedDist[i].size() << "):";
        for(int fid : combinedDist[i]) {
            cout << fid << " ";
        }
    }
    cout << endl;
    return;
}

int Combination::CalCombination(int m, int n) { // calculate m choose n
    long long res = 1;
    for(int i = 0; i < n; i++) {
        res *= m - i;
    }
    for(int i = 1; i <= n; i++) {
        res /= i;
    }
    return int(res);
}
