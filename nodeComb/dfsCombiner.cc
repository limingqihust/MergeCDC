#include "dfsCombiner.hh"

using namespace std;

DFSCombiner::DFSCombiner(NodeInfo *info, int orgN, int destN, int fn, int r) : nodeInfo(info), orgNodeNum(orgN), destNodeNum(destN), fileNum(fn), replicaNum(r) {
    assert(nodeInfo->fileDistribution.size() == orgN);
    where.resize(orgN);
    GenerateStdFileDist();
}

DFSCombiner::~DFSCombiner() {

}

void DFSCombiner::GetCombinationResult(vector<int> &nparts) {
    vector<vector<int>> combinedNodes(destNodeNum);
    vector<map<int, int>> combinedRacks(destNodeNum);
    CombineNodesRecursive(combinedNodes, combinedRacks, 0);
    nparts = where;
    // combinedNodes.resize(destNodeNum);
    // for(int i = 0; i < orgNodeNum; i++) {
    //     combinedNodes[nparts[i]].push_back(i);
    // }
    // debugFlag = true;
    // for(int i = 0; i < destNodeNum; i++) {
    //     for(int nodeIdx : combinedNodes[i]) {
    //         CodeGeneration(nodeIdx + 1, i + 1, combinedNodes);
    //     }
    // }

    return ;
}

void DFSCombiner::CombineNodesRecursive(vector<vector<int>> &combinedNodes, vector<map<int, int>> &combinedRacks, int curIdx) {
    if(curIdx == orgNodeNum) {
        double cut = CalculateCombinationQualityV2(combinedNodes);
        if(cut >= 0 && (minCut < 0 || cut < minCut)) {
            minCut = cut;
            for(int i = 0; i < combinedNodes.size(); i++) {
                for(int nid : combinedNodes[i]) {
                    where[nid] = i;
                }
            }
        }
        return ;
    }
    int rackId = nodeInfo->networkTopology[curIdx];
    for(int i = 0; i < destNodeNum; i++) {
        if(combinedNodes[i].size() / ubFactor > orgNodeNum / destNodeNum || (combinedRacks[i].size() >= maxRackNum && combinedRacks[i].find(rackId) == combinedRacks[i].end())) {
            continue;
        }
        combinedNodes[i].push_back(curIdx);
        combinedRacks[i][rackId]++;
        CombineNodesRecursive(combinedNodes, combinedRacks, curIdx + 1);
        combinedNodes[i].pop_back();
        combinedRacks[i][rackId]--;
        if(combinedRacks[i][rackId] == 0) {
            combinedRacks[i].erase(rackId);
        }
    }

    return ;
}

double DFSCombiner::CalculateCombinationQualityV2(vector<vector<int>> &combinedNodes) {
    // check empty node
    for(auto & cnode : combinedNodes) {
        if(cnode.size() == 0) {
            return -1.0;
        }
    }
    // check if unbalance
    int minvwgt = fileNum * replicaNum, maxvwgt = 0;
    for(int i = 0; i < combinedNodes.size(); i++) {
        int vwgt = 0;
        for(int nodeId : combinedNodes[i]) {
            vwgt += nodeInfo->fileDistribution[nodeId].size();
        }
        minvwgt = min(minvwgt, vwgt);
        maxvwgt = max(maxvwgt, vwgt);
    }
    if(maxvwgt / ubFactor > minvwgt) {
        return -1.0;
    }
    double totLoad = 0;
    for(int i = 0; i < destNodeNum; i++) {
        for(int nodeIdx : combinedNodes[i]) {
            totLoad += CodeGeneration(nodeIdx + 1, i + 1, combinedNodes);
        }
    }
    return totLoad;
}

double DFSCombiner::CodeGeneration(int rank, int logicalId, vector<vector<int>> &combinedNodes) {
  int innerRackSize = 0, crossRackSize = 0; // assume the size of normalized intermediate value = 1
  vector<set<int>> NodeSubsetR = GenerateNodeSubset(replicaNum);

  vector<set<int>> NodeSubsetS = GenerateNodeSubset(replicaNum + 1); // 多播组

  const vector<set<int> > fileNodeTab = GetInputDistribution(combinedNodes);

  map<set<int>, vector<int>> sameFileTab;
  map<set<int>, int> multiGroupsFileNum;
  map<int, vector<set<int>>> multicastGroupsTab;
  map<set<int>, unsigned int> SubsetSIdMap;
  map<int, vector<int>> file2ReducerNeeded;

  for(int i = 0; i < fileNodeTab.size(); i++) {
    sameFileTab[fileNodeTab[i]].push_back(i);
  }
  // search for coded multicast group
  unsigned int nsid = 0;
  int codedFileNum = 0;
  for(auto it : NodeSubsetS) {
    int minFileNum = INT_MAX;
    bool canBeCoded = true;
    for(int nodeId : it) {
      set<int> subsetR(it);
      subsetR.erase(nodeId);
      if(sameFileTab.find(subsetR) == sameFileTab.end()) {
        canBeCoded = false;
        break;
      }
      minFileNum = min(minFileNum, (int)sameFileTab[subsetR].size());
    }
    if(canBeCoded) {
      multiGroupsFileNum[it] = minFileNum;
      codedFileNum += minFileNum;
      SubsetSIdMap[it] = nsid++;
      for(int nodeId : it) {
        multicastGroupsTab[nodeId].push_back(it);
        set<int> subsetR(it);
        subsetR.erase(nodeId);
        for(int i = 0; i < minFileNum; i++) {
          int fileId = sameFileTab[subsetR][i];
          file2ReducerNeeded[fileId].push_back(nodeId);
        }
      }
    }
  }

  // init p2p file tab
  vector<vector<int>> combinedNodeStorage(destNodeNum);
  for(int i = 0; i < destNodeNum; i++) {
    for(int orgNodeIdx : combinedNodes[i]) {
      combinedNodeStorage[i].insert(combinedNodeStorage[i].end(), nodeInfo->fileDistribution[orgNodeIdx].begin(), nodeInfo->fileDistribution[orgNodeIdx].end());
    }
  }
  vector<set<int>> p2pFiles;
  const vector<int> reducerNodes = GetReducerNodes(combinedNodes);
  if(debugFlag) {
    cout << "reducerNodes: ";
    for(int rid : reducerNodes) {
        cout << rid << ' ';
    }
    cout << endl;
  }
  p2pFiles.resize(destNodeNum);
  const vector<int> &fileList = nodeInfo->fileDistribution[rank - 1];
  for(int i = 0; i < destNodeNum; i++) {
    int reducerId = reducerNodes[i];
    if(rank == reducerId) { 
      continue;
    }
    // except files already stored by dest node
    vector<int> destFileList;
    if(logicalId == i + 1) {
        const vector<int> destFiles = nodeInfo->fileDistribution[reducerId - 1];
        destFileList.insert(destFileList.end(), destFiles.begin(), destFiles.end());
    } else {
        const vector<int> destFiles = combinedNodeStorage[i];
        destFileList.insert(destFileList.end(), destFiles.begin(), destFiles.end());
    }
    set<int> destFileSet(destFileList.begin(), destFileList.end());
    for(int fileId : fileList) {
      if(destFileSet.find(fileId) != destFileSet.end()) {
        continue;
      }
      p2pFiles[i].insert(fileId);
    }
  }
  // update p2p file tab: remove files can be decoded from coded multicast
  const vector<NodeSet> &codedGroupsInc = multicastGroupsTab[logicalId];
  for(auto it : codedGroupsInc) {
    int codedFileNum = multiGroupsFileNum[it];
    for(int nodeId : it) {
      if(nodeId == logicalId) {
        continue;
      }
      set<int> subsetR(it);
      subsetR.erase(nodeId);
      for(int i = 0; i < codedFileNum; i++) {
        int removedFileId = sameFileTab[subsetR][i];
        for(int recNodeId : it) { 
          // for node in subsetR: already have this file, remove
          // for node = it - subsetR: can decode this file from multicast, remove
          if(recNodeId == logicalId) {
            continue;
          }
          p2pFiles[recNodeId - 1].erase(removedFileId);
        }
      }
    }
  }
  if(debugFlag) {
    cout << rank << ": p2p files: " << endl;
  }
  vector<map<int, vector<int>>> InnerNodeInputDist(destNodeNum);
  for(int i = 0; i < destNodeNum; i++) {
    for(int orgIdx : combinedNodes[i]) {
        for(int fileId : nodeInfo->fileDistribution[orgIdx]) {
            InnerNodeInputDist[i][fileId].push_back(orgIdx + 1);
        }
    }
  }
  int curRackId = nodeInfo->networkTopology[rank - 1];
  for(int i = 0; i < destNodeNum; i++) {
    int reducerId = reducerNodes[i];
    if(rank == reducerId) {
        continue;
    }
    int reducerRack = nodeInfo->networkTopology[reducerId - 1];
    double blockNum = 0;
    if(logicalId == i + 1) {
        for(int fileId : p2pFiles[i]) {
            blockNum += 1.0 / InnerNodeInputDist[logicalId - 1][fileId].size();
        }
    } else {
        blockNum = (0.0 + p2pFiles[i].size()) / replicaNum;
    }
    if(reducerRack == curRackId) {
        innerRackSize += blockNum;
    } else {
        crossRackSize += blockNum;
    }
    if(debugFlag) {
        cout << "to reducer: " << i << " ";
        for(int fileId : p2pFiles[i]) {
            cout << fileId << ' ';
        }
        cout << endl;
    }
  }

    // calculate intermediate values to be passed within the logical node
    set<int> innerP2pFiles;
    int reducerId = reducerNodes[logicalId - 1];
    if(rank == reducerId) {
      return innerRackSize + crossRackSize * InnerRackFactor;
    }
    const vector<int> &destFileList = nodeInfo->fileDistribution[reducerId - 1];
    set<int> destFileSet(destFileList.begin(), destFileList.end());
    set<int> distFileList;
    for(int fileId : fileList) {
      if(destFileSet.find(fileId) == destFileSet.end()) {
        distFileList.insert(fileId);
      }
    }
    // only add intermediate values to be used by cdc
    for(auto it : codedGroupsInc) {
      int codedFileNum = multiGroupsFileNum[it];
      for(int nodeId : it) {
        if(nodeId == logicalId) {
          continue;
        }
        set<int> subsetR(it);
        subsetR.erase(nodeId);
        for(int i = 0; i < codedFileNum; i++) {
          int fileId = sameFileTab[subsetR][i];
          if(distFileList.find(fileId) != distFileList.end()) {
            innerP2pFiles.insert(fileId);
          }
        }
      }
    }
    int logicalRackId = nodeInfo->networkTopology[reducerId - 1];
    int blockNum = 0;
    for(int fileId : innerP2pFiles) {
        blockNum += file2ReducerNeeded[fileId].size();
    }
    if(logicalRackId == curRackId) {
        innerRackSize += blockNum;
    } else {
        crossRackSize += blockNum;
    }
    if(debugFlag) {
        cout << "inner node trans: " ;
        for(int fileId : innerP2pFiles) {
            cout << fileId << ' ';
        }
        cout << endl;
    }

  return innerRackSize + crossRackSize * InnerRackFactor;
}

vector<set<int>> DFSCombiner::GenerateNodeSubset(int r)
{
  vector<NodeSet> list;
  vector<NodeSet> ret;
  list.push_back(NodeSet());
  for (int i = 1; i <= destNodeNum; i++)
  {
    unsigned int numList = list.size();
    for (unsigned int j = 0; j < numList; j++)
    {
      NodeSet n(list[j]);
      n.insert(i);
      if (int(n.size()) == r)
      {
        ret.push_back(n);
      }
      else
      {
        list.push_back(n);
      }
    }
  }
  return ret;
}

double DFSCombiner::CalculateCombinationQuality(vector<vector<int>> &combinedNodes) {
    // check empty node
    for(auto & cnode : combinedNodes) {
        if(cnode.size() == 0) {
            return -1.0;
        }
    }
    // check if unbalance
    int minvwgt = fileNum * replicaNum, maxvwgt = 0;
    for(int i = 0; i < combinedNodes.size(); i++) {
        int vwgt = 0;
        for(int nodeId : combinedNodes[i]) {
            vwgt += nodeInfo->fileDistribution[nodeId].size();
        }
        minvwgt = min(minvwgt, vwgt);
        maxvwgt = max(maxvwgt, vwgt);
    }
    if(maxvwgt / ubFactor > minvwgt) {
        return -1.0;
    }
    Graph *g = ConvertNodeInfo2Graph(combinedNodes);
    // calculate cut
    double cut = CalculateCutV2(g);
    delete g;
    return cut;
}

double DFSCombiner::CalculateCut(Graph *g) {
    double cut = 0;
    for(int i = 0; i < g->nvtxs; i++) {
        for(int j = g->xadj[i]; j < g->xadj[i + 1]; j++) {
            int k = g->adjncy[j];
            if(k < i || g->where[i] == g->where[j]) {
                continue;
            }
            cut += g->adjwgt[j];
        }
    }
    return cut;
}

double DFSCombiner::CalculateCutV2(Graph *g) {
    double cut = 0;
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    vector<int> &networkTopology = nodeInfo->networkTopology;
    vector<vector<int>> combinedFileDist(destNodeNum);
    vector<set<int>> combinedRackDist(destNodeNum);
    for(int i = 0; i < g->nvtxs; i++) {
        int me = g->where[i], rackId = networkTopology[i];
        combinedFileDist[me].insert(combinedFileDist[me].end(), blockInfo[i].begin(), blockInfo[i].end());
        combinedRackDist[me].insert(rackId);
    }
    // check if rackNum > maxRackNum
    for(int i = 0; i < destNodeNum; i++) {
        if(combinedRackDist[i].size() > maxRackNum) {
            return -2.0;
        }
    }
    // calculate cut
    for(int i = 0; i < destNodeNum; i++) {
        for(int j = i + 1; j < destNodeNum; j++) {
            // wgt = similarity * average_vwgt / (vwgt[i] + vwgt[j])
            double wgt = GetSimilarity(combinedFileDist[i], combinedFileDist[j]) * 100 * 2 * g->tvwgt / destNodeNum / (combinedFileDist[i].size() + combinedFileDist[j].size());
            // considering network topology, wgt = original_wgt / ((1 - netTpEIFactor) * 2 * #(set_union(topKRacks[i], topKRacks[j])) / (#(topKRacks[i]) + #(topKRacks[j])))
            set<int> unionSet;
            set_union(combinedRackDist[i].begin(), combinedRackDist[i].end(),
                      combinedRackDist[j].begin(), combinedRackDist[j].end(),
                      inserter(unionSet, unionSet.begin()));
            wgt /= crossRackFactor * 2 * pow(unionSet.size(), 2.0) / pow(combinedRackDist[i].size() + combinedRackDist[j].size(), 2);
            cut += wgt;
        }
    }
    return cut;
}

DFSCombiner::Graph *DFSCombiner::ConvertNodeInfo2Graph(vector<vector<int>> &combinedNodes) {
    vector<vector<int>> &blockInfo = nodeInfo->fileDistribution;
    vector<int> &networkTopology = nodeInfo->networkTopology;
    Graph *graph = new Graph(orgNodeNum);
    for(int i = 0; i < graph->nvtxs; i++) {
        graph->vwgt.push_back(blockInfo[i].size());
        graph->tvwgt += blockInfo[i].size();
    }
    for(int i = 0; i < graph->nvtxs; i++) {
        graph->xadj.push_back(graph->adjncy.size());
        for(int j = 0; j < graph->nvtxs; j++) {
            if(j == i) {
                continue;
            }
            // wgt = similarity * average_vwgt / (vwgt[i] + vwgt[j])
            double wgt = GetSimilarity(blockInfo[i], blockInfo[j]) * 100 * 2 * graph->tvwgt / graph->nvtxs / (blockInfo[i].size() + blockInfo[j].size()); // 保留两位小数
            // considering network topology, if two nodes from different rack, wgt *= crossRackFactor
            if(networkTopology[i] != networkTopology[j]) {
                wgt *= crossRackFactor;
            }
            if(wgt != 0) { 
                graph->adjncy.push_back(j);
                graph->adjwgt.push_back(wgt);
            }
        }
    }
    graph->xadj.push_back(graph->adjncy.size());

    // parse combination result
    for(int i = 0; i < combinedNodes.size(); i++) {
        for(int orgNodeIdx : combinedNodes[i]) {
            graph->where[orgNodeIdx] = i;
        }
    }

    return graph;
}

void DFSCombiner::GenerateStdFileDist() {
    stdFileDist.resize(destNodeNum);
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
    assert(fileNum % nodeSubsets.size() == 0);
    int eta = fileNum / nodeSubsets.size();
    int fileId = 0;
    for(int i = 0; i < nodeSubsets.size(); i++) {
        for(int j = 0; j < eta; j++) {
            for(int nid : nodeSubsets[i]) {
                stdFileDist[nid].push_back(fileId);
            }
            fileId++;
        }
    }
    return ;
}

int DFSCombiner::Dot(string &a, string &b) {
    int res = 0;
    assert(a.size() == b.size());
    for(int i = 0; i < a.size(); i++) {
        res += (a[i] - '0') * (b[i] - '0');
    }
    return res;
}

double DFSCombiner::GetVectorLength(string &s) {
    int res = 0;
    for(int i = 0; i < s.size(); i++) {
        res += pow((s[i] - '0'), 2);
    }
    return sqrt(1.0 * res);
}

double DFSCombiner::GetSimilarity(const vector<int> &nodeX, const vector<int> &nodeY) {
    double maxSimilarity = 0;
    string a = string(fileNum, '0');
    string b = string(fileNum, '0');
    for(int fid : nodeX) {
        a[fid] = '1';
    }
    for(int fid : nodeY) {
        a[fid] = '1';
    }
    for(int i = 0; i < destNodeNum; i++) {
        for(int fid : stdFileDist[i]) {
            b[fid] = '1';
        }
        maxSimilarity = max(maxSimilarity, (Dot(a, b) * 1.0) / (GetVectorLength(a) * GetVectorLength(b)));
    }
    return maxSimilarity;
}
