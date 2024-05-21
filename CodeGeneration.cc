#include <iostream>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>

#include "CodeGeneration.h"

using namespace std;

CodeGeneration::CodeGeneration(int _N, int _K, int _R, int rank, CodedConfiguration *c) : N(_N), K(_K), R(_R), conf(c)
{
  NodeSubsetR = generateNodeSubset(R);

  NodeSubsetS = generateNodeSubset(R + 1); // 多播组

  const vector<set<int> > &fileNodeTab = conf->getInputDistribution();

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
  if(!conf->EnableNodeCombination()) {
    int eta = N / CalCombination(K, R);
    int groupNum = eta * NodeSubsetS.size();
    // cout << codedFileNum << " out of " << groupNum << " file groups can be coded multicasted" << endl;
  }

  // init p2p file tab
  if(rank == conf->getNumMapper() + 1) {
    return ;
  }
  const vector<int> reducerNodes = conf->getReducerNodes();
  p2pFiles.resize(conf->getNumReducer());
  const vector<int> &fileList = conf->getNodeStorage(rank);
  int logicalId = conf->getLogicalNodeId(rank);
  for(int i = 0; i < conf->getNumReducer(); i++) {
    int reducerId = reducerNodes[i];
    if(rank == reducerId) { 
      continue;
    }
    // except files already stored by dest node
    vector<int> destFileList;
    if(conf->EnableNodeCombination()) {
      if(logicalId == i + 1) {
        const vector<int> destFiles = conf->getNodeStorage(reducerId);
        destFileList.insert(destFileList.end(), destFiles.begin(), destFiles.end());
      } else {
        const vector<int> destFiles = conf->getCombinedNodeStorage(i + 1);
        destFileList.insert(destFileList.end(), destFiles.begin(), destFiles.end());
      }
    } else {
      const vector<int> destFiles = conf->getNodeStorage(reducerId);
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
  const vector<NodeSet> &codedGroupsInc = getNodeSubsetSContain(logicalId);
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

  if(!conf->EnableNodeCombination()) {
    return ;
  }

  if(conf->EnableInnerCode()) {
    // calculate intermediate values to be passed within the logical node
    const vector<int> &networkTopology = conf->getNetworkTopology();
    int reducerId = conf->getReducerId(logicalId), reducerRackId = networkTopology[reducerId - 1], curRackId = networkTopology[rank - 1];
    const vector<int> &nodeList = conf->getOriginalNodeList(logicalId);
    map<int, int> file2Node; // fileId -> originalNodeId
    map<int, int> fragFileCnt; // nodeId -> fragment file number
    map<int, map<int, int>> rackNodeFileCnt; // rackId -> (nodeId -> fragment file number)
    for(int nodeId : nodeList) {
      const vector<int> &fileList = conf->getNodeStorage(nodeId);
      for(int fileId : fileList) {
        file2Node[fileId] = nodeId;
      }
    }
    for(auto it : codedGroupsInc) {
      int codedFileNum = multiGroupsFileNum[it];
      for(int i = 0; i < codedFileNum; i++) {
        // for every code group
        map<int, vector<pair<int, int>>> rackFileTab; // rackId -> (fileId, partitionId) list
        map<int, set<int>> rackNodeTab; // rackId -> node Cnt;
        map<int, NodeSet> file2NodeSet; // fileId -> same nodeSet
        for(int nodeId : it) {
          if(nodeId == logicalId) {
            continue;
          }
          set<int> subsetR(it);
          subsetR.erase(nodeId);
          int fileId = sameFileTab[subsetR][i];
          if(file2Node[fileId] == reducerId) {
            continue;
          }
          int fileRack = networkTopology[file2Node[fileId] - 1];
          file2NodeSet[fileId] = subsetR;
          rackFileTab[fileRack].push_back(make_pair(fileId, nodeId - 1));
          rackNodeTab[fileRack].insert(file2Node[fileId]);
        }
        for(auto &rackFile : rackFileTab) {
          if(rackFile.first != reducerRackId && rackNodeTab[rackFile.first].size() > 1 && rackFile.first == curRackId) {
            // can be coded locally
            // get encode info for all node
            for(int nodeId : it) {
              vector<tuple<int, int, int>> encodeInfo;
              for(auto fragFile : rackFile.second) {
                int fileId = fragFile.first;
                if(file2NodeSet[fileId].find(nodeId) == file2NodeSet[fileId].end()) {
                  continue;
                }
                int partitionId = fragFile.second;
                int chunkRank = 0, fileNodeId = file2Node[fileId];
                fragFileCnt[fileNodeId]++;
                if(rank == fileNodeId) {
                  innerP2PFragFiles.fileList.insert(make_pair(fileId, partitionId));
                }
                for(int logId : file2NodeSet[fileId]) {
                  if(logId == nodeId) {
                    break;
                  }
                  chunkRank++;
                }
                encodeInfo.push_back(make_tuple(fileId, partitionId, chunkRank));
              }
              innerP2PFragFiles.codeInfo.push_back(encodeInfo);
            }
          } else if(rackFile.first == curRackId) {
            // need point to point trans(for those can't coded or in reducer rack)
            for(auto entireFile : rackFile.second) {
              int fileNodeId = file2Node[entireFile.first];
              if(rank == fileNodeId) {
                innerP2PEntireFiles.insert(entireFile);
              }
            }
          }
          if(rank == reducerId && rackFile.first != reducerRackId && rackNodeTab[rackFile.first].size() > 1) {
            for(auto fragFile : rackFile.second) {
              int fileNodeId = file2Node[fragFile.first];
              rackNodeFileCnt[rackFile.first][fileNodeId]++;
            }
          }
        }
      }
    }
    int leaderId = -1, maxFragCnt = 0;
    for(auto it : fragFileCnt) {
      innerRackGroup.insert(it.first);
      if(it.second > maxFragCnt) {
        leaderId = it.first;
        maxFragCnt = it.second;
      }
    }
    innerP2PFragFiles.leaderId = leaderId;
    innerRackGroup.erase(leaderId);
    if(rank == reducerId) {
      for(auto it : rackNodeFileCnt) {
        int leaderId = -1, maxFragCnt = 0;
        for(auto node : it.second) {
          int nodeId = node.first, fileCnt = node.second;
          if(fileCnt > maxFragCnt) {
            leaderId = nodeId;
            maxFragCnt = fileCnt;
          }
        }
        innerRackLeaders.insert(leaderId);
      }
    }
  } else {
    // calculate intermediate values to be passed within the logical node
    int reducerId = conf->getReducerId(logicalId);
    if(rank == reducerId) {
      return;
    }
    const vector<int> &destFileList = conf->getNodeStorage(reducerId);
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
  }

  return ;
}

vector<NodeSet> CodeGeneration::generateNodeSubset(int r)
{
  vector<NodeSet> list;
  vector<NodeSet> ret;
  list.push_back(NodeSet());
  for (int i = 1; i <= K; i++)
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

vector<NodeSet> CodeGeneration::generateNodeSubsetContain(int nodeId, int r)
{
  set<int> nodes;
  vector<NodeSet> list;
  vector<NodeSet> ret;

  for (int i = 1; i <= K; i++)
  {
    if (i == nodeId)
    {
      NodeSet ns;
      ns.insert(nodeId);
      list.push_back(ns);
      continue;
    }
    nodes.insert(i);
  }

  for (auto nit = nodes.begin(); nit != nodes.end(); nit++)
  {
    int node = *nit;
    unsigned lsize = list.size();
    for (unsigned int i = 0; i < lsize; i++)
    {
      NodeSet ns(list[i]);
      ns.insert(node);
      if (ns.size() == (unsigned int)r)
      {
        ret.push_back(ns);
      }
      else
      {
        list.push_back(ns);
      }
    }
  }

  return ret;
}

// void CodeGeneration::generateSubsetDestVpairList()
// {
//   for( auto it = SubsetSIdMap.begin(); it != SubsetSIdMap.end(); ++it ) {
//     NodeSet s = it->first;
//     SubsetSId id = it->second;
//     for( auto kit = s.begin(); kit != s.end(); ++kit ) {
//       int q = *kit;
//       NodeSet t = s;
//       t.erase( q );
//       for( int n = 1; n <= N; n++ ) {
// 	bool exclusive = true;
// 	if( NodeImMatrix[ q ][ q - 1 ][ n - 1 ] == true ) {
// 	  exclusive = false;
// 	}
// 	else {
// 	  for( auto jit = t.begin(); jit != t.end(); ++jit ) {
// 	    int j = *jit;
// 	    if( NodeImMatrix[ j ][ q - 1 ][ n - 1 ] == false ) {
// 	      exclusive = false;
// 	      break;
// 	    }
// 	  }
// 	}
// 	if( exclusive == true ) {
// 	  SubsetDestVpairList[ id ][ q ].push_back( Vpair( q, n ) );
// 	}
//       }
//     }
//   }
// }

// void CodeGeneration::generateSubsetSrcVjList()
// {
//   for( auto it = SubsetSIdMap.begin(); it != SubsetSIdMap.end(); it++ ) {
//     NodeSet s = it->first;
//     SubsetSId id = it->second;
//     for( auto kit = s.begin(); kit != s.end(); kit++ ) {
//       int k = *kit;
//       NodeSet t = s;
//       t.erase( k );
//       VpairList vpl = SubsetDestVpairList[ id ][ k ];
//       int order = 1;
//       for( auto jit = t.begin(); jit != t.end(); jit++ ) {
// 	int j = *jit;
// 	SubsetSrcVjList[ id ][ j ].push_back( Vj( vpl, k, order ) );
// 	order++;
//       }
//     }
//   }
// }

void CodeGeneration::printNodeSet(NodeSet ns)
{
  cout << '{';
  for (auto nit = ns.begin(); nit != ns.end(); ++nit)
  {
    cout << ' ' << *nit;
    if (nit != --ns.end())
    {
      cout << ',';
    }
  }
  cout << " }";
}

void CodeGeneration::printVpairList(VpairList vpl)
{
  cout << '[';
  for (auto pit = vpl.begin(); pit != vpl.end(); pit++)
  {
    cout << " ( " << pit->first << ", " << pit->second << " )";
    if (pit != --vpl.end())
    {
      cout << ',';
    }
  }
  cout << " ]";
}

SubsetSId CodeGeneration::getSubsetSId(NodeSet ns)
{
  auto it = SubsetSIdMap.find(ns);
  if (it != SubsetSIdMap.end())
  {
    // return it->second;
    return SubsetSIdMap[ns];
  }
  else
  {
    cout << "Cannot find SubsetId\n";
    assert(false);
  }
}
