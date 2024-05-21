#ifndef _MR_CONFIGURATION
#define _MR_CONFIGURATION

#include <cstring>
#include <vector>
#include <set>
#include <map>
#include <unordered_set>
#include <fstream>
#include <iostream>
#include <cassert>
#include <string>
#include <sstream>
#include <random>
#include <algorithm>
#include <sys/time.h>

using namespace std;

class Configuration {

 protected:
  unsigned int numReducer;
  unsigned int numMapper;
  unsigned int numInput;
  bool enableInnerCode;
  bool enableNodeCombination;

  char inputPath[1024];
  char outputPath[1024];
  char partitionPath[1024];
  unsigned long numSamples;

  int fileNum; // number of input file, index from 0 to fileNum - 1
  char distributionPath[1024];
  char combinationPath[1024];
  vector<vector<int>> combinedInputDistribution; // file id -> logical(combined) node id list
  vector<set<int>> combinedInputDistributionDist; // file id -> logical node id set
  vector<set<int>> uncombinedInputDistributionDist; // file id -> logical node id set (but only local input files)
  vector<vector<int>> combinedNodeStorage; // logical node id -> stored file id list
  vector<vector<int>> originNodeStorage; // original node id -> stored file id list
  vector<vector<int>> originInputDistribution; // file id -> original node id
  vector<vector<int>> mapWorkLoad; // original node id -> file id list to be mapped
  vector<int> file2MapWorker; // idx: file id, element: mapper original node id
  vector<int> reducerNodes; // reducer node id(original node id)
  vector<int> origin2LogicalNode; // idx: origin node id, element: logical node id
  vector<vector<int>> logical2OriginNode; // idx: logical node id, element: origin node id list
  vector<map<int, vector<int> > > InnerNodeInputDist; // idx: logical node id, element: fileId -> original nodeid set
  vector<int> networkTopology; // idx: original node id, element: rackId

 public:
  Configuration() {
    numMapper = 30;
    numReducer = 5; // worker node id(rank) index from 1 to numReducer
    numInput = numMapper;
    fileNum = 100;
    enableInnerCode = true;
    enableNodeCombination = true;

    // strcpy(inputPath, "./Input/tera10G");
    // strcpy(outputPath, "./Output/tera10G");
    // strcpy(partitionPath, "./Partition/tera10G");
    // numSamples = 100000000;
    strcpy(inputPath, "./Input/Input10000");
    strcpy(outputPath, "./Output/Output10000");
    strcpy(partitionPath, "./Partition/Output10000");
    numSamples = 10000;
    // strcpy(inputPath, "./Input/Input10000000");
    // strcpy(outputPath, "./Output/Input10000000");
    // strcpy(partitionPath, "./Partition/Input10000000");
    // numSamples = 10000000;
    strcpy(distributionPath, "./Distribution/Distribution");
    strcpy(combinationPath, "./Distribution/combination_my");
  }
  ~Configuration() {}
  const static unsigned int KEY_SIZE = 10;
  const static unsigned int VALUE_SIZE = 90;

  unsigned int getNumReducer() const { return numReducer; }
  unsigned int getNumMapper() const { return numMapper; }
  unsigned int getNumInput() const { return numInput; }
  const char *getInputPath() const { return inputPath; }
  const char *getOutputPath() const { return outputPath; }
  const char *getPartitionPath() const { return partitionPath; }
  unsigned int getKeySize() const { return KEY_SIZE; }
  unsigned int getValueSize() const { return VALUE_SIZE; }
  unsigned int getLineSize() const { return KEY_SIZE + VALUE_SIZE; }
  unsigned long getNumSamples() const { return numSamples; }
  unsigned int getFileNum() const { return fileNum; }
  const vector<int> &getWorkLoad(int rank) const { 
    return mapWorkLoad[rank - 1]; 
  }
  const vector<set<int>> &getInputDistribution() const {
    if(enableNodeCombination) {
      return combinedInputDistributionDist;
    } else {
      return uncombinedInputDistributionDist;
    }
  }
  const vector<int> &getNodeStorage(int rank) const {
    return originNodeStorage[rank - 1];
  }
  const vector<int> &getCombinedNodeStorage(int logicalId) {
    return combinedNodeStorage[logicalId - 1];
  }
  int getMapNode(int fileId) const { return file2MapWorker[fileId]; }
  const vector<int> getChunkNum(int fileId, int rank) {
    vector<int> res;
    for(int i = 0; i < originInputDistribution[fileId].size(); i++) {
      if(originInputDistribution[fileId][i] == rank) {
        res.push_back(i);
      }
    }
    return res;
  }
  const vector<int> getChunkNumInnerNode(int fileId, int rank) {
    int logicalId = origin2LogicalNode[rank - 1];
    vector<int> res;
    const vector<int> &fileDist = InnerNodeInputDist[logicalId - 1][fileId];
    for(int i = 0; i < fileDist.size(); i++) {
      if(fileDist[i] == rank) {
        res.push_back(i);
      }
    }
    return res;
  }
  int getInnerNodeNumPart(int fileId, int logicalId) {
    return InnerNodeInputDist[logicalId - 1][fileId].size();
  }
  const vector<int> &getReducerNodes() const { return reducerNodes; }
  int getLogicalNodeId(int rank) { return origin2LogicalNode[rank - 1]; }
  int getReducerId(int logicalId) { return reducerNodes[logicalId - 1]; }
  const vector<int> &getOriginalNodeList(int logicalId) { return logical2OriginNode[logicalId - 1]; }
  const vector<int> &getNetworkTopology() { return networkTopology; }
  bool EnableInnerCode() { return enableInnerCode; }
  bool EnableNodeCombination() { return enableNodeCombination; }
  void ParseFileDistribution() {
    originNodeStorage.resize(numMapper);
    originInputDistribution.resize(fileNum);
    combinedNodeStorage.resize(numReducer);
    combinedInputDistribution.resize(fileNum);
    combinedInputDistributionDist.resize(fileNum);
    uncombinedInputDistributionDist.resize(fileNum);
    mapWorkLoad.resize(numMapper);
    file2MapWorker.resize(fileNum);
    origin2LogicalNode.resize(numMapper);
    logical2OriginNode.resize(numReducer);
    InnerNodeInputDist.resize(numReducer);
    srand(0);

    ifstream ifs(distributionPath);
    if(!ifs.is_open()) {
      cout << "can't open distribution file" << distributionPath << endl;
      assert(false);
    }
    // read distribution from distributionFile
    for(unsigned int i = 0 ; i < numMapper; i++) {
      string line;
      if(getline(ifs, line)) {
        istringstream iss(line);
        int fileId;
        while(iss >> fileId) {
          originNodeStorage[i].push_back(fileId);
          originInputDistribution[fileId].push_back(i + 1);
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
    assert(networkTopology.size() == numMapper);
    ifs.close();
    // read node combination result
    ifs = ifstream(combinationPath);
    int originalNodeId = 0;
    if(getline(ifs, line)) {
      istringstream iss(line);
      int logicalNodeId;
      while(iss >> logicalNodeId) {
        originalNodeId++;
        assert(logicalNodeId >= 0 && logicalNodeId < numReducer);
        origin2LogicalNode[originalNodeId - 1] = logicalNodeId + 1;
        logical2OriginNode[logicalNodeId].push_back(originalNodeId);
        combinedNodeStorage[logicalNodeId].insert(combinedNodeStorage[logicalNodeId].end(), originNodeStorage[originalNodeId - 1].begin(), originNodeStorage[originalNodeId - 1].end());
      }
    } else {
      cout << "can't read node combination result from file: " << combinationPath << endl;
      assert(false);
    }
    // get top rack for each logical node
    vector<map<int, int>> logRackInfo(numReducer);
    vector<int> logTopRack(numReducer);
    for(int i = 0; i < numReducer; i++) {
      assert(!logical2OriginNode[i].empty());
      for(int originId : logical2OriginNode[i]) {
        int rackId = networkTopology[originId - 1];
        logRackInfo[i][rackId]++;
      }
      int maxRackId = -1, maxRackNum = 0;
      for(auto it : logRackInfo[i]) {
        if(it.second > maxRackNum) {
          maxRackId = it.first;
          maxRackNum = it.second;
        }
      }
      logTopRack[i] = maxRackId;
    }
    for(int i = 0; i < numReducer; i++) {
      int maxFileNodeIdx = 0, topRack = logTopRack[i];
      for(int j = 1; j < logical2OriginNode[i].size(); j++) {
        int originId = logical2OriginNode[i][j];
        if(networkTopology[originId - 1] == topRack 
           && (networkTopology[logical2OriginNode[i][maxFileNodeIdx] - 1] != topRack 
               || originNodeStorage[originId - 1].size() > originNodeStorage[logical2OriginNode[i][maxFileNodeIdx] - 1].size())) {
          maxFileNodeIdx = j;
        }
      }
      reducerNodes.push_back(logical2OriginNode[i][maxFileNodeIdx]);
    }
    // if(getline(ifs, line)) {
    //   istringstream iss(line);
    //   int groupSize, nodeId = 1, logicalNodeId = 0;
    //   while(iss >> groupSize) {
    //     reducerNodes.push_back(nodeId);
    //     for(int i = nodeId; i < nodeId + groupSize; i++) {
    //       origin2LogicalNode[i - 1] = logicalNodeId + 1;
    //       logical2OriginNode[logicalNodeId].push_back(i);
    //       combinedNodeStorage[logicalNodeId].insert(combinedNodeStorage[logicalNodeId].end(), originNodeStorage[i - 1].begin(), originNodeStorage[i - 1].end());
    //     }
    //     logicalNodeId++;
    //     nodeId += groupSize;
    //   }
    // } else {
    //   cout << "can't read node combination result from distribution file " << endl;
    //   assert(false);
    // }
    ifs.close();
    assert(reducerNodes.size() == numReducer);
    

    // for each file, select the first node as its worker node
    for(int i = 0; i < fileNum; i++) {
      assert(!originInputDistribution[i].empty());
      // int nodeId = inputDistribution[i][0];
      int nodeId = originInputDistribution[i][rand() % originInputDistribution[i].size()];
      mapWorkLoad[nodeId - 1].push_back(i);
      file2MapWorker[i] = nodeId;
    }

    // calculate uncombined input distribution 
    for(int i = 0; i < numReducer; i++) {
      int orgNodeId = reducerNodes[i];
      for(int fileId : originNodeStorage[orgNodeId - 1]) {
        uncombinedInputDistributionDist[fileId].insert(i + 1);
      }
    }

    // calculate combined input distribution for cdc
    for(int i = 0; i < numReducer; i++) {
      for(int fileId : combinedNodeStorage[i]) {
        combinedInputDistribution[fileId].push_back(i + 1);
        combinedInputDistributionDist[fileId].insert(i + 1);
      }
    }

    // calculate inner node input distribution
    for(int i = 1; i <= numMapper; i++) {
      int logicalId = origin2LogicalNode[i - 1];
      for(int fileId : originNodeStorage[i - 1]) {
        InnerNodeInputDist[logicalId - 1][fileId].push_back(i);
      }
    }
    return ;
  }
};

#endif
