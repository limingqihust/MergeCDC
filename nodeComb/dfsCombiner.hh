#ifndef _DFS_COMBINER_HH_
#define _DFS_COMBINER_HH_

#include <vector>
#include "combiner.hh"

using namespace std;

class DFSCombiner {
public:
    DFSCombiner(NodeInfo *info, int orgN, int destN, int fn, int r);
    ~DFSCombiner();
    void GetCombinationResult(vector<int> &nparts);

private:
    class Graph {
    public:
        int nvtxs, nedges; // The number of vertices and edges in current graph
        vector<int> xadj; // vertices
        vector<int> vwgt; // weight of vertices (input file number)
        vector<int> adjncy; // edges, array that stores the adjacency lists of nvtxs in CSR format
        vector<int> adjwgt; // weight of edges, array that stores the weights of the adjacency lists in CSR format
        int tvwgt; // The sum of the vertex weights in the graph
        vector<int> where; // the combination map from orginal node to logical node
        Graph() { tvwgt = 0; }
        Graph(int nv) : nvtxs(nv) {
            where.resize(nv);
            iota(where.begin(), where.end(), 0);
            tvwgt = 0;
        }
        ~Graph() {}
    };
    NodeInfo *nodeInfo; 
    int orgNodeNum; // number of original nodes
    int destNodeNum; // number of destination nodes
    int fileNum; // number of input files
    int replicaNum; // replication of files
    double ubFactor = 2; // max unbalanced factor
    double crossRackFactor = 0.7; // punishment of the combination cross-rack
    double minCut = -1;
    int maxRackNum = 2; // no more than maxRackNum racks in one logicial vertice
    vector<vector<int>> stdFileDist; // standard file distribution for cdc
    vector<int> where; // best combination
    double InnerRackFactor = 20.0; // the network resources factor = inner-rack bandwith
    bool debugFlag = false; 

    void CombineNodesRecursive(vector<vector<int>> &combinedNodes, vector<map<int, int>> &combinedRacks, int curIdx);
    double CalculateCombinationQuality(vector<vector<int>> &combinedNodes);
    double CalculateCombinationQualityV2(vector<vector<int>> &combinedNodes);
    double CalculateCut(Graph *g);
    double CalculateCutV2(Graph *g);
    Graph *ConvertNodeInfo2Graph(vector<vector<int>> &combinedNodes);
    int Dot(string &a, string &b);
    double GetVectorLength(string &s);
    double GetSimilarity(const vector<int> &nodeX, const vector<int> &nodeY);
    void GenerateStdFileDist();
    double CodeGeneration(int rank, int logicalId, vector<vector<int>> &combinedNodes);
    vector<set<int>> GenerateNodeSubset(int r);
    vector<set<int>> GetInputDistribution(vector<vector<int>> &combinedNodes) {
        vector<set<int>> combinedInputDistributionDist(fileNum);
        vector<vector<int>> combinedNodeStorage(destNodeNum);
        for(int i = 0; i < destNodeNum; i++) {
            for(int orgNodeIdx : combinedNodes[i]) {
                combinedNodeStorage[i].insert(combinedNodeStorage[i].end(), nodeInfo->fileDistribution[orgNodeIdx].begin(), nodeInfo->fileDistribution[orgNodeIdx].end());
            }
        }
        for(int i = 0; i < destNodeNum; i++) {
            for(int fileId : combinedNodeStorage[i]) {
                combinedInputDistributionDist[fileId].insert(i + 1);
            }
        }
        return combinedInputDistributionDist;
    }
    vector<int> GetReducerNodes(vector<vector<int>> &combinedNodes) {
        vector<int> reducerNodes;
        vector<map<int, int>> logRackInfo(destNodeNum);
        vector<int> logTopRack(destNodeNum);
        for(int i = 0; i < destNodeNum; i++) {
            for(int originIdx : combinedNodes[i]) {
                int rackId = nodeInfo->networkTopology[originIdx];
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
        for(int i = 0; i < destNodeNum; i++) {
            int maxFileNodeIdx = 0, topRack = logTopRack[i];
            for(int j = 1; j < combinedNodes[i].size(); j++) {
                int originIdx = combinedNodes[i][j];
                if(nodeInfo->networkTopology[originIdx] == topRack 
                && (nodeInfo->networkTopology[combinedNodes[i][maxFileNodeIdx]] != topRack 
                    || nodeInfo->fileDistribution[originIdx].size() > nodeInfo->fileDistribution[combinedNodes[i][maxFileNodeIdx]].size())) {
                maxFileNodeIdx = j;
                }
            }
            reducerNodes.push_back(combinedNodes[i][maxFileNodeIdx] + 1);
        }
        return reducerNodes;
    }
};


#endif