#ifndef _COMBINER_HH_
#define _COMBINER_HH_

#include <vector>
#include <assert.h>
#include <set>
#include <string>
#include <cmath>
#include <algorithm>
#include <random>
#include <numeric>
#include <unordered_map>
#include <queue>
#include <map>
#include <climits>
#include <iostream>
#include <sys/time.h>

using namespace std;

struct NodeInfo {
    vector<vector<int>> fileDistribution;
    vector<int> networkTopology;
};

typedef set<int> NodeSet;

class Combiner {
public:
    Combiner(NodeInfo *info, int nv, int destN, int fn, int r);
    ~Combiner();
    void GetCombinationResult(vector<int> &nparts);
private:
    struct KwayNbrInfo {
        int id; // similarity between current node and its partition
        int ed; // average similarity( only >= id) between current node and adjacent partition
        int pNum; // number of partitions( only >= id)
        vector<int> nbrs; // similarity between current node and all partitions
    };
    class Graph {
    public:
        // definition for graph
        int nvtxs, nedges; // The number of vertices and edges in current graph
        int orgNvtxs; // The number of vertices and edges in original graph
        vector<int> xadj; // vertices
        vector<int> vwgt; // weight of vertices (input file number)
        vector<int> adjncy; // edges, array that stores the adjacency lists of nvtxs in CSR format
        vector<int> adjwgt; // weight of edges, array that stores the weights of the adjacency lists in CSR format
        int tvwgt; // The sum of the vertex weights in the graph
        double invtvwgt; // The inverse of the sum of the vertex weights in the graph
        vector<int> cmap; // the coarsening map
        vector<int> orgvwgt; // weight of origin vertices (input file number)
        set<int> bndList; // boundary nodes ( ed >= id )
        vector<KwayNbrInfo> ckrinfo; // neighbor infos
        vector<int> where; // the coarsening map from orginal node to current coarsening node
        Graph *cgraph, *finer; //the linked-list structure of the sequence of graphs
        vector<vector<pair<int, int>>> vracks; // racks of vertices in current graph
        vector<set<int>> topKRacks; // top k racks of vertices in current graph
        int kOfRack = 1; // only select top k rack
        int maxRackNum = 2; // no more than maxRackNum racks in one logicial vertice

        Graph() {
            cgraph = finer = NULL;
        }
        Graph(int nv) : nvtxs(nv), orgNvtxs(nv) {
            cmap.resize(nv);
            where.resize(nv);
            ckrinfo.resize(nv);
            vracks.resize(nv);
            topKRacks.resize(nv);
            iota(where.begin(), where.end(), 0);
            cgraph = finer = NULL;
            tvwgt = 0;
        }
        Graph(int nv, Graph *f) : nvtxs(nv), finer(f), orgvwgt(f->orgvwgt) {
            cmap.resize(nv);
            vracks.resize(nv);
            topKRacks.resize(nv);
            this->orgNvtxs = f->orgNvtxs;
            this->tvwgt = f->tvwgt;
            this->invtvwgt = f->invtvwgt;
            where.resize(f->orgNvtxs);
            ckrinfo.resize(f->orgNvtxs);
            cgraph = NULL;
        }
        ~Graph() {
            if(cgraph) {
                delete cgraph; // delete cgraph recursively
                cgraph = NULL;
            }
            return ;
        }
    };
    enum {
        UNMATCHED = -1,
        OMODE_REFINE = 1,
        OMODE_BALANCE = 2,
        OMODE_NETWORK_TOPOLOGY = 3,
        VPQSTATUS_NOTPRESENT = 4,
        VPQSTATUS_PRESENT = 5,
        VPQSTATUS_EXTRACTED = 6
    };
    Graph *graph;
    NodeInfo *nodeInfo;
    int fileNum; // input file number
    int replicaNum; // replication of files
    vector<vector<int>> stdFileDist; // standard file distribution for cdc
    vector<vector<int>> topSimStdFileDist; // top k standard distributions(for each vtx i, top k partitions(idx j) that have higher similarity between file[i] and stdFileDist[j])
    // definition for ctrl information
    double ubfactor; // the maximum allowed load imbalance among the partitions
    int combineTo; // destination node number after combination
    int iterNum; // iteration number for getting combination result
    double balEIFactor; // in OMODE_BALANCE mode, is boundary only when ed/id >= balEIFactor, balEIFactor in [0,1]
    double netTpEIFactor; // in OMODE_NETWORK_TOPOLOGY mode, is boundary only when ed/id >= netTpEIFactor, netTpEIFactor in [0,1]

    void GenerateStdFileDist();
    void ConvertNodeInfo2Graph();
    int Dot(string &a, string &b);
    double GetVectorLength(string &s);
    double GetSimilarity(const vector<int> &nodeX, const vector<int> &nodeY);
    double GetSimilarityForVtx(int vtx, const vector<int> &nodeX, const vector<int> &nodeY);
    void GreedyMatch(Graph *curGraph);
    void GreedyMatchForDestStrict(Graph *curGraph);
    void CreateCoarsenGraph(Graph *curGraph, int cnvtxs, const vector<int> &match);
    void CalculateCombinationQuality(Graph *g, double &ubVariance, int &codeNum);
    void GenerateNodeSubset(int r, vector<NodeSet> &nodeSubSet);
    void ComputeKWayRefineParams(Graph *g, double modeEIFactor);
    void ComputeKWayBoundary(Graph *g, int omode);
    void GreedyKWayOptimize(Graph *g, int niter, int omode);
    void UpdateVertexInfoAndBND(Graph *g, int movedVtx, int from, int to, int omode);
    void DeleteCoarsenGraph();
    void UpdateAdjacencyEdges(Graph *g);
    void CalculateTopKStdDist();
    void AdjustSingleRackNode(Graph *g);
    void ShowRackInfo(Graph *g);
};

#endif