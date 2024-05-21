#ifndef _COMBINATION_HH_
#define _COMBINATION_HH_

#include <vector>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <map>
#include <set>
#include <cmath>
#include <climits>
#include "fileDIstribution.hh"
#include "dfsCombiner.hh"

using namespace std;

class Graph {
public:
    FileDistGenerator *fileDist;
    vector<vector<int> > blockInfo; // node -> block list
    vector<int> networkTopology;
    vector<int> xadj; // nodes
    vector<int> adjncy; // edges
    vector<int> adjwgt; // weight of edges
    vector<int> vwgt; // weight of nodes
    Graph(FileDistGenerator *fd) : fileDist(fd) {};
    ~Graph() {};
    void Convert2Graph();
};

class Combination {
private:
    Graph *graph;
    int nParts; // number of subgraph
    int replicaNum; // number of replicas
    vector<int> parts; // result of metis
    vector<vector<int> > combinedDist; // result of node combination
    vector<map<int, vector<int>> > fileRealNodeTab; // idx: combined node id -> element: fileid -> real node id list
public:
    Combination(Graph *g, int p, int r) : graph(g), nParts(p), replicaNum(r) { combinedDist.resize(nParts); fileRealNodeTab.resize(nParts); };
    ~Combination() { delete graph; };
    void CombineNodes();
    void GenRandomDist();
    void GenMyCombinationDist();
    void GenDFSCombinationDist();
    void SearchForGroups();
    void ShowResult();
    int CalCombination(int m, int n);
};

#endif