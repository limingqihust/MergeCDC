#ifndef _FILE_DISTRIBUTION_HH_
#define _FILE_DISTRIBUTION_HH_

#include <vector>
#include <assert.h>
#include <iostream>
#include <string>
#include <cmath>
#include <set>
#include <cstdlib>
#include <fstream>
#include <sstream>

using namespace std;

class FileDistGenerator {
private:
    int fileNum; 
    int originalNodeNum; // original node number(random distribution)
    int destNodeNum; // node number of standard distribution
    int replicaNum; // number of replicas
    int rackNum; // number of rack
    vector<vector<int> > stdDist; // node -> standard file distribution
public:
    FileDistGenerator(int fn, int onn, int dnn, int r, int rn);
    ~FileDistGenerator();
    void GenerateStandardDistribution();
    vector<vector<int> > GetRandomDistribution();
    vector<vector<int> > GetRandomDistributionFromStandard();
    double GetSimilarity(vector<int> nodeX, vector<int> nodeY);
    int Dot(string &a, string &b);
    int GetFileNum() { return fileNum; }
    double GetVectorLength(string &s);
    vector<int> GenerateRandomNetworkTopology();
    pair<vector<vector<int>>, vector<int>> GetDistributionFromFile(string filePath);
};

#endif