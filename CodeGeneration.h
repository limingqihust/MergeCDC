#ifndef _CMR_CODEGENERATION
#define _CMR_CODEGENERATION

#include <vector>
#include <set>
#include <map>
#include <utility>
#include <unordered_map>
#include <assert.h>
#include <climits>
#include <tuple>
#include "CodedConfiguration.h"

using namespace std;

typedef set< int > NodeSet;
typedef set< int > InputSet;
typedef vector< vector< bool > > ImMatrix;
typedef pair< int, int > Vpair; // < destId, inputId >
typedef vector< Vpair > VpairList;
typedef struct _Vj {
  VpairList vpList;
  int dest;
  int order;
_Vj( VpairList vpl, int _dest, int _order ): vpList( vpl ), dest( _dest ), order( _order ) {}
} Vj;
typedef vector< Vj > VjList;
typedef unsigned int SubsetSId;
struct InnerP2PFragInfo {
  int leaderId; // node that collects and codes intermediate value
  set<pair<int, int>> fileList; // (fileId, partitionId)
  vector<vector<tuple<int, int, int>>> codeInfo; // (code group idx, (code fragment idx, (fileId, partitionId, chunkRank)))
  InnerP2PFragInfo() : leaderId(-1) {}
};

class CodeGeneration {
 private:
  int N; // file num
  int K;
  int R;
  int Eta;
  CodedConfiguration* conf;
  vector< NodeSet > NodeSubsetR; // subset of nodes of size R
  vector< NodeSet > NodeSubsetS; // subset of nodes of size R+1
  map< NodeSet, int > multiGroupsFileNum;  // node subset( R + 1, only contain subsets that can be coded ) -> file num 
  map< NodeSet, SubsetSId > SubsetSIdMap;  // node subset( R + 1, only contain subsets that can be coded ) -> node subset id
  vector<set<int>> p2pFiles; // idx: nodeId, element: file ids need to be transmitted point to point
  set<int> innerP2pFiles; // element: intermediate values to be passed inner logical node
  map<int, vector<NodeSet> > multicastGroupsTab; // nodeId -> coded multicast groups containing nodeid
  map<NodeSet, vector<int>> sameFileTab; // nodeset(size R) -> same fileid
  map<int, vector<int>> file2ReducerNeeded; // fileId -> reducer logical id( need fileId during coded shuffle)
  set<pair<int, int>> innerP2PEntireFiles; // element: entire intermediate values to be passed inner logical node (fileId, partitionId)
  InnerP2PFragInfo innerP2PFragFiles; // intermediate values fragment to be passed inner logical node
  set<int> innerRackGroup; // nodeId in one inner rack group
  set<int> innerRackLeaders; // leader nodeId list in current logical node
  /* unordered_map< int, InputSet > M;  // Map: key = nodeId, value = files at the node */
  /* unordered_map< int, ImMatrix > NodeImMatrix; */
  // map< int, InputSet > M;  // Map: key = nodeId, value = files at the node
  // map< int, ImMatrix > NodeImMatrix;  

  /* unordered_map< SubsetSId, unordered_map< int, VpairList > > SubsetDestVpairList; */
  /* unordered_map< SubsetSId, unordered_map< int, VjList > > SubsetSrcVjList; */

  /* unordered_map< int, vector< NodeSet > > NodeSubsetSMap; // Map: key = nodeID, value = list of subset of size R+1 containing nodeId */
  /* unordered_map< unsigned long, NodeSet > FileNodeMap; */
  // map< unsigned long, NodeSet > FileNodeMap;  // Map: fileid -> B_\tau
  // map< NodeSet, unsigned long > NodeFileMap;  // Map: B_\tau -> fileid

 public:
  CodeGeneration( int _N, int _K, int _R, int rank, CodedConfiguration *conf );
  ~CodeGeneration() {}
  static void printNodeSet( NodeSet ns );
  static void printVpairList( VpairList vpl );

  vector< NodeSet >& getNodeSubsetR() { return NodeSubsetR; }
  vector< NodeSet >& getNodeSubsetS() { return NodeSubsetS; }
  map< NodeSet, int >& getmultiGroupsFileNum() { return multiGroupsFileNum; }
  const vector<NodeSet> & getNodeSubsetSContain(int nodeId) { return multicastGroupsTab[nodeId]; }
  /* unordered_map< int, InputSet >& getM() { return M; } */
  /* unordered_map< int, ImMatrix >& getNodeImMatrix() { return NodeImMatrix; } */
  
  /* unordered_map< SubsetSId, unordered_map< int, VpairList > >& getSubsetDestVpairList() { return SubsetDestVpairList; } */
  /* unordered_map< SubsetSId, unordered_map< int, VjList > >& getSubsetSrcVjList() { return SubsetSrcVjList; } */
  int getEta() { return Eta; }
  int getN() { return N; }
  int getK() { return K; }
  int getR() { return R; }
  int getCodedFileNum(NodeSet &ns) { return multiGroupsFileNum[ns]; }
  const vector<int> &getSameFileList(NodeSet &ns) { return sameFileTab[ns]; }
  SubsetSId getSubsetSId( NodeSet ns );
  const set<int> &getP2pFileList(int rank) { return p2pFiles[rank - 1]; }
  const set<int> &getInnerP2pFileList() { return innerP2pFiles; }
  const vector<int> &getNeededReducer(int fileId) { return file2ReducerNeeded[fileId]; }
  const set<pair<int, int>> &getInnerP2pEntireFiles() { return innerP2PEntireFiles; }
  const InnerP2PFragInfo &getInnerP2pFragFileInfo() { return innerP2PFragFiles; }
  bool isLeaderOfRank(int rank) { return innerRackGroup.find(rank) != innerRackGroup.end(); }
  bool isreducerOfLeader(int rank) { return innerRackLeaders.find(rank) != innerRackLeaders.end(); }
  int CalCombination(int m, int n) { // calculate m choose n
    long long res = 1;
    for(int i = 0; i < n; i++) {
        res *= m - i;
    }
    for(int i = 1; i <= n; i++) {
        res /= i;
    }
    return int(res);
}

 private:
  vector< NodeSet > generateNodeSubset( int r );

  vector< NodeSet > generateNodeSubsetContain( int nodeId, int r );
};


#endif
