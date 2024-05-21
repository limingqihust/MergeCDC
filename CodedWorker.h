#ifndef _CMR_WORKER
#define _CMR_WORKER

#include <mpi.h>
#include <unordered_map>
#include <pthread.h>
#include <iostream>

#include "CodedConfiguration.h"
#include "CodeGeneration.h"
#include "Common.h"
#include "Utility.h"
#include "Trie.h"

using namespace std;

class CodedWorker
{
 public:
  typedef unordered_map< unsigned int, LineList* > PartitionCollection; // key = destID
  typedef unordered_map< unsigned int, PartitionCollection > InputPartitionCollection;  // key = inputID
  /* typedef map< unsigned int, LineList* > PartitionCollection; // key = destID */
  /* typedef map< unsigned int, PartitionCollection > InputPartitionCollection;  // key = inputID */
  typedef map< Vpair, unsigned long long > VpairSizeMap;
  typedef struct _TxData {
    unsigned char* data;
    unsigned long long numLine; // Number of lines
  } TxData;
  typedef unordered_map< unsigned int, TxData > PartitionPackData;  // key in { 0, 1, 2, ... }

  typedef struct _DataChunk {
    unsigned char* data;
    unsigned long long size; // number of lines
    _DataChunk() : size(0) {}
  } DataChunk;
  typedef map< VpairList, vector< DataChunk > > DataPartMap;
  typedef unordered_map< SubsetSId, DataPartMap > NodeSetDataPartMap;  // [Encode/Decode]PreData
  /* typedef map< SubsetSId, DataPartMap > NodeSetDataPartMap;  // [Encode/Decode]PreData   */
  
  typedef struct _MetaData {
    VpairList vpList;
    VpairSizeMap vpSize;
    unsigned int partNumber; // { 1, 2, ... }
    unsigned long long size; // number of lines
  } MetaData;
  
  typedef struct _EnData {
    vector< MetaData > metaList;
    unsigned char* data;          // encoded chunk
    unsigned long long size;      // in number of lines
    unsigned char* serialMeta;
    unsigned long long metaSize;  // in number of bytes
  } EnData;
  struct EnFragData {
    vector<tuple<int, int, int, unsigned long long>> metaList; // (fileId, partitionId, chunkRank, numLine)
    int maxSizeFileId;
    unsigned long long size;
    unsigned char *data;
    unsigned long long metaSize;
    unsigned char *serialMeta;
  };
  typedef unordered_map< SubsetSId, vector<EnData> > NodeSetEnDataMap;  // SendData, nsid -> (idx: fileId, element: EnData)
  typedef unordered_map< SubsetSId, vector<vector< EnData >> > NodeSetVecEnDataMap;  // RecvDat, nsid -> (idx: fileId, (idx: nodeId, element: EnData))
  /* typedef map< SubsetSId, EnData > NodeSetEnDataMap;  // SendData */
  /* typedef map< SubsetSId, vector< EnData > > NodeSetVecEnDataMap;  // RecvData   */
  

  typedef struct {
    SubsetSId sid;
    EnData endata;
  } DecodeJob;

  
 private:
  MPI_Comm workerComm;
  CodeGeneration* cg;

  PartitionList partitionList;
  InputPartitionCollection inputPartitionCollection;
  LineList localList;
  NodeSet localLoadSet;
  TrieNode* trie;


  NodeSetEnDataMap encodeDataSend;
  NodeSetVecEnDataMap encodeDataRecv;
  pthread_t decodeThread;
  PartitionPackData partitionTxData;
  PartitionPackData partitionRxData;
  DataChunk InnerNodeEntireTxData;
  DataChunk InnerNodeFragPreTxData;
  DataChunk InnerNodeCodedFragTxData;
  DataChunk InnerNodeTxData;
  double shuffleTx;
  double shuffleTime;

 public: // Because of thread
  CodedConfiguration* conf;
  unsigned int rank;  
  NodeSetDataPartMap encodePreData;
  NodeSetDataPartMap decodePreData;    
  //queue< DecodeJob > decodeQueue;  // For parallel decode
  unsigned long maxDecodeJob;  

 public:
 CodedWorker( unsigned int _rank ): rank( _rank ) {}
  ~CodedWorker();
  void setWorkerComm( MPI_Comm& comm ) { workerComm = comm; }
  void run();
  

 private:
  unsigned int findAssociatePartition( const unsigned char* key );
  void execMap();
  void execReduce();
  void execEncoding();
  void execShuffle();
  void execDecoding();
  void execInnerNodeTrans();
  void execInnerNodeTransCoded();
  static void* parallelDecoder( void* pthis );
  void sendEncodeData( EnData& endata, MPI_Comm& comm );
  void recvEncodeData( SubsetSId nsid, unsigned int actId, MPI_Comm& comm );
  void sendEncodeDataMulticast( SubsetSId nsid, EnData& endata );
  void recvEncodeDataMulticast( SubsetSId nsid, int fileIdx );
  void genMulticastGroup();
  void printLocalList();
  void writeInputPartitionCollection();
  void outputLocalList();
  TrieNode* buildTrie( PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize, int maxDepth );
};


#endif
