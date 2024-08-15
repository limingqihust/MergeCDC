#ifndef _MR_WORKER
#define _MR_WORKER

#include <unordered_map>

#include "Configuration.h"
#include "Common.h"
#include "Utility.h"
#include "Trie.h"

class Worker
{
 public:
  typedef unordered_map< unsigned int, LineList* > PartitionCollection;
  typedef unordered_map< unsigned int, PartitionCollection > InputPartitionCollection;  // key = fileId
  typedef struct _TxData {
    unsigned char* data;
    unsigned long long numLine; // Number of lines
  } TxData;
  typedef unordered_map< unsigned int, TxData > PartitionPackData;  // key in { 0, 1, 2, ... }

 private:
  Configuration* conf;
  unsigned int rank;
  PartitionList partitionList;
  // PartitionCollection partitionCollection;
  InputPartitionCollection inputPartitionCollection;

  PartitionPackData partitionTxData;
  PartitionPackData partitionRxData;
  LineList localList;
  TrieNode* trie;

 public:
 Worker( unsigned int _rank ): rank( _rank ) {}
  ~Worker();
  void run();

 private:
  void execMap();
  void execReduce();
  void printLocalList();
  void outputLocalList();
  TrieNode* buildTrie( PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize, int maxDepth );
  void updateInputPartitionCollection4WordCount();
  void updateLineList4WordCount(const std::unordered_map<std::string, int>& word_count, LineList* lineList);
  void execReduceWordCount();
  std::string key2String(const unsigned char* key, unsigned int size);
  int key2Int(const unsigned char* key, unsigned int size);
};


#endif
