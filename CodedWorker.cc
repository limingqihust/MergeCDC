#include <iostream>
#include <mpi.h>
#include <iomanip>
#include <fstream>
#include <cstdio>
#include <map>
#include <unordered_map>
#include <assert.h>
#include <algorithm>
#include <ctime>
#include <string.h>
#include <cstdint>

#include "CodedWorker.h"
#include "CodedConfiguration.h"
#include "Common.h"
#include "Utility.h"
#include "CodeGeneration.h"

using namespace std;

CodedWorker::~CodedWorker()
{
  for (auto it = partitionList.begin(); it != partitionList.end(); ++it)
  {
    delete[] * it;
  }

  // Delete from encodePreData
  for (auto it = encodePreData.begin(); it != encodePreData.end(); it++)
  {
    DataPartMap dp = it->second;
    for (auto it2 = dp.begin(); it2 != dp.end(); it2++)
    {
      vector<DataChunk> &vdc = it2->second;
      for (auto dcit = vdc.begin(); dcit != vdc.end(); dcit++)
      {
        delete[] dcit->data;
      }
    }
  }

  // Delete from localList
  for (auto it = localList.begin(); it != localList.end(); ++it)
  {
    delete[] * it;
  }

  delete trie;
  delete cg;
  delete conf;
}

void CodedWorker::run()
{
  // RECEIVE CONFIGURATION FROM MASTER
  conf = new CodedConfiguration;
  MPI_Bcast((void *)conf, sizeof(CodedConfiguration), MPI_CHAR, 0, MPI_COMM_WORLD);
  conf->ParseFileDistribution();

  // RECEIVE PARTITIONS FROM MASTER
  for (unsigned int i = 1; i < conf->getNumReducer(); i++)
  {
    unsigned char *buff = new unsigned char[conf->getKeySize() + 1];
    MPI_Bcast(buff, conf->getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
    partitionList.push_back(buff);
  }

  struct timeval start, end;
  double rTime;

  // codegen: search and generate multicast groups
  gettimeofday(&start,NULL);
  // cg = new CodeGeneration(conf->getNumInput(), conf->getNumReducer(), conf->getLoad());
  cg = new CodeGeneration(conf->getFileNum(), conf->getNumReducer(), conf->getLoad(), rank, conf);
  genMulticastGroup();
  gettimeofday(&end,NULL);
  rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  // EXECUTE MAP PHASE
  execMap();

  // EXECUTE INNER NODE INTERMEDIATE VALUE TRANSFER
  shuffleTime = 0;
  shuffleTx = 0;
  if(conf->EnableNodeCombination()) {
    if(conf->EnableInnerCode()) {
      execInnerNodeTransCoded();
    } else {
      execInnerNodeTrans();
    }
  }
  MPI_Gather(&shuffleTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
  MPI_Gather(&shuffleTx, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  // EXECUTE ENCODING PHASE
  gettimeofday(&start,NULL);
  execEncoding();
  gettimeofday(&end,NULL);
  rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  // // START PARALLEL DECODE
  // // maxDecodeJob = ( cg->getNodeSubsetS().size() * ( cg->getR() * ( cg->getR() + 1 ) ) ) / cg->getK();
  // // int tid = pthread_create( &decodeThread, NULL, parallelDecoder, (void*) this );
  // // if( tid ) {
  // //   cout << rank << ": ERROR -- cannot create parallel decoder thread\n";
  // //   assert( false );
  // // }

  // SHUFFLING PHASE
  // time = clock();
  execShuffle();
  // time = clock() - time;
  // cout << rank << ": Shuffle phase takes " << double( time ) / CLOCKS_PER_SEC << " seconds.\n";
  
  bool isReducer = false;
  vector<int> reducerNodes = conf->getReducerNodes();
  for(int i = 0; i < reducerNodes.size(); i++) {
    if(reducerNodes[i] == rank) {
      isReducer = true;
    }
  }
  if(!isReducer) {
    rTime = 0;
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    
    return;
  }

  // EXECUTE DECODING PHASE
  gettimeofday(&start,NULL);
  execDecoding();
  gettimeofday(&end,NULL);
  rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  // // // WAIT UNTIL PARALLEL DECODING IS DONE
  // // time = clock();
  // // pthread_join( decodeThread, NULL );
  // // time = clock() - time;
  // // cout << rank << ": Additional decoding phase takes " << double( time ) / CLOCKS_PER_SEC << " seconds.\n";

  // REDUCE PHASE
  gettimeofday(&start,NULL);
  execReduce();
  gettimeofday(&end,NULL);
  rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  outputLocalList();
  // printLocalList();
}

unsigned int CodedWorker::findAssociatePartition(const unsigned char *line)
{
  unsigned int i;
  for (i = 0; i < partitionList.size(); i++)
  {
    if (cmpKey(line, partitionList.at(i), conf->getKeySize()) == true)
    {
      return i;
    }
  }
  return i;
}

void CodedWorker::execMap()
{
  const vector<int> &inputFileList = conf->getNodeStorage(rank);
  set<int> inputSet(inputFileList.begin(), inputFileList.end());
  struct timeval start, end;
  double rTime;
  gettimeofday(&start,NULL);
  // Build trie
  unsigned char prefix[conf->getKeySize()];
  trie = buildTrie(&partitionList, 0, partitionList.size(), prefix, 0, 2);

  // Read input files and partition data
  for (auto init = inputSet.begin(); init != inputSet.end(); init++)
  {
    unsigned int inputId = *init;

    // Read input
    char filePath[MAX_FILE_PATH];
    sprintf(filePath, "%s_%d", conf->getInputPath(), inputId);
    ifstream inputFile(filePath, ios::in | ios::binary | ios::ate);
    if (!inputFile.is_open())
    {
      cout << rank << ": Cannot open input file " << filePath << endl;
      assert(false);
    }

    unsigned long fileSize = inputFile.tellg();
    unsigned long int lineSize = conf->getLineSize();
    unsigned long int numLine = fileSize / lineSize;
    inputFile.seekg(0, ios::beg);
    PartitionCollection &pc = inputPartitionCollection[inputId];

    // Crate lists of lines
    for (unsigned int i = 0; i < conf->getNumReducer(); i++)
    {
      pc[i] = new LineList;
      // inputPartitionCollection[ inputId ][ i ] = new LineList;
    }

    // Partition data in the input file
    for (unsigned long i = 0; i < numLine; i++)
    {
      unsigned char *buff = new unsigned char[lineSize];
      inputFile.read((char *)buff, lineSize);
      unsigned int wid = trie->findPartition(buff);
      pc[wid]->push_back(buff);
      // inputPartitionCollection[ inputId ][ wid ]->push_back( buff );
    }

    inputFile.close();
  }
  gettimeofday(&end,NULL);
  rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
  // Remove unnecessarily lists (partitions associated with the other nodes having the file)
  // 意思是 node i 与 node rank 文件集合中都有文件 inputid，node i 自然有该文件 reduce i 的结果，node rank 这里就不需要保存了
  int logicalId = conf->getLogicalNodeId(rank);
  const vector<NodeSet> &codedGroupsInc = cg->getNodeSubsetSContain(logicalId);
  for(auto it : codedGroupsInc) {
    int codedFileNum = cg->getCodedFileNum(it);
    for(int nodeId : it) {
      if(nodeId == logicalId) {
        continue;
      }
      set<int> subsetR(it);
      subsetR.erase(nodeId);
      const vector<int> sameFileList = cg->getSameFileList(subsetR);
      for(int i = 0; i < codedFileNum; i++) {
        int removedFileId = sameFileList[i];
        PartitionCollection &pc = inputPartitionCollection[removedFileId];
        for(int recNodeId : subsetR) { 
          if(recNodeId == logicalId) {
            continue;
          }
          LineList *list = pc[recNodeId - 1];
          if(!list) {
            continue;
          }
          for (auto lit = list->begin(); lit != list->end(); lit++) {
            delete[] * lit;
          }
          delete list;
          pc[recNodeId - 1] = NULL;
        }
      }
    }
  }

  // Packet partitioned p2p data to a chunk
  const vector<int> reducerNodes = conf->getReducerNodes();
  unsigned long int lineSize = conf->getLineSize();
  const vector<int> &storedFileList = conf->getNodeStorage(rank);
  for (unsigned int i = 0; i < conf->getNumReducer(); i++) {
    int reducerId = conf->getReducerId(i + 1);
    if(reducerId == rank) {
      continue;
    }
    unsigned long long numLine = 0;
    unordered_map<int, vector<DataChunk>> packetPreData; // fileId -> chunks of intermediate value
    const set<int> &fileList = cg->getP2pFileList(i + 1);
    for(int fileId : fileList) {
      LineList *ll = inputPartitionCollection[fileId][i];
      unsigned int numPart;
      vector<int> chunkNums;
      if(conf->EnableNodeCombination()) {
        if(logicalId == i + 1) {
          numPart = conf->getInnerNodeNumPart(fileId, logicalId);
          const vector<int> chunks = conf->getChunkNumInnerNode(fileId, rank);
          chunkNums.insert(chunkNums.end(), chunks.begin(), chunks.end());
        } else {
          numPart = conf->getLoad();
          const vector<int> chunks = conf->getChunkNum(fileId, rank);
          chunkNums.insert(chunkNums.end(), chunks.begin(), chunks.end());
        }
      } else {
        numPart = conf->getLoad();
        const vector<int> chunks = conf->getChunkNum(fileId, rank);
        chunkNums.insert(chunkNums.end(), chunks.begin(), chunks.end());
      }
      // unsigned int numPart = conf->getLoad();
      // const vector<int> chunkNums = conf->getChunkNum(fileId, rank);
      unsigned long long chunkSize = ll->size() / numPart;
      auto lit = ll->begin();
      // first chunk to second last chunk
      for (unsigned int ci = 0; ci < numPart - 1; ci++)
      {
        unsigned char *chunk = new unsigned char[chunkSize * lineSize];
        for (unsigned long long j = 0; j < chunkSize; j++)
        {
          memcpy(chunk + j * lineSize, *lit, lineSize);
          delete[] *lit;
          lit++;
        }
        DataChunk dc;
        dc.data = chunk;
        dc.size = chunkSize;
        packetPreData[fileId].push_back(dc);
      }
      // last chuck
      unsigned long long lastChunkSize = ll->size() - chunkSize * (numPart - 1);
      unsigned char *chunk = new unsigned char[lastChunkSize * lineSize];
      for (unsigned long long j = 0; j < lastChunkSize; j++)
      {
        memcpy(chunk + j * lineSize, *lit, lineSize);
        delete[] *lit;
        lit++;
      }
      DataChunk dc;
      dc.data = chunk;
      dc.size = lastChunkSize;
      packetPreData[fileId].push_back(dc);
      // calculate num of lines
      if(*(chunkNums.end() - 1) == numPart - 1) {
        numLine += (chunkNums.size() - 1) * chunkSize + lastChunkSize;
      } else {
        numLine += chunkNums.size() *chunkSize;
      }
      delete ll;
      inputPartitionCollection[fileId][i] = NULL;
    }

    partitionTxData[i].data = new unsigned char[numLine * lineSize];
    partitionTxData[i].numLine = numLine;
    unsigned long long offset = 0;
    for(int fileId : fileList) {
      unsigned int numPart;
      vector<int> chunkNums;
      if(conf->EnableNodeCombination()) {
        if(logicalId == i + 1) {
          numPart = conf->getInnerNodeNumPart(fileId, logicalId);
          const vector<int> chunks = conf->getChunkNumInnerNode(fileId, rank);
          chunkNums.insert(chunkNums.end(), chunks.begin(), chunks.end());
        } else {
          numPart = conf->getLoad();
          const vector<int> chunks = conf->getChunkNum(fileId, rank);
          chunkNums.insert(chunkNums.end(), chunks.begin(), chunks.end());
        }
      } else {
        numPart = conf->getLoad();
        const vector<int> chunks = conf->getChunkNum(fileId, rank);
        chunkNums.insert(chunkNums.end(), chunks.begin(), chunks.end());
      }
      // unsigned int numPart = conf->getLoad();
      // const vector<int> chunkNums = conf->getChunkNum(fileId, rank);
      for(int chunkNum : chunkNums) {
        memcpy(partitionTxData[i].data + offset, packetPreData[fileId][chunkNum].data, packetPreData[fileId][chunkNum].size * lineSize);
        offset += packetPreData[fileId][chunkNum].size * lineSize;
      }
      for(int j = 0; j < numPart; j++) {
        delete []packetPreData[fileId][j].data;
      }
    }
  }

  if(!conf->EnableNodeCombination()) {
    return ;
  }
  
  // packet inner node p2p data to a chunk
  if(conf->EnableInnerCode()) {
    // for entire inner node tx data(send to reducer directly)
    int curReducerId = conf->getReducerId(logicalId);
    if(curReducerId == rank) {
      return;
    }
    const set<pair<int, int>> &innerP2pEntireFileList = cg->getInnerP2pEntireFiles();
    unsigned long long totSize = sizeof(int); // add block num
    for(auto it : innerP2pEntireFileList) {
      int fileId = it.first, partitionId = it.second;
      LineList *ll = inputPartitionCollection[fileId][partitionId];
      // fileId, reducerId, dataSize, data
      totSize += 2 * sizeof(int) + sizeof(unsigned long long);
      totSize += ll->size() * lineSize;
    }
    InnerNodeEntireTxData.size = totSize;
    InnerNodeEntireTxData.data = new unsigned char[totSize];
    unsigned char *p = InnerNodeEntireTxData.data;
    int blockNum = innerP2pEntireFileList.size();
    memcpy(p, &(blockNum), sizeof(int));
    p += sizeof(int);
    for(auto it : innerP2pEntireFileList) {
      int fileId = it.first, reducerId = it.second + 1;
      LineList *ll = inputPartitionCollection[fileId][reducerId - 1];
      // fileId, reducerId, dataSize, data
      memcpy(p, &(fileId), sizeof(int));
      p += sizeof(int);
      memcpy(p, &reducerId, sizeof(int));
      p += sizeof(int);
      unsigned long long dataSize = ll->size() * lineSize;
      memcpy(p, &dataSize, sizeof(unsigned long long));
      p += sizeof(unsigned long long);
      for(auto lit = ll->begin(); lit != ll->end(); lit++) {
        memcpy(p, *lit, lineSize);
        p += lineSize;
        delete []*lit;
      }
      delete ll;
      inputPartitionCollection[fileId][reducerId - 1] = NULL;
    }
    // for fragment inner node tx data(send to rack leader)
    const InnerP2PFragInfo &fragInfo = cg->getInnerP2pFragFileInfo();
    if(fragInfo.leaderId == rank || fragInfo.leaderId == -1) {
      return ;
    }
    totSize = sizeof(int); // add block num
    for(auto it : fragInfo.fileList) {
      int fileId = it.first, partitionId = it.second;
      LineList *ll = inputPartitionCollection[fileId][partitionId];
      // fileId, reducerId, dataSize, data
      totSize += 2 * sizeof(int) + sizeof(unsigned long long);
      totSize += ll->size() * lineSize;
    }
    InnerNodeFragPreTxData.size = totSize;
    InnerNodeFragPreTxData.data = new unsigned char[totSize];
    p = InnerNodeFragPreTxData.data;
    blockNum = fragInfo.fileList.size();
    memcpy(p, &(blockNum), sizeof(int));
    p += sizeof(int);
    for(auto it : fragInfo.fileList) {
      int fileId = it.first, reducerId = it.second + 1;
      LineList *ll = inputPartitionCollection[fileId][reducerId - 1];
      // fileId, reducerId, dataSize, data
      memcpy(p, &(fileId), sizeof(int));
      p += sizeof(int);
      memcpy(p, &reducerId, sizeof(int));
      p += sizeof(int);
      unsigned long long dataSize = ll->size() * lineSize;
      memcpy(p, &dataSize, sizeof(unsigned long long));
      p += sizeof(unsigned long long);
      for(auto lit = ll->begin(); lit != ll->end(); lit++) {
        memcpy(p, *lit, lineSize);
        p += lineSize;
        delete []*lit;
      }
      delete ll;
      inputPartitionCollection[fileId][reducerId - 1] = NULL;
    }
  } else {
    // packet inner node p2p data to a chunk
    int reducerId = conf->getReducerId(logicalId);
    if(reducerId == rank) {
      return;
    }
    int blockNum = 0;
    unsigned long long totSize = sizeof(int); // add block num
    const set<int> innerFileList = cg->getInnerP2pFileList();
    for(int fileId : innerFileList) {
      const vector<int> reducers = cg->getNeededReducer(fileId);
      for(int reducerId : reducers) {
        // fileId, reducerId, dataSize, data
        totSize += 2 * sizeof(int) + sizeof(unsigned long long);
        LineList *ll = inputPartitionCollection[fileId][reducerId - 1];
        totSize += ll->size() * lineSize;
        blockNum++;
      }
    }
    InnerNodeTxData.size = totSize;
    InnerNodeTxData.data = new unsigned char[totSize];
    unsigned char *p = InnerNodeTxData.data;
    memcpy(p, &(blockNum), sizeof(int));
    p += sizeof(int);
    for(int fileId : innerFileList) {
      const vector<int> reducers = cg->getNeededReducer(fileId);
      for(int reducerId : reducers) {
        // fileId, reducerId, dataSize, data
        memcpy(p, &(fileId), sizeof(int));
        p += sizeof(int);
        memcpy(p, &reducerId, sizeof(int));
        p += sizeof(int);
        LineList *ll = inputPartitionCollection[fileId][reducerId - 1];
        unsigned long long dataSize = ll->size() * lineSize;
        memcpy(p, &dataSize, sizeof(unsigned long long));
        p += sizeof(unsigned long long);
        for(auto lit = ll->begin(); lit != ll->end(); lit++) {
          memcpy(p, *lit, lineSize);
          p += lineSize;
          delete []*lit;
        }
        delete ll;
      }
    }
  }
}

void CodedWorker::execInnerNodeTrans() {
  // pass intermediate values within the logical node
  unsigned lineSize = conf->getLineSize();
  struct timeval start, end;
  for(int i = 1; i <= conf->getNumMapper(); i++) {
    int logicalId = conf->getLogicalNodeId(i);
    int reducerId = conf->getReducerId(logicalId);
    if(reducerId == i) {
      continue;
    }
    if(rank == reducerId) {
      // receive
      MPI_Barrier(workerComm);
      unsigned long long totSize;
      MPI_Recv(&totSize, 1, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      unsigned char *data = new unsigned char[totSize];
      MPI_Recv(data, totSize, MPI_UNSIGNED_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Barrier(workerComm);
      // unpacket 
      unsigned char *p = data;
      int chunkNum;
      memcpy(&chunkNum, p, sizeof(int));
      p += sizeof(int);
      for(int j = 0; j < chunkNum; j++) {
        // fileId, reducerId, dataSize, data
        int fileId, reducerId;
        unsigned long long dataSize;
        memcpy(&fileId, p, sizeof(int));
        p += sizeof(int);
        memcpy(&reducerId, p, sizeof(int));
        p += sizeof(int);
        memcpy(&dataSize, p, sizeof(unsigned long long));
        p += sizeof(unsigned long long);
        unsigned long long numline = dataSize / lineSize;
        LineList *ll = new LineList;
        for(int k = 0; k < numline; k++) {
          unsigned char *buff = new unsigned char[lineSize];
          memcpy(buff, p, lineSize);
          p += lineSize;
          ll->push_back(buff);
        }
        inputPartitionCollection[fileId][reducerId - 1] = ll;
      }
      delete []data;
    } else if(rank == i) {
      MPI_Barrier(workerComm);
      gettimeofday(&start,NULL);
      // send to corresponding logical reducer
      MPI_Send(&(InnerNodeTxData.size), 1, MPI_UNSIGNED_LONG_LONG, reducerId, 0, MPI_COMM_WORLD);
      MPI_Send(InnerNodeTxData.data, InnerNodeTxData.size, MPI_UNSIGNED_CHAR, reducerId, 0, MPI_COMM_WORLD);
      MPI_Barrier(workerComm);
      gettimeofday(&end,NULL);
      shuffleTime += (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
      shuffleTx += InnerNodeTxData.size * 1e-6;
    } else {
      MPI_Barrier(workerComm);
      MPI_Barrier(workerComm);
    }
  }
}

void CodedWorker::execInnerNodeTransCoded() {
  // pass intermediate values within the logical node
  unsigned lineSize = conf->getLineSize();
  struct timeval start, end;
  // send entire intermediate value to logical node directly
  for(int i = 1; i <= conf->getNumMapper(); i++) {
    int logicalId = conf->getLogicalNodeId(i);
    int reducerId = conf->getReducerId(logicalId);
    if(reducerId == i) {
      continue;
    }
    if(rank == reducerId) {
      // receive
      MPI_Barrier(workerComm);
      unsigned long long totSize;
      MPI_Recv(&totSize, 1, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      unsigned char *data = new unsigned char[totSize];
      MPI_Recv(data, totSize, MPI_UNSIGNED_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Barrier(workerComm);
      // unpacket 
      unsigned char *p = data;
      int chunkNum;
      memcpy(&chunkNum, p, sizeof(int));
      p += sizeof(int);
      for(int j = 0; j < chunkNum; j++) {
        // fileId, reducerId, dataSize, data
        int fileId, reducerId;
        unsigned long long dataSize;
        memcpy(&fileId, p, sizeof(int));
        p += sizeof(int);
        memcpy(&reducerId, p, sizeof(int));
        p += sizeof(int);
        memcpy(&dataSize, p, sizeof(unsigned long long));
        p += sizeof(unsigned long long);
        unsigned long long numline = dataSize / lineSize;
        LineList *ll = new LineList;
        for(int k = 0; k < numline; k++) {
          unsigned char *buff = new unsigned char[lineSize];
          memcpy(buff, p, lineSize);
          p += lineSize;
          ll->push_back(buff);
        }
        inputPartitionCollection[fileId][reducerId - 1] = ll;
      }
      delete []data;
    } else if(rank == i) {
      MPI_Barrier(workerComm);
      gettimeofday(&start,NULL);
      // send to corresponding logical reducer
      MPI_Send(&(InnerNodeEntireTxData.size), 1, MPI_UNSIGNED_LONG_LONG, reducerId, 0, MPI_COMM_WORLD);
      MPI_Send(InnerNodeEntireTxData.data, InnerNodeEntireTxData.size, MPI_UNSIGNED_CHAR, reducerId, 0, MPI_COMM_WORLD);
      MPI_Barrier(workerComm);
      gettimeofday(&end,NULL);
      shuffleTime += (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
      shuffleTx += InnerNodeEntireTxData.size * 1e-6;
      delete []InnerNodeEntireTxData.data;
    } else {
      MPI_Barrier(workerComm);
      MPI_Barrier(workerComm);
    }
  }
  // send pre fragment intermediate value to local rack leader
  const vector<int> &networkTopology = conf->getNetworkTopology();
  const InnerP2PFragInfo &fragInfo = cg->getInnerP2pFragFileInfo();
  for(int i = 1; i <= conf->getNumMapper(); i++) {
    int logicalId = conf->getLogicalNodeId(i);
    int reducerId = conf->getReducerId(logicalId);
    if(reducerId == i) {
      continue;
    }
    if(rank == fragInfo.leaderId && cg->isLeaderOfRank(i)) {
      // receive
      MPI_Barrier(workerComm);
      unsigned long long totSize;
      MPI_Recv(&totSize, 1, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      unsigned char *data = new unsigned char[totSize];
      MPI_Recv(data, totSize, MPI_UNSIGNED_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Barrier(workerComm);
      // unpacket 
      unsigned char *p = data;
      int chunkNum;
      memcpy(&chunkNum, p, sizeof(int));
      p += sizeof(int);
      for(int j = 0; j < chunkNum; j++) {
        // fileId, reducerId, dataSize, data
        int fileId, reducerId;
        unsigned long long dataSize;
        memcpy(&fileId, p, sizeof(int));
        p += sizeof(int);
        memcpy(&reducerId, p, sizeof(int));
        p += sizeof(int);
        memcpy(&dataSize, p, sizeof(unsigned long long));
        p += sizeof(unsigned long long);
        unsigned long long numline = dataSize / lineSize;
        LineList *ll = new LineList;
        for(int k = 0; k < numline; k++) {
          unsigned char *buff = new unsigned char[lineSize];
          memcpy(buff, p, lineSize);
          p += lineSize;
          ll->push_back(buff);
        }
        inputPartitionCollection[fileId][reducerId - 1] = ll;
      }
      delete []data;
    } else if(rank == i && fragInfo.leaderId != -1 && fragInfo.leaderId != rank) {
      // send to rack leader
      MPI_Barrier(workerComm);
      gettimeofday(&start,NULL);
      // send to corresponding logical reducer
      MPI_Send(&(InnerNodeFragPreTxData.size), 1, MPI_UNSIGNED_LONG_LONG, fragInfo.leaderId, 0, MPI_COMM_WORLD);
      MPI_Send(InnerNodeFragPreTxData.data, InnerNodeFragPreTxData.size, MPI_UNSIGNED_CHAR, fragInfo.leaderId, 0, MPI_COMM_WORLD);
      MPI_Barrier(workerComm);
      gettimeofday(&end,NULL);
      shuffleTime += (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
      shuffleTx += InnerNodeFragPreTxData.size * 1e-6;
      delete[] InnerNodeFragPreTxData.data;
    } else {
      MPI_Barrier(workerComm);
      MPI_Barrier(workerComm);
    }
  }
  // encode data and send fragment intermediate value to reducer
  // encode data
  DataChunk codedFragments;
  if(rank == fragInfo.leaderId) {
    vector<EnFragData> codedFrags;
    unsigned long long totSize = sizeof(int); // add block num
    for(auto singleCodeInfo : fragInfo.codeInfo) {
      unsigned long long maxSize = 0;
      int maxSizeFileId = -1;
      vector<DataChunk> codePreChunks;
      EnFragData ed;
      // get corresponding chunks
      for(auto it : singleCodeInfo) {
        int fileId = get<0>(it), partitionId = get<1>(it), chunkRank = get<2>(it);
        LineList *ll = inputPartitionCollection[fileId][partitionId];
        auto lit = ll->begin();
        unsigned int numPart = conf->getLoad();
        unsigned long long chunkSize = ll->size() / numPart;
        ed.metaList.push_back(make_tuple(fileId, partitionId, chunkRank, ll->size()));
        DataChunk dc;
        if(chunkRank == numPart - 1) {
          // last chunk
          unsigned long long lastChunkSize = ll->size() - chunkSize * (numPart - 1);
          unsigned char *chunk = new unsigned char[lastChunkSize * lineSize];
          auto lit = ll->begin() + chunkSize * (numPart - 1);
          for(unsigned long long j = 0; j < lastChunkSize; j++) {
            memcpy(chunk + j * lineSize, *lit, lineSize);
            delete[] *lit;
            lit++;
          }
          dc.data = chunk;
          dc.size = lastChunkSize;
        } else {
          unsigned char *chunk = new unsigned char[chunkSize * lineSize];
          auto lit = ll->begin() + chunkSize * chunkRank;
          for(unsigned long long j = 0; j < chunkSize; j++) {
            memcpy(chunk + j * lineSize, *lit, lineSize);
            delete[] *lit;
            lit++;
          }
          dc.data = chunk;
          dc.size = chunkSize;
        }
        codePreChunks.push_back(dc);
        if(dc.size > maxSize) {
          maxSize = dc.size;
          maxSizeFileId = fileId;
        }
      }
      // start encoding
      ed.data = new unsigned char[maxSize * lineSize]();
      ed.size = maxSize;
      ed.maxSizeFileId = maxSizeFileId;
      unsigned char *data = ed.data;
      for(auto &dataChunk : codePreChunks) {
        unsigned char *preData = dataChunk.data;
        unsigned long long preDataSize = dataChunk.size;
        unsigned long long maxiter = preDataSize * lineSize / sizeof(uint32_t);
        for(unsigned long long i = 0; i < maxiter; i++) {
          ((uint32_t *)data)[i] ^= ((uint32_t *)preData)[i];
        }
        delete []preData;
      }
      // serialize metadata
      unsigned int ms = sizeof(unsigned int); // metaList.size()
      ms += ed.metaList.size() * (sizeof(int) * 3 + sizeof(unsigned long long));
      ed.metaSize = ms;
      ed.serialMeta = new unsigned char[ms];
      unsigned char *p = ed.serialMeta;
      unsigned int metaSize = ed.metaList.size();
      memcpy(p, &metaSize, sizeof(unsigned int));
      p += sizeof(unsigned int);
      for(auto it : ed.metaList) {
        int fileId = get<0>(it), partitionId = get<1>(it), chunkRank = get<2>(it);
        unsigned long long numLine = get<3>(it);
        memcpy(p, &fileId, sizeof(int));
        p += sizeof(int);
        memcpy(p, &partitionId, sizeof(int));
        p += sizeof(int);
        memcpy(p, &chunkRank, sizeof(int));
        p += sizeof(int);
        memcpy(p, &numLine, sizeof(unsigned long long));
        p += sizeof(unsigned long long);
      }
      totSize += sizeof(int) + ed.metaSize + ed.size * lineSize; // maxSizeFileId, metadata, data
      codedFrags.push_back(ed);
    }
    // packet encode fragments
    codedFragments.data = new unsigned char[totSize];
    codedFragments.size = totSize;
    unsigned char *p = codedFragments.data;
    int edNum = codedFrags.size();
    memcpy(p, &edNum, sizeof(int));
    p += sizeof(int);
    for(auto &it : codedFrags) {
      memcpy(p, &it.maxSizeFileId, sizeof(int));
      p += sizeof(int);
      memcpy(p, it.serialMeta, it.metaSize);
      p += it.metaSize;
      memcpy(p, it.data, it.size * lineSize);
      p += it.size * lineSize;
      delete it.serialMeta;
      delete it.data;
    }
  }
  // send to reducer
  for(int i = 1; i <= conf->getNumMapper(); i++) {
    int logicalId = conf->getLogicalNodeId(i);
    int reducerId = conf->getReducerId(logicalId);
    if(reducerId == i) {
      continue;
    }
    if(rank == reducerId && cg->isreducerOfLeader(i)) {
      // receive
      MPI_Barrier(workerComm);
      unsigned long long totSize;
      MPI_Recv(&totSize, 1, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      unsigned char *data = new unsigned char[totSize];
      MPI_Recv(data, totSize, MPI_UNSIGNED_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Barrier(workerComm);
      // unpacket 
      unsigned char *p = data;
      int chunkNum;
      memcpy(&chunkNum, p, sizeof(int));
      p += sizeof(int);
      for(int j = 0; j < chunkNum; j++) {
        // unpack metadata
        int destFileId;
        memcpy(&destFileId, p, sizeof(int));
        p += sizeof(int);
        unsigned int metaSize;
        memcpy(&metaSize, p, sizeof(unsigned int));
        p += sizeof(unsigned int);
        vector<tuple<int, int, int, unsigned long long>> metaInfo;
        for(int i = 0; i < metaSize; i++) {
          int fileId, partitionId, chunkRank;
          unsigned long long numLine;
          memcpy(&fileId, p, sizeof(int));
          p += sizeof(int);
          memcpy(&partitionId, p, sizeof(int));
          p += sizeof(int);
          memcpy(&chunkRank, p, sizeof(int));
          p += sizeof(int);
          memcpy(&numLine, p, sizeof(unsigned long long));
          p += sizeof(unsigned long long);
          metaInfo.push_back(make_tuple(fileId, partitionId, chunkRank, numLine));
        }
        for(auto &it : metaInfo) {
          int fileId = get<0>(it), partitionId = get<1>(it), chunkRank = get<2>(it);
          unsigned long long numLine = get<3>(it);
          if(!inputPartitionCollection[fileId][partitionId]) {
            inputPartitionCollection[fileId][partitionId] = new LineList;
            for(int i = 0; i < numLine; i++) {
              unsigned char *buff = new unsigned char[lineSize]();
              // memset(buff, 0, sizeof(unsigned char) * lineSize);
              inputPartitionCollection[fileId][partitionId]->push_back(buff);
            }
          }
          LineList *ll = inputPartitionCollection[fileId][partitionId];
          if(fileId == destFileId) {
            unsigned int numPart = conf->getLoad();
            unsigned long long chunkSize = numLine / numPart;
            auto lit = ll->begin() + chunkSize * chunkRank;
            if(chunkRank == numPart - 1) {
              // last chunk
              unsigned long long lastChunkSize = numLine - chunkSize * (numPart - 1);
              for(unsigned long long j = 0; j < lastChunkSize; j++) {
                memcpy(*lit, p, lineSize);
                p += lineSize;
                lit++;
              }
            } else {
              for(unsigned long long j = 0; j < chunkSize; j++) {
                memcpy(*lit, p, lineSize);
                p += lineSize;
                lit++;
              }
            }
          }
        }
      }
      delete []data;
    } else if(rank == i && rank == fragInfo.leaderId) {
      // send to reducer
      MPI_Barrier(workerComm);
      gettimeofday(&start,NULL);
      MPI_Send(&(codedFragments.size), 1, MPI_UNSIGNED_LONG_LONG, reducerId, 0, MPI_COMM_WORLD);
      MPI_Send(codedFragments.data, codedFragments.size, MPI_UNSIGNED_CHAR, reducerId, 0, MPI_COMM_WORLD);
      MPI_Barrier(workerComm);
      gettimeofday(&end,NULL);
      shuffleTime += (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
      shuffleTx += InnerNodeEntireTxData.size * 1e-6;
      delete []codedFragments.data;
    } else {
      MPI_Barrier(workerComm);
      MPI_Barrier(workerComm);
    }
  }
}

void CodedWorker::execEncoding() {
  unsigned lineSize = conf->getLineSize();
  int logicalId = conf->getLogicalNodeId(rank);
  int reducerId = conf->getReducerId(logicalId);
  if(rank != reducerId) {
    return ;
  }
  const vector<NodeSet> subsetS = cg->getNodeSubsetSContain(logicalId);
  for (auto nsit = subsetS.begin(); nsit != subsetS.end(); nsit++) { 
    SubsetSId nsid = cg->getSubsetSId(*nsit);
    NodeSet ns(*nsit);
    int codedFileNum = cg->getCodedFileNum(ns);
    for(int i = 0; i < codedFileNum; i++) {
      unsigned long long maxSize = 0;
      // Construct chucks of input from data with index ns\{q}
      for (auto qit = nsit->begin(); qit != nsit->end(); qit++)
      {
        if ((unsigned int)*qit == logicalId)
        {
          continue;
        }
        int destId = *qit;
        NodeSet inputIdx(*nsit);
        inputIdx.erase(destId);

        vector<int> fileList = cg->getSameFileList(inputIdx);
        int fid = fileList[i];
        VpairList vplist;
        vplist.push_back(Vpair(destId, fid));

        unsigned int partitionId = destId - 1;

        LineList *ll = inputPartitionCollection[fid][partitionId];

        auto lit = ll->begin();
        unsigned int numPart = conf->getLoad();
        unsigned long long chunkSize = ll->size() / numPart; // a number of lines ( not bytes )
        // first chunk to second last chunk
        for (unsigned int ci = 0; ci < numPart - 1; ci++)
        {
          unsigned char *chunk = new unsigned char[chunkSize * lineSize];
          for (unsigned long long j = 0; j < chunkSize; j++)
          {
            memcpy(chunk + j * lineSize, *lit, lineSize);
            lit++;
          }
          DataChunk dc;
          dc.data = chunk;
          dc.size = chunkSize;
          encodePreData[nsid][vplist].push_back(dc);
        }
        // last chuck
        unsigned long long lastChunkSize = ll->size() - chunkSize * (numPart - 1);
        unsigned char *chunk = new unsigned char[lastChunkSize * lineSize];
        for (unsigned long long j = 0; j < lastChunkSize; j++)
        {
          memcpy(chunk + j * lineSize, *lit, lineSize);
          lit++;
        }
        DataChunk dc;
        dc.data = chunk;
        dc.size = lastChunkSize;
        encodePreData[nsid][vplist].push_back(dc);

        // Determine associated chunk of a worker ( order in ns )
        unsigned int rankChunk = 0; // in [ 0, ... , r - 1 ]
        for (auto it = inputIdx.begin(); it != inputIdx.end(); it++)
        {
          if ((unsigned int)*it == logicalId)
          {
            break;
          }
          rankChunk++;
        }
        maxSize = max(maxSize, encodePreData[nsid][vplist][rankChunk].size);

        // Remode unused intermediate data from Map
        for (auto lit = ll->begin(); lit != ll->end(); lit++)
        {
          delete[] * lit;
        }
        delete ll;
      }

      // Initialize encode data
      EnData *ed = new EnData();
      ed->data = new unsigned char[maxSize * lineSize](); // Initial it with 0
      ed->size = maxSize;
      unsigned char *data = ed->data;

      // Encode Data
      for (auto qit = nsit->begin(); qit != nsit->end(); qit++)
      {
        if ((unsigned int)*qit == logicalId)
        {
          continue;
        }
        int destId = *qit;
        NodeSet inputIdx(*nsit);
        inputIdx.erase(destId);

        vector<int> fileList = cg->getSameFileList(inputIdx);
        int fid = fileList[i];
        VpairList vplist;
        vplist.push_back(Vpair(destId, fid));

        // Determine associated chunk of a worker ( order in ns )
        unsigned int rankChunk = 0; // in [ 0, ... , r - 1 ]
        for (auto it = inputIdx.begin(); it != inputIdx.end(); it++)
        {
          if ((unsigned int)*it == logicalId)
          {
            break;
          }
          rankChunk++;
        }

        // Start encoding
        unsigned char *predata = encodePreData[nsid][vplist][rankChunk].data;
        unsigned long long size = encodePreData[nsid][vplist][rankChunk].size;
        unsigned long long maxiter = size * lineSize / sizeof(uint32_t);
        for (unsigned long long i = 0; i < maxiter; i++)
        {
          ((uint32_t *)data)[i] ^= ((uint32_t *)predata)[i];
        }

        // Fill metadata
        MetaData md;
        md.vpList = vplist;
        md.vpSize[vplist[0]] = size; // Assume Eta = 1;
        md.partNumber = rankChunk + 1;
        md.size = size;
        ed->metaList.push_back(md);
      }

      // Serialize Metadata
      unsigned int ms = 0;
      ms += sizeof(unsigned int); // metaList.size()
      for (unsigned int m = 0; m < ed->metaList.size(); m++)
      {
        ms += sizeof(unsigned int);                                                              // vpList.size()
        ms += sizeof(int) * 2 * ed->metaList[m].vpList.size();                                // vpList
        ms += sizeof(unsigned int);                                                              // vpSize.size()
        ms += (sizeof(int) * 2 + sizeof(unsigned long long)) * ed->metaList[m].vpSize.size(); // vpSize
        ms += sizeof(unsigned int);                                                              // partNumber
        ms += sizeof(unsigned long long);                                                        // size
      }
      ed->metaSize = ms;

      unsigned char *mbuff = new unsigned char[ms];
      unsigned char *p = mbuff;
      unsigned int metaSize = ed->metaList.size();
      memcpy(p, &metaSize, sizeof(unsigned int));
      p += sizeof(unsigned int);
      // meta data List
      for (unsigned int m = 0; m < metaSize; m++)
      {
        MetaData mdata = ed->metaList[m];
        unsigned int numVp = mdata.vpList.size();
        memcpy(p, &numVp, sizeof(unsigned int));
        p += sizeof(unsigned int);
        // vpair List
        for (unsigned int v = 0; v < numVp; v++)
        {
          memcpy(p, &(mdata.vpList[v].first), sizeof(int));
          p += sizeof(int);
          memcpy(p, &(mdata.vpList[v].second), sizeof(int));
          p += sizeof(int);
        }
        // vpair size Map
        unsigned int numVps = mdata.vpSize.size();
        memcpy(p, &numVps, sizeof(unsigned int));
        p += sizeof(unsigned int);
        for (auto vpsit = mdata.vpSize.begin(); vpsit != mdata.vpSize.end(); vpsit++)
        {
          Vpair vp = vpsit->first;
          unsigned long long size = vpsit->second;
          memcpy(p, &(vp.first), sizeof(int));
          p += sizeof(int);
          memcpy(p, &(vp.second), sizeof(int));
          p += sizeof(int);
          memcpy(p, &size, sizeof(unsigned long long));
          p += sizeof(unsigned long long);
        }
        memcpy(p, &(mdata.partNumber), sizeof(unsigned int));
        p += sizeof(unsigned int);
        memcpy(p, &(mdata.size), sizeof(unsigned long long));
        p += sizeof(unsigned long long);
      }
      ed->serialMeta = mbuff;
      encodeDataSend[nsid].push_back(*ed);
    }
  }
}

void CodedWorker::execShuffle()
{
  double multTime = 0, p2pTime = 0;
  double multTx = 0, p2pTx = 0;
  struct timeval start, end;
  // transmit intermediate value point to point
  unsigned int lineSize = conf->getLineSize();
  unsigned long long tolSize = 0;
  int reducerIdx = -1;
  vector<int> reducerNodes = conf->getReducerNodes();
  for(int i = 0; i < reducerNodes.size(); i++) {
    if(reducerNodes[i] == rank) {
      reducerIdx = i;
    }
  }
  for (unsigned int i = 1; i <= conf->getNumMapper(); i++) {
    if (i == rank) {
      MPI_Barrier(workerComm);
      gettimeofday(&start,NULL);
      // Sending from node i
      for (unsigned int j = 1; j <= conf->getNumReducer(); j++) {
        int reducerRank = conf->getReducerId(j);
        if (reducerRank == rank) {
          continue;
        }
        TxData &txData = partitionTxData[j - 1];
        MPI_Send(&(txData.numLine), 1, MPI_UNSIGNED_LONG_LONG, reducerRank, 0, MPI_COMM_WORLD);
        MPI_Send(txData.data, txData.numLine * lineSize, MPI_UNSIGNED_CHAR, reducerRank, 0, MPI_COMM_WORLD);
        tolSize += txData.numLine * lineSize + sizeof(unsigned long long);
        p2pTx += txData.numLine * lineSize + sizeof(unsigned long long);
        delete[] txData.data;
      }
      MPI_Barrier(workerComm);
      gettimeofday(&end,NULL);
      p2pTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
      // cout << rank << ": Avg sending rate is " << ( tolSize * 8 ) / ( rtxTime * 1e6 ) << " Mbps, Data size is " << tolSize / 1e6 << " MByte\n";
    }
    else if(reducerIdx != -1) {
      MPI_Barrier(workerComm);
      // Receiving from node i
      TxData &rxData = partitionRxData[i - 1];
      MPI_Recv(&(rxData.numLine), 1, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      rxData.data = new unsigned char[rxData.numLine * lineSize];
      MPI_Recv(rxData.data, rxData.numLine * lineSize, MPI_UNSIGNED_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Barrier(workerComm);
    } else {
      MPI_Barrier(workerComm);
      MPI_Barrier(workerComm);
    }
  }

  if(reducerIdx == -1) {
    shuffleTx += tolSize * 1e-6;
    shuffleTime += p2pTime;
    for (unsigned int activeId = 1; activeId <= conf->getNumReducer(); activeId++) {
      MPI_Barrier(workerComm);
      vector<NodeSet> vset = cg->getNodeSubsetSContain(activeId);
      for (auto nsit = vset.begin(); nsit != vset.end(); nsit++) {
        NodeSet ns = *nsit;
        SubsetSId nsid = cg->getSubsetSId(ns);

        // Ignore subset that does not contain the activeId
        if (ns.find(activeId) == ns.end())
        {
          continue;
        }

        int fileNum = cg->getCodedFileNum(ns);
        for(int i = 0; i < fileNum; i++) {
          MPI_Barrier(workerComm);
          MPI_Barrier(workerComm);
        }
      }
      MPI_Barrier(workerComm);
    }
    MPI_Send(&shuffleTime, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    MPI_Send(&shuffleTx, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    MPI_Send(&p2pTime, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    p2pTx *= 1e-6;
    MPI_Send(&p2pTx, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    MPI_Send(&multTime, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    MPI_Send(&multTx, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    return ;
  }

  // multicast the intermediate values that can be coded
  // NODE-BY-NODE
  // map< NodeSet, SubsetSId > ssmap = cg->getSubsetSIdMap();
  map<NodeSet, int> &multiGruopFileNum = cg->getmultiGroupsFileNum();
  for(auto &it : multiGruopFileNum) {
    int fileNum = it.second;
    SubsetSId nsid = cg->getSubsetSId(it.first);
    encodeDataRecv[nsid].resize(fileNum);
  }
  struct timeval totStart, totEnd;
  double sendTime = 0;
  for (unsigned int activeId = 1; activeId <= conf->getNumReducer(); activeId++)
  {
    MPI_Barrier(workerComm);
    if (reducerIdx + 1 == activeId)
    {
      gettimeofday(&totStart,NULL);
    }
    vector<NodeSet> vset = cg->getNodeSubsetSContain(activeId);
    // for( auto nsit = ssmap.begin(); nsit != ssmap.end(); nsit++ ) {
    for (auto nsit = vset.begin(); nsit != vset.end(); nsit++)
    {
      // NodeSet ns = nsit->first;
      // SubsetSId nsid = nsit->second;
      NodeSet ns = *nsit;
      SubsetSId nsid = cg->getSubsetSId(ns);

      // Ignore subset that does not contain the activeId
      if (ns.find(activeId) == ns.end())
      {
        continue;
      }

      int fileNum = cg->getCodedFileNum(ns);
      for(int i = 0; i < fileNum; i++) {
        if (reducerIdx + 1 == activeId)
        {
          // sendEncodeData(encodeDataSend[nsid], mcComm);
          gettimeofday(&start,NULL);
          MPI_Barrier(workerComm);
          sendEncodeDataMulticast(nsid, encodeDataSend[nsid][i]);
          gettimeofday(&end,NULL);
          MPI_Barrier(workerComm);
          multTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	    start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
          shuffleTime += multTime;
          sendTime += multTime;
          EnData endata = encodeDataSend[nsid][i];
          tolSize += (endata.size * conf->getLineSize()) + endata.metaSize + (2 * sizeof(unsigned long long));
          multTx += (endata.size * conf->getLineSize()) + endata.metaSize + (2 * sizeof(unsigned long long));
        }
        else if (ns.find(reducerIdx + 1) != ns.end())
        {
          MPI_Barrier(workerComm);
          recvEncodeDataMulticast(nsid, i);
          MPI_Barrier(workerComm);
        } else {
          MPI_Barrier(workerComm);
          MPI_Barrier(workerComm);
        }
      }
    }

    MPI_Barrier(workerComm);
    if (reducerIdx + 1 == activeId)
    {
      gettimeofday(&totEnd,NULL);
      double totSendTime = (totEnd.tv_sec*1000000.0 + totEnd.tv_usec -
		 	    totStart.tv_sec*1000000.0 - totStart.tv_usec) / 1000000.0;
      // cout << "node " << rank << " total send time: " << totSendTime << ", sub send time: " << sendTime << endl;
      shuffleTime += p2pTime;
      shuffleTx += tolSize * 1e-6 ;
      MPI_Send(&shuffleTime, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      MPI_Send(&shuffleTx, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      MPI_Send(&p2pTime, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      p2pTx *= 1e-6;
      MPI_Send(&p2pTx, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      MPI_Send(&sendTime, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      multTx *= 1e-6;
      MPI_Send(&multTx, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      // cout << rank  << ": Avg sending rate is " << ( tolSize * 8 ) / ( rtxTime * 1e6 ) << " Mbps, Data size is " << tolSize / 1e6 << " MByte\n";
    }
  }
}

void CodedWorker::execDecoding() {
  for (auto nsit = encodeDataRecv.begin(); nsit != encodeDataRecv.end(); nsit++) {
    SubsetSId nsid = nsit->first;
    for(auto &endataList : nsit->second) {
      for (auto eit = endataList.begin(); eit != endataList.end(); eit++) {
        EnData &endata = *eit;
        unsigned char *cdData = endata.data;
        unsigned long long cdSize = endata.size;
        vector<MetaData> &metaList = endata.metaList;

        unsigned int numDecode = 0;
        // Decode per VpairList
        MetaData dcMeta;
        for (auto mit = metaList.begin(); mit != metaList.end(); mit++)
        {
          MetaData &meta = *mit;
          if (encodePreData[nsid].find(meta.vpList) == encodePreData[nsid].end())
          {
            dcMeta = meta;
            // No original data for decoding;
            continue;
          }
          unsigned char *oData = encodePreData[nsid][meta.vpList][meta.partNumber - 1].data;
          unsigned long long oSize = encodePreData[nsid][meta.vpList][meta.partNumber - 1].size;
          unsigned long long maxByte = min(oSize, cdSize) * conf->getLineSize();
          unsigned long long maxIter = maxByte / sizeof(uint32_t);
          for (unsigned long long i = 0; i < maxIter; i++)
          {
            ((uint32_t *)cdData)[i] ^= ((uint32_t *)oData)[i];
          }
          numDecode++;
        }

        // sanity check
        if (numDecode != metaList.size() - 1)
        {
          cout << rank << ": Decode error " << numDecode << '/' << metaList.size() - 1 << endl;
          assert(numDecode != metaList.size() - 1);
        }

        if (decodePreData[nsid][dcMeta.vpList].empty())
        {
          for (unsigned int i = 0; i < conf->getLoad(); i++)
          {
            decodePreData[nsid][dcMeta.vpList].push_back(DataChunk());
          }
        }

        decodePreData[nsid][dcMeta.vpList][dcMeta.partNumber - 1].data = cdData;
        decodePreData[nsid][dcMeta.vpList][dcMeta.partNumber - 1].size = dcMeta.size;
      }
    }
  }

  vector<int> reducerNodes = conf->getReducerNodes();
  unsigned int partitionId = -1;
  for(int i = 0; i < reducerNodes.size(); i++) {
    if(reducerNodes[i] == rank) {
      partitionId = i;
    }
  }
  unsigned int lineSize = conf->getLineSize();

  // Get partitioned data from input files, already stored in memory.
  const vector<int> &inputFileList = conf->getNodeStorage(rank);
  set<int> inputSet(inputFileList.begin(), inputFileList.end());
  for (auto init = inputSet.begin(); init != inputSet.end(); init++)
  {
    unsigned int inputId = *init;
    LineList *ll = inputPartitionCollection[inputId][partitionId];
    // copy line by line
    for (auto lit = ll->begin(); lit != ll->end(); lit++)
    {
      unsigned char *buff = new unsigned char[lineSize];
      memcpy(buff, *lit, lineSize);
      localList.push_back(buff);
      delete *lit;
    }
    delete ll;
  }

  // Get partitioned data from other workers
  // from coded multicast
  for (auto nvit = decodePreData.begin(); nvit != decodePreData.end(); nvit++)
  {
    DataPartMap &dpMap = nvit->second;
    for (auto vvit = dpMap.begin(); vvit != dpMap.end(); vvit++)
    {
      VpairList vplist = vvit->first;
      vector<DataChunk> vdc = vvit->second;
      // Add data from each part to locallist
      for (auto dcit = vdc.begin(); dcit != vdc.end(); dcit++)
      {
        unsigned char *data = dcit->data;
        for (unsigned long long i = 0; i < dcit->size; i++)
        {
          unsigned char *buff = new unsigned char[lineSize];
          memcpy(buff, data + i * lineSize, lineSize);
          localList.push_back(buff);
        }
        delete[] dcit->data;
      }
    }
  }
  // from p2p transmission
  for (unsigned int i = 1; i <= conf->getNumMapper(); i++) {
    if (i == rank) {
      continue;
    }
    TxData &rxData = partitionRxData[i - 1];
    for (unsigned long long lc = 0; lc < rxData.numLine; lc++) {
      unsigned char *buff = new unsigned char[lineSize];
      memcpy(buff, rxData.data + lc * lineSize, lineSize);
      localList.push_back(buff);
    }
    delete[] rxData.data;
  }
}

// PARALLEL DECODE THREAD FUNCTION
// void* CodedWorker::parallelDecoder( void* pthis )
// {
//   CodedWorker* parent = ( CodedWorker* ) pthis;

//   unsigned long maxNumJob = parent->maxDecodeJob;
//   while( maxNumJob > 0 ) {
//     // decode from queue
//     if( parent->decodeQueue.empty() ) {
//       continue;
//     }

//     DecodeJob job = parent->decodeQueue.front();
//     parent->decodeQueue.pop();
//     SubsetSId& nsid = job.sid;
//     EnData& endata = job.endata;
//     LineList* cdData = endata.data;
//     vector< MetaData >& metaList = endata.metaList;

//     unsigned int numDecode = 0;
//     // Decode per VpairList
//     MetaData dcMeta;
//     for( auto mit = metaList.begin(); mit != metaList.end(); mit++ ) {
//       MetaData& meta = *mit;
//       if( parent->encodePreData[ nsid ].find( meta.vpList ) == parent->encodePreData[ nsid ].end() ) {
// 	dcMeta = meta;
// 	// No original data for decoding;
// 	continue;
//       }
//       LineList* oData = parent->encodePreData[ nsid ][ meta.vpList ][ meta.partNumber - 1 ];
//       auto cdlit = cdData->begin();
//       auto olit = oData->begin();
//       while( cdlit != cdData->end() && olit != oData->end() ) {
// 	// 4-Byte by 4-Byte decoding
// 	// This assumes that line size is divisible by 4
// 	unsigned int maxIter = parent->conf->getLineSize() / sizeof( uint32_t );
// 	for( unsigned int i = 0; i < maxIter; i++ ) {
// 	  ((uint32_t*) *cdlit)[ i ] ^= ((uint32_t*) *olit)[ i ];
// 	}

// 	cdlit++;
// 	olit++;
//       }
//       numDecode++;
//     }

//     // sanity check
//     if( numDecode != metaList.size() - 1 ) {
//       cout << parent->rank << ": Decode error " << numDecode << '/' << metaList.size() - 1 << endl;
//       assert( numDecode != metaList.size() - 1 );
//     }

//     // Trim decoded data back to its original data size
//     while( cdData->size() > dcMeta.size ) {
//       cdData->pop_back();
//     }

//     if( parent->decodePreData[ nsid ][ dcMeta.vpList ].empty() ) {
//       for( unsigned int i = 0; i < parent->conf->getLoad(); i++ ) {
// 	parent->decodePreData[ nsid ][ dcMeta.vpList ].push_back( NULL );
//       }
//     }

//     parent->decodePreData[ nsid ][ dcMeta.vpList ][ dcMeta.partNumber - 1] = cdData;
//     maxNumJob--;
//     cout << parent->rank << ": maxNumjob " << maxNumJob << endl;
//   }

//   return NULL;
// }

void CodedWorker::execReduce()
{
  // if( rank == 1) {
  //   cout << rank << ":Sort " << localList.size() << " lines\n";
  // }
  // stable_sort( localList.begin(), localList.end(), Sorter( conf->getKeySize() ) );
  sort(localList.begin(), localList.end(), Sorter(conf->getKeySize()));
}

// void CodedWorker::sendEncodeData(CodedWorker::EnData &endata, MPI::Intracomm &comm)
// {
//   // Send actual data
//   unsigned lineSize = conf->getLineSize();
//   int rootId = comm.Get_rank();
//   comm.Bcast(&(endata.size), 1, MPI::UNSIGNED_LONG_LONG, rootId);
//   comm.Bcast(endata.data, endata.size * lineSize, MPI::UNSIGNED_CHAR, rootId);
//   delete[] endata.data;

//   // Send serialized meta data
//   comm.Bcast(&(endata.metaSize), 1, MPI::UNSIGNED_LONG_LONG, rootId);
//   comm.Bcast(endata.serialMeta, endata.metaSize, MPI::UNSIGNED_CHAR, rootId);
//   delete[] endata.serialMeta;
// }

// void CodedWorker::recvEncodeData(SubsetSId nsid, unsigned int rootId, MPI::Intracomm &comm)
// {
//   EnData endata;
//   unsigned lineSize = conf->getLineSize();

//   // Receive actual data
//   comm.Bcast(&(endata.size), 1, MPI::UNSIGNED_LONG_LONG, rootId);
//   endata.data = new unsigned char[endata.size * lineSize];
//   comm.Bcast(endata.data, endata.size * lineSize, MPI::UNSIGNED_CHAR, rootId);

//   // Receive serialized meta data
//   comm.Bcast(&(endata.metaSize), 1, MPI::UNSIGNED_LONG_LONG, rootId);
//   endata.serialMeta = new unsigned char[endata.metaSize];
//   comm.Bcast((unsigned char *)endata.serialMeta, endata.metaSize, MPI::UNSIGNED_CHAR, rootId);

//   // De-serialized meta data
//   unsigned char *p = endata.serialMeta;
//   unsigned int metaNum;
//   memcpy(&metaNum, p, sizeof(unsigned int));
//   p += sizeof(unsigned int);
//   // meta data List
//   for (unsigned int m = 0; m < metaNum; m++)
//   {
//     MetaData mdata;
//     // vpair List
//     unsigned int numVp;
//     memcpy(&numVp, p, sizeof(unsigned int));
//     p += sizeof(unsigned int);
//     for (unsigned int v = 0; v < numVp; v++)
//     {
//       Vpair vp;
//       memcpy(&(vp.first), p, sizeof(int));
//       p += sizeof(int);
//       memcpy(&(vp.second), p, sizeof(int));
//       p += sizeof(int);
//       mdata.vpList.push_back(vp);
//     }
//     // VpairSize Map
//     unsigned int numVps;
//     memcpy(&numVps, p, sizeof(unsigned int));
//     p += sizeof(unsigned int);
//     for (unsigned int vs = 0; vs < numVps; vs++)
//     {
//       Vpair vp;
//       unsigned long long size;
//       memcpy(&(vp.first), p, sizeof(int));
//       p += sizeof(int);
//       memcpy(&(vp.second), p, sizeof(int));
//       p += sizeof(int);
//       memcpy(&size, p, sizeof(unsigned long long));
//       p += sizeof(unsigned long long);
//       mdata.vpSize[vp] = size;
//     }
//     memcpy(&(mdata.partNumber), p, sizeof(unsigned int));
//     p += sizeof(unsigned int);
//     memcpy(&(mdata.size), p, sizeof(unsigned long long));
//     p += sizeof(unsigned long long);
//     endata.metaList.push_back(mdata);
//   }
//   delete[] endata.serialMeta;

//   // Serial decoder
//   encodeDataRecv[nsid].push_back(endata);
// }

void CodedWorker::sendEncodeDataMulticast(SubsetSId nsid, CodedWorker::EnData &endata)
{
  // Send actual data to router(rank == 1)
  int routerRank = conf->getNumMapper() + 1;
  unsigned lineSize = conf->getLineSize();
  MPI_Send(&(endata.size), 1, MPI_UNSIGNED_LONG_LONG, routerRank, 0, MPI_COMM_WORLD);
  MPI_Send(endata.data, endata.size * lineSize, MPI_UNSIGNED_CHAR, routerRank, 0, MPI_COMM_WORLD);
  // comm.Bcast(&(endata.size), 1, MPI::UNSIGNED_LONG_LONG, rootId);
  // comm.Bcast(endata.data, endata.size * lineSize, MPI::UNSIGNED_CHAR, rootId);
  delete[] endata.data;

  // Send serialized meta data
  MPI_Send(&(endata.metaSize), 1, MPI_UNSIGNED_LONG_LONG, routerRank, 0, MPI_COMM_WORLD);
  MPI_Send(endata.serialMeta, endata.metaSize, MPI_UNSIGNED_CHAR, routerRank, 0, MPI_COMM_WORLD);
  // comm.Bcast(&(endata.metaSize), 1, MPI::UNSIGNED_LONG_LONG, rootId);
  // comm.Bcast(endata.serialMeta, endata.metaSize, MPI::UNSIGNED_CHAR, rootId);
  delete[] endata.serialMeta;
}

void CodedWorker::recvEncodeDataMulticast(SubsetSId nsid, int fileIdx)
{
  EnData endata;
  unsigned lineSize = conf->getLineSize();
  int routerRank = conf->getNumMapper() + 1;

  // Receive actual data
  // comm.Bcast(&(endata.size), 1, MPI::UNSIGNED_LONG_LONG, rootId);
  MPI_Recv(&(endata.size), 1, MPI_UNSIGNED_LONG_LONG, routerRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  endata.data = new unsigned char[endata.size * lineSize];
  MPI_Recv(endata.data, endata.size * lineSize, MPI_UNSIGNED_CHAR, routerRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  // comm.Bcast(endata.data, endata.size * lineSize, MPI::UNSIGNED_CHAR, rootId);

  // Receive serialized meta data
  // comm.Bcast(&(endata.metaSize), 1, MPI::UNSIGNED_LONG_LONG, rootId);
  MPI_Recv(&(endata.metaSize), 1, MPI_UNSIGNED_LONG_LONG, routerRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  endata.serialMeta = new unsigned char[endata.metaSize];
  MPI_Recv((unsigned char *)endata.serialMeta, endata.metaSize, MPI_UNSIGNED_CHAR, routerRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  // comm.Bcast((unsigned char *)endata.serialMeta, endata.metaSize, MPI::UNSIGNED_CHAR, rootId);

  // De-serialized meta data
  unsigned char *p = endata.serialMeta;
  unsigned int metaNum;
  memcpy(&metaNum, p, sizeof(unsigned int));
  p += sizeof(unsigned int);
  // meta data List
  for (unsigned int m = 0; m < metaNum; m++)
  {
    MetaData mdata;
    // vpair List
    unsigned int numVp;
    memcpy(&numVp, p, sizeof(unsigned int));
    p += sizeof(unsigned int);
    for (unsigned int v = 0; v < numVp; v++)
    {
      Vpair vp;
      memcpy(&(vp.first), p, sizeof(int));
      p += sizeof(int);
      memcpy(&(vp.second), p, sizeof(int));
      p += sizeof(int);
      mdata.vpList.push_back(vp);
    }
    // VpairSize Map
    unsigned int numVps;
    memcpy(&numVps, p, sizeof(unsigned int));
    p += sizeof(unsigned int);
    for (unsigned int vs = 0; vs < numVps; vs++)
    {
      Vpair vp;
      unsigned long long size;
      memcpy(&(vp.first), p, sizeof(int));
      p += sizeof(int);
      memcpy(&(vp.second), p, sizeof(int));
      p += sizeof(int);
      memcpy(&size, p, sizeof(unsigned long long));
      p += sizeof(unsigned long long);
      mdata.vpSize[vp] = size;
    }
    memcpy(&(mdata.partNumber), p, sizeof(unsigned int));
    p += sizeof(unsigned int);
    memcpy(&(mdata.size), p, sizeof(unsigned long long));
    p += sizeof(unsigned long long);
    endata.metaList.push_back(mdata);
  }
  delete[] endata.serialMeta;

  // Serial decoder
  encodeDataRecv[nsid][fileIdx].push_back(endata);
}

void CodedWorker::genMulticastGroup() // not in use
{
  // map<NodeSet, SubsetSId> ssmap = cg->getSubsetSIdMap();
  // for (auto nsit = ssmap.begin(); nsit != ssmap.end(); nsit++)
  // {
  //   NodeSet ns = nsit->first;
  //   SubsetSId nsid = nsit->second;
  //   int color = (ns.find(rank) != ns.end()) ? 1 : 0;
  //   MPI::Intracomm mgComm = workerComm.Split(color, rank);
  //   multicastGroupMap[nsid] = mgComm;
  // }
}

void CodedWorker::printLocalList()
{
  unsigned long int i = 0;
  for (auto it = localList.begin(); it != localList.end(); ++it)
  {
    cout << rank << ": " << i++ << "| ";
    printKey(*it, conf->getKeySize());
    cout << endl;
  }
}

// void CodedWorker::writeInputPartitionCollection()
// {
//   char buff[MAX_FILE_PATH];
//   sprintf(buff, "./Tmp/InputPartitionCollection_%u", rank);
//   ofstream outf(buff, ios::out | ios::binary | ios::trunc);
//   InputSet inputSet = cg->getM(rank);
//   for (auto init = inputSet.begin(); init != inputSet.end(); init++)
//   {
//     unsigned int inputId = *init;
//     PartitionCollection &pc = inputPartitionCollection[inputId];
//     for (auto pit = pc.begin(); pit != pc.end(); pit++)
//     {
//       unsigned int parId = pit->first;
//       LineList *list = pit->second;
//       sprintf(buff, ">> Input %u, Partition %u <<", inputId, parId);
//       outf.write(buff, strlen(buff));
//       for (auto lit = list->begin(); lit != list->end(); lit++)
//       {
//         outf.write((char *)*lit, conf->getLineSize());
//       }
//     }
//   }
//   outf.close();
//   cout << rank << ": InputPartitionCollection is saved.\n";
// }

void CodedWorker::outputLocalList()
{
  char buff[MAX_FILE_PATH];
  int logicalId = conf->getLogicalNodeId(rank);
  sprintf(buff, "%s_%u", conf->getOutputPath(), logicalId - 1);
  ofstream outputFile(buff, ios::out | ios::binary | ios::trunc);
  for (auto it = localList.begin(); it != localList.end(); ++it)
  {
    outputFile.write((char *)*it, conf->getLineSize());
  }
  outputFile.close();
  // cout << rank << ": outputFile " << buff << " is saved.\n";
}

TrieNode *CodedWorker::buildTrie(PartitionList *partitionList, int lower, int upper, unsigned char *prefix, int prefixSize, int maxDepth)
{
  if (prefixSize >= maxDepth || lower == upper)
  {
    return new LeafTrieNode(prefixSize, partitionList, lower, upper);
  }
  InnerTrieNode *result = new InnerTrieNode(prefixSize);
  int curr = lower;
  for (unsigned char ch = 0; ch < 255; ch++)
  {
    prefix[prefixSize] = ch;
    lower = curr;
    while (curr < upper)
    {
      if (cmpKey(prefix, partitionList->at(curr), prefixSize + 1))
      {
        break;
      }
      curr++;
    }
    result->setChild(ch, buildTrie(partitionList, lower, curr, prefix, prefixSize + 1, maxDepth));
  }
  prefix[prefixSize] = 255;
  result->setChild(255, buildTrie(partitionList, curr, upper, prefix, prefixSize + 1, maxDepth));
  return result;
}
