#include <iostream>
#include <mpi.h>
#include <iomanip>
#include <fstream>
#include <cstdio>
#include <map>
#include <assert.h>
#include <algorithm>
#include <ctime>

#include "Worker.h"
#include "Configuration.h"
#include "Common.h"
#include "Utility.h"

using namespace std;

Worker::~Worker()
{
  delete conf;
  for (auto it = partitionList.begin(); it != partitionList.end(); ++it)
  {
    delete[] * it;
  }

  for (auto it = localList.begin(); it != localList.end(); ++it)
  {
    delete[] * it;
  }

  delete trie;
}

void Worker::run()
{
  // RECEIVE CONFIGURATION FROM MASTER
  conf = new Configuration;
  MPI_Bcast((void *)conf, sizeof(Configuration), MPI_CHAR, 0, MPI_COMM_WORLD);

  conf->ParseFileDistribution();

  // RECEIVE PARTITIONS FROM MASTER
  for (unsigned int i = 1; i < conf->getNumReducer(); i++)
  {
    unsigned char *buff = new unsigned char[conf->getKeySize() + 1];
    MPI_Bcast(buff, conf->getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
    partitionList.push_back(buff);
  }

  // EXECUTE MAP PHASE
  struct timeval start, end;
  double rTime = 0;
  execMap();

  // SHUFFLING PHASE
  unsigned int lineSize = conf->getLineSize();
  vector<int> reducerNodes = conf->getReducerNodes();
  int reducerIdx = -1;
  for(int i = 0; i < reducerNodes.size(); i++) {
    if(reducerNodes[i] == rank) {
      reducerIdx = i;
    }
  }
  for (unsigned int i = 1; i <= conf->getNumMapper(); i++)
  {
    if (i == rank)
    {
      // clock_t txTime = 0;
      unsigned long long tolSize = 0;
      const vector<int> &fileList = conf->getWorkLoad(rank);
      MPI_Barrier(MPI_COMM_WORLD);
      // Sending from node i
      for (unsigned int j = 0; j < conf->getNumReducer(); j++)
      {
        int reducerRank = conf->getReducerId(j + 1);
        if (reducerRank == rank)
        {
          continue;
        }
        int fileNum = fileList.size();
        MPI_Send(&fileNum, 1, MPI_INT, reducerRank, 0, MPI_COMM_WORLD);
        tolSize += sizeof(int);
        vector<pair<unsigned long long, unsigned char *>> shuffleBlocks;
        for(int fileId : fileList) {
          auto ll = inputPartitionCollection[fileId][j];
          unsigned long long numLine = ll->size();
          unsigned char *data = new unsigned char[numLine * lineSize];
          unsigned char *p = data;
          for(auto lit = ll->begin(); lit != ll->end(); lit++) {
            memcpy(p, *lit, lineSize);
            p += lineSize;
            delete [] *lit;
          }
          delete ll;
          shuffleBlocks.push_back(make_pair(numLine, data));
        }
        for(auto it : shuffleBlocks) {
          unsigned long long numLine = it.first;
          unsigned char *p = it.second;
          gettimeofday(&start, NULL);
          MPI_Send(&numLine, 1, MPI_UNSIGNED_LONG_LONG, reducerRank, 0, MPI_COMM_WORLD);
          MPI_Send(p, numLine * lineSize, MPI_UNSIGNED_CHAR, reducerRank, 0, MPI_COMM_WORLD);
          gettimeofday(&end, NULL);
          rTime += (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
          tolSize += numLine * lineSize + sizeof(unsigned long long);
          delete []p;
        }
        // TxData &txData = partitionTxData[j];
        // // txTime -= clock();
        // MPI_Send(&(txData.numLine), 1, MPI_UNSIGNED_LONG_LONG, reducerRank, 0, MPI_COMM_WORLD);
        // MPI_Send(txData.data, txData.numLine * lineSize, MPI_UNSIGNED_CHAR, reducerRank, 0, MPI_COMM_WORLD);
        // // txTime += clock();
        // tolSize += txData.numLine * lineSize + sizeof(unsigned long long);
        // delete[] txData.data;
      }
      MPI_Barrier(MPI_COMM_WORLD);
      double tx = tolSize * 1e-6;
      MPI_Send(&rTime, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      MPI_Send(&tx, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
      // cout << rank << ": Avg sending rate is " << ( tolSize * 8 ) / ( rtxTime * 1e6 ) << " Mbps, Data size is " << tolSize / 1e6 << " MByte\n";
    }
    else if(reducerIdx != -1)
    {
      MPI_Barrier(MPI_COMM_WORLD);
      // Receiving from node i
      int fileNum;
      TxData &rxData = partitionRxData[i - 1];
      vector<pair<unsigned long long, unsigned char *>> shuffleBlocks;
      unsigned long long totSize = 0;
      MPI_Recv(&fileNum, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      for(int j = 0; j < fileNum; j++) {
        unsigned long long numLine;
        MPI_Recv(&numLine, 1, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        totSize += numLine;
        unsigned char *p = new unsigned char[numLine * lineSize];
        MPI_Recv(p, numLine * lineSize, MPI_UNSIGNED_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        shuffleBlocks.push_back(make_pair(numLine * lineSize, p));
      }
      rxData.data = new unsigned char[totSize * lineSize];
      rxData.numLine = totSize;
      unsigned char *p = rxData.data;
      for(auto it : shuffleBlocks) {
        int dataSize = it.first;
        unsigned char *data = it.second;
        memcpy(p, data, dataSize);
        p += dataSize;
        delete []data;
      }
      MPI_Barrier(MPI_COMM_WORLD);
    } else {
      MPI_Barrier(MPI_COMM_WORLD);
      MPI_Barrier(MPI_COMM_WORLD);
    }
  }

  // UNPACK PHASE
  if(reducerIdx == -1) {
    rTime = 0;
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    
    return;
  }
  gettimeofday(&start,NULL);
  // append local partition to localList
  const vector<int> &fileList = conf->getWorkLoad(rank);
  for(int i = 0; i < fileList.size(); i++) {
    unsigned int fileId = fileList[i];
    PartitionCollection &pc = inputPartitionCollection[fileId];
    for(auto it = pc[reducerIdx]->begin(); it != pc[reducerIdx]->end(); it++) {
      unsigned char *buff = new unsigned char[conf->getLineSize()];
      memcpy(buff, *it, conf->getLineSize());
      localList.push_back(buff);
      delete[] *it;
    }
    delete pc[reducerIdx];
  }
  // append data from other workers
  for (unsigned int i = 1; i <= conf->getNumMapper(); i++)
  {
    if (i == rank)
    {
      continue;
    }
    TxData &rxData = partitionRxData[i - 1];
    for (unsigned long long lc = 0; lc < rxData.numLine; lc++)
    {
      unsigned char *buff = new unsigned char[lineSize];
      memcpy(buff, rxData.data + lc * lineSize, lineSize);
      localList.push_back(buff);
    }
    delete[] rxData.data;
  }
  gettimeofday(&end,NULL);
  rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

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

void Worker::execMap()
{
  struct timeval start, end;
  double rTime = 0;
  gettimeofday(&start,NULL);

  // Build trie
  unsigned char prefix[conf->getKeySize()];
  trie = buildTrie(&partitionList, 0, partitionList.size(), prefix, 0, 2);

  // READ INPUT FILE AND PARTITION DATA
  unsigned long int lineSize = conf->getLineSize();
  const vector<int> &fileList = conf->getWorkLoad(rank);
  for(int i = 0; i < fileList.size(); i++) {
    unsigned int fileId = fileList[i];
    
    // read input
    char filePath[MAX_FILE_PATH];
    sprintf(filePath, "%s_%d", conf->getInputPath(), fileId);
    ifstream inputFile(filePath, ios::in | ios::binary | ios::ate);
    if (!inputFile.is_open())
    {
      cout << rank << ": Cannot open input file " << filePath << endl;
      assert(false);
    }

    unsigned long fileSize = inputFile.tellg();
    unsigned long int numLine = fileSize / lineSize;
    inputFile.seekg(0, ios::beg);
    PartitionCollection &pc = inputPartitionCollection[fileId];

    // Create lists of lines
    for (unsigned int j = 0; j < conf->getNumReducer(); j++)
    {
      pc[j] = new LineList;
    }
    // MAP
    // Put each line to associated collection according to partition list
    for (unsigned long j = 0; j < numLine; j++)
    {
      unsigned char *buff = new unsigned char[lineSize];
      inputFile.read((char *)buff, lineSize);
      unsigned int wid = trie->findPartition(buff);
      pc[wid]->push_back(buff);
    }
    inputFile.close();
  }

  gettimeofday(&end,NULL);
  rTime = (end.tv_sec*1000000.0 + end.tv_usec -
		 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  // gettimeofday(&start,NULL);
  // // Packet partitioned data to a chunk
  // vector<int> reducerNodes = conf->getReducerNodes();
  // for (unsigned int i = 0; i < conf->getNumReducer(); i++)
  // {
  //   int reducerRank = conf->getReducerId(i + 1);
  //   if (reducerRank == rank)
  //   {
  //     continue;
  //   }
  //   unsigned long long numLine = 0;
  //   for(int j = 0; j < fileList.size(); j++) {
  //     unsigned int fileId = fileList[j];
  //     PartitionCollection &pc = inputPartitionCollection[fileId];
  //     numLine += pc[i]->size();
  //   }
  //   partitionTxData[i].data = new unsigned char[numLine * lineSize];
  //   partitionTxData[i].numLine = numLine;
  //   unsigned long long offset = 0;
  //   for(int j = 0; j < fileList.size(); j++) {
  //     unsigned int fileId = fileList[j];
  //     PartitionCollection &pc = inputPartitionCollection[fileId];
  //     for(int k = 0; k < pc[i]->size(); k++) {
  //       unsigned char *buf = (*pc[i])[k];
  //       memcpy(partitionTxData[i].data + offset, buf, lineSize);
  //       delete[] buf;
  //       offset += lineSize;
  //     }
  //     // for(auto lit = pc[i]->begin(); lit != pc[i]->end(); lit++) {
  //     //   memcpy(partitionTxData[i].data + offset, *lit, lineSize);
  //     //   delete[] *lit;
  //     // }
  //     delete pc[i];
  //   }
  // }
  // gettimeofday(&end,NULL);
  // rTime = (end.tv_sec*1000000.0 + end.tv_usec -
	// 	 	start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  // MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
}

void Worker::execReduce()
{
  // if( rank == 1) {
  //   cout << rank << ":Sort " << localList.size() << " lines\n";
  // }
  // stable_sort( localList.begin(), localList.end(), Sorter( conf->getKeySize() ) );
  sort(localList.begin(), localList.end(), Sorter(conf->getKeySize()));
}

void Worker::printLocalList()
{
  unsigned long int i = 0;
  for (auto it = localList.begin(); it != localList.end(); ++it)
  {
    cout << rank << ": " << i++ << "| ";
    printKey(*it, conf->getKeySize());
    cout << endl;
  }
}

void Worker::outputLocalList()
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

TrieNode *Worker::buildTrie(PartitionList *partitionList, int lower, int upper, unsigned char *prefix, int prefixSize, int maxDepth)
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
