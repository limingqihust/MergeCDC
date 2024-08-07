#include <iostream>
#include <assert.h>
#include <mpi.h>
#include <iomanip>

#include "Master.h"
#include "Common.h"
#include "Configuration.h"
#include "PartitionSampling.h"

using namespace std;

void Master::run()
{
  struct timeval total_start, total_end;
  double total_time;
  gettimeofday(&total_start,NULL);
  if ( totalNode != 1 + conf.getNumReducer() ) {
    cout << "The number of workers mismatches the number of processes.\n";
    assert( false );
  }

  // GENERATE LIST OF PARTITIONS.
  PartitionSampling partitioner;
  partitioner.setConfiguration( &conf );
  PartitionList* partitionList = partitioner.createPartitions();


  // BROADCAST CONFIGURATION TO WORKERS
  MPI::COMM_WORLD.Bcast( &conf, sizeof( Configuration ), MPI::CHAR, 0 );
  // Note: this works because the number of partitions can be derived from the number of workers in the configuration.


  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize() + 1, MPI::UNSIGNED_CHAR, 0 );
  }

  
  // TIME BUFFER
  int numWorker = conf.getNumReducer();
  double rcvTime[ numWorker + 1 ];  
  double rTime = 0;
  double avgTime;
  double maxTime;


  // COMPUTE MAP TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": MAP     | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;


  // COMPUTE PACKING TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": PACK    | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  
  // COMPUTE SHUFFLE TIME
  double txRate = 0;
  double avgRate = 0;
  for( unsigned int i = 1; i <= conf.getNumReducer(); i++ ) {
    MPI::COMM_WORLD.Barrier();
    MPI::COMM_WORLD.Barrier();
    MPI::COMM_WORLD.Recv( &rTime, 1, MPI::DOUBLE, i, 0 );
    avgTime += rTime;
    MPI::COMM_WORLD.Recv( &txRate, 1, MPI::DOUBLE, i, 0 );
    avgRate += txRate;
  }
  cout << rank << ": SHUFFLE | Sum = " << setw(10) << avgTime
       << "   Rate = " << setw(10) << avgRate/numWorker << " Mbps" << endl;  


  // COMPUTE UNPACK TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": UNPACK  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;
  
  
  
  heapSort();
  encodeAndSort();
  // COMPUTE REDUCE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": REDUCE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;      
  
  receiveAndDecode();
  // CLEAN UP MEMORY
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    delete [] *it;
  }
  for (auto heap: heaps) {
    for (auto key: heap) {
      delete [] key;
    }
  }
  for (auto key: encodedList) {
    delete [] key;
  }
  gettimeofday(&total_end,NULL);
  total_time = (total_end.tv_sec*1000000.0 + total_end.tv_usec - total_start.tv_sec*1000000.0 - total_start.tv_usec) / 1000000.0;
  std::cout << "total time: " << total_time << std::endl;
}


void Master::heapSort() {
  // receive keys from workers
  int size = conf.getNumSamples() / conf.getNumReducer();
  double time = 0;
  for (int i = 1; i <= conf.getNumReducer(); i++) {
    // std::cout << "master receive from rank " << i << " size: " << size << std::endl;
    LineList heap;
    for (int j = 0; j < size; j++) {
      unsigned char* key = new unsigned char[conf.getKeySize()];
      MPI::COMM_WORLD.Recv(key, conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0);
      heap.push_back(key);
    }
    // printLineList(heap);
    heaps.push_back(heap);
    // std::cout << "master receive from rank " << i << " done" << std::endl;
    double time_temp = 0;
    MPI::COMM_WORLD.Recv(&time_temp, 1, MPI::DOUBLE, i, 0);
    time += time_temp;
    time /= conf.getNumReducer();
    MPI::COMM_WORLD.Barrier();
  }
  std::cout << "encode pre time: " << time << std::endl;

}

void Master::printLineList(LineList list)
{
  unsigned long int i = 0;
  for ( auto it = list.begin(); it != list.end(); ++it ) {
    cout << rank << ": " << i++ << "| ";
    printKey( *it, conf.getKeySize() );
    cout << endl;
  }
}


void Master::encodeAndSort() {

  struct timeval start, end;
  double time;

  int size = conf.getNumSamples() / conf.getNumReducer();
  for (int i = 0; i < size; i++) {
    unsigned char* key = new unsigned char[conf.getKeySize()];
    memset(key, 0, conf.getKeySize());
    encodedList.push_back(key);
  }
  gettimeofday(&start,NULL);
  for (int i = 0; i < conf.getNumReducer(); i++) {
    for (int j = 0; j < size; j++) {
      for (int k = 0; k < conf.getKeySize(); k++) {
        encodedList[j][k] += heaps[i][j][k];
      }
    }
  }
  // std::cout << "encoded list:" << std::endl;
  // printLineList(encodedList);
  gettimeofday(&end,NULL);
  time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  std::cout << "encode time: " << time << std::endl;
  MPI::COMM_WORLD.Barrier();

  std::sort(encodedList.begin(), encodedList.end(), [&](const unsigned char* keyA, const unsigned char* keyB) {
    return cmpKey(keyA, keyB, conf.getKeySize());
  });
}


void Master::receiveAndDecode() {
  int fail_index = 15;
  std::vector<LineList> decode_lists;
  struct timeval start, end;
  double time;
  gettimeofday(&start,NULL);
  int size = conf.getNumSamples() / conf.getNumReducer();
  // receive 
  for (int i = 1; i <= conf.getNumReducer(); i++) {
    if (i == fail_index) {
      MPI::COMM_WORLD.Barrier();
      continue;
    }
    // receive from worker[i]
    LineList decode_list;
    for (int j = 0; j < size; j++) {
      unsigned char* key = new unsigned char[conf.getKeySize()];
      MPI::COMM_WORLD.Recv(key, conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0);
      decode_list.push_back(key);
    }
    decode_lists.push_back(decode_list);
    MPI::COMM_WORLD.Barrier();
  }
  gettimeofday(&end, NULL);
  time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  std::cout << "transfer time: " << time << std::endl;


  // decode
  gettimeofday(&start,NULL);
  for (int i = 0; i < conf.getNumReducer() - 1; i++) {
    for (int j = 0; j < size; j++) {
      for (int k = 0; k < conf.getKeySize(); k++) {
        encodedList[j][k] -= decode_lists[i][j][k];
      }
    }
  }
  // printLineList(encodedList);

  gettimeofday(&end, NULL);
  time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  std::cout << "decode time: " << time << std::endl;


  for (auto decode_list: decode_lists) {
    for (auto key: decode_list) {
      delete [] key;
    }
  }
}
