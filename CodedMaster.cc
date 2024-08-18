#include <iostream>
#include <assert.h>
#include <mpi.h>
#include <iomanip>

#include "CodedMaster.h"
#include "Common.h"
#include "CodedConfiguration.h"
#include "PartitionSampling.h"

using namespace std;

void CodedMaster::run()
{
  if ( totalNode != 2 + conf.getNumMapper() ) {
    cout << "The number of workers mismatches the number of processes.\n";
    assert( false );
  }

  // GENERATE LIST OF PARTITIONS.
  PartitionSampling partitioner;
  partitioner.setConfiguration( &conf );
  PartitionList* partitionList = partitioner.createPartitions(); // 取样的 reduce partitions 用来为每个 reduce 节点划定范围


  // BROADCAST CONFIGURATION TO WORKERS
  MPI_Bcast(&conf, sizeof(CodedConfiguration), MPI_CHAR, 0, MPI_COMM_WORLD);
  // Note: this works because the number of partitions can be derived from the number of workers in the configuration.
  conf.ParseFileDistribution();

  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    unsigned char* partition = *it;
    MPI_Bcast( partition, conf.getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD );
  }

  // TIME BUFFER
  int numWorker = conf.getNumMapper();
  double rcvTime[ numWorker + 2 ];  
  double rTime = 0;
  double avgTime;
  double maxTime;


  // COMPUTE CODE GENERATION TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << "CODEGEN " << avgTime/numWorker << " " << maxTime << endl;
      

  // COMPUTE MAP TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << "MAP " << avgTime/numWorker << " " << maxTime << endl;
  
  // inner node intermediate value transfer
  double innerTx[numWorker + 2], rTx = 0;
  int numReducer = conf.getNumReducer();
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  MPI_Gather( &rTx, 1, MPI_DOUBLE, innerTx, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime += innerTx[i];
  }
  cout << "INNERTRANS " << avgTime/(numWorker - numReducer) << " " << maxTime << endl;

  // COMPUTE ENCODE TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << "ENCODE " << avgTime/numWorker << " " << maxTime << endl;

  // COMPUTE SHUFFLE TIME
  avgTime = 0;
  maxTime = 0;
  double tx = 0;
  double avgtx = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI_Recv( &rTime, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgTime += rTime;    
    maxTime = max( maxTime, rTime );
    MPI_Recv( &tx, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgtx += tx;
  }
  cout << "SHUFFLE " << avgTime << " " << maxTime << " " << avgtx << endl;
  double size = conf.getLineSize() * conf.getNumSamples();
  std::cout << "SHUFFLE DATA SIZE(byte): " << conf.getLineSize() * conf.getNumSamples() << std::endl;
  std::cout << "SHUFFLE RATIO(byte/s):" << size / avgTime << std::endl;
  avgTime = 0;
  maxTime = 0;
  avgtx = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI_Recv( &rTime, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgTime += rTime;
    maxTime = max( maxTime, rTime );
    MPI_Recv( &tx, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgtx += tx;
  }
  cout << "SHUFFLE(P2P) " << avgTime/numWorker << " " << maxTime << endl;
  avgTime = 0;
  maxTime = 0;
  avgtx = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI_Recv( &rTime, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgTime += rTime;
    maxTime = max( maxTime, rTime );
    MPI_Recv( &tx, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgtx += tx;
  }
  cout << "SHUFFLE(MULTI) " << avgTime/numWorker << " " << maxTime << endl;

  // COMPUTE DECODE TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  const vector<int> reducerNodes = conf.getReducerNodes();
  for( int i = 0; i < reducerNodes.size(); i++ ) {
    avgTime += rcvTime[ reducerNodes[i] ];
    maxTime = max( maxTime, rcvTime[ reducerNodes[i] ] );
  }
  cout << "DECODE " << avgTime/numWorker << " " << maxTime << endl;
  // COMPUTE REDUCE TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 0; i < reducerNodes.size(); i++ ) {
    avgTime += rcvTime[ reducerNodes[i] ];
    maxTime = max( maxTime, rcvTime[ reducerNodes[i] ] );
  }
  cout << "REDUCE " << avgTime/reducerNodes.size() << " " << maxTime << endl;
  

  // CLEAN UP MEMORY
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    delete [] *it;
  }
}
