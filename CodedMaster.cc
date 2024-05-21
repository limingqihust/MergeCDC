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
  cout << rank
       << ": CODEGEN | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;
      

  // COMPUTE MAP TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": MAP     | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  
  
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
  cout << rank
       << ": INNERTRANS     | Sum = " << setw(10) << avgTime
       << "   Avg = " << setw(10) << avgTime/(numWorker - numReducer)
       << "   TX = " << setw(10) << maxTime << " MB" << endl;  

  // COMPUTE ENCODE TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": ENCODE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  // COMPUTE SHUFFLE TIME
  avgTime = 0;
  double tx = 0;
  double avgtx = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI_Recv( &rTime, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgTime += rTime;    
    MPI_Recv( &tx, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgtx += tx;
  }
  cout << rank
       << ": SHUFFLE | SumTime = " << setw(10) << avgTime << "   AvgTime = " << setw(10) << avgTime/numWorker
       << "   TX = " << setw(10) << avgtx << " MB" << endl;
  
  avgTime = 0;
  avgtx = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI_Recv( &rTime, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgTime += rTime;
    MPI_Recv( &tx, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgtx += tx;
  }
  cout << rank
       << ": SHUFFLE(P2P) | SumTime = " << setw(10) << avgTime << "   AvgTime = " << setw(10) << avgTime/numWorker
       << "   TX = " << setw(10) << avgtx << " MB" << endl;
  
  avgTime = 0;
  avgtx = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI_Recv( &rTime, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgTime += rTime;
    MPI_Recv( &tx, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
    avgtx += tx;
  }
  cout << rank
       << ": SHUFFLE(MULTI) | SumTime = " << setw(10) << avgTime << "   AvgTime = " << setw(10) << avgTime/numReducer
       << "   TX = " << setw(10) << avgtx << " MB" << endl;

  // COMPUTE DECODE TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  const vector<int> reducerNodes = conf.getReducerNodes();
  for( int i = 0; i < reducerNodes.size(); i++ ) {
    avgTime += rcvTime[ reducerNodes[i] ];
    maxTime = max( maxTime, rcvTime[ reducerNodes[i] ] );
  }
  cout << rank
       << ": DECODE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  // COMPUTE REDUCE TIME
  MPI_Gather( &rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD );
  avgTime = 0;
  maxTime = 0;
  for( int i = 0; i < reducerNodes.size(); i++ ) {
    avgTime += rcvTime[ reducerNodes[i] ];
    maxTime = max( maxTime, rcvTime[ reducerNodes[i] ] );
  }
  cout << rank
       << ": REDUCE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;      
  

  // CLEAN UP MEMORY
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    delete [] *it;
  }
}
