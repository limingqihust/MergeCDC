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
  if ( totalNode != 1 + conf.getNumMapper() ) {
    cout << "The number of workers mismatches the number of processes.\n";
    assert( false );
  }

  // GENERATE LIST OF PARTITIONS.
  PartitionSampling partitioner;
  partitioner.setConfiguration( &conf );
  PartitionList* partitionList = partitioner.createPartitions();


  // BROADCAST CONFIGURATION TO WORKERS
  MPI_Bcast(&conf, sizeof(Configuration), MPI_CHAR, 0, MPI_COMM_WORLD);
  // Note: this works because the number of partitions can be derived from the number of workers in the configuration.
  conf.ParseFileDistribution();

  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    unsigned char* partition = *it;
    MPI_Bcast(partition, conf.getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
  }

  vector<int> reducerNodes = conf.getReducerNodes();
  // TIME BUFFER
  int numWorker = conf.getNumMapper();
  double rcvTime[ numWorker + 1 ];  
  double rTime = 0;
  double avgTime;
  double maxTime;


  // COMPUTE MAP TIME
  MPI_Gather(&rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": MAP     | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;


  // // COMPUTE PACKING TIME
  // MPI_Gather(&rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
  // avgTime = 0;
  // maxTime = 0;
  // for( int i = 1; i <= numWorker; i++ ) {
  //   avgTime += rcvTime[i];
  //   maxTime = max( maxTime, rcvTime[ i ] );
  // }
  // cout << rank << ": PACK    | Avg = " << setw(10) << avgTime/numWorker
  //      << "   Max = " << setw(10) << maxTime << endl;  

  
  // COMPUTE SHUFFLE TIME
  double tx = 0;
  double avgTx = 0;
  avgTime = 0;
  for( unsigned int i = 1; i <= numWorker; i++ ) {
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Recv(&rTime, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    avgTime += rTime;
    MPI_Recv(&tx, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);    
    avgTx += tx;
  }
  cout << rank << ": SHUFFLE | SumTime = " << setw(10) << avgTime << "   AvgTime = " << setw(10) << avgTime/numWorker
       << "   Tx = " << setw(10) << avgTx << " MB" << endl;  


  // COMPUTE UNPACK TIME
  MPI_Gather(&rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
  avgTime = 0;
  maxTime = 0;
  for( int i = 0; i < reducerNodes.size(); i++ ) {
    avgTime += rcvTime[ reducerNodes[i] ];
    maxTime = max( maxTime, rcvTime[ reducerNodes[i] ] );
  }
  cout << rank << ": UNPACK  | Avg = " << setw(10) << avgTime/reducerNodes.size()
       << "   Max = " << setw(10) << maxTime << endl;
  
  
  
  // COMPUTE REDUCE TIME
  MPI_Gather(&rTime, 1, MPI_DOUBLE, rcvTime, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
  avgTime = 0;
  maxTime = 0;
  for( int i = 0; i < reducerNodes.size(); i++ ) {
    avgTime += rcvTime[ reducerNodes[i] ];
    maxTime = max( maxTime, rcvTime[ reducerNodes[i] ] );
  }
  cout << rank << ": REDUCE  | Avg = " << setw(10) << avgTime/reducerNodes.size()
       << "   Max = " << setw(10) << maxTime << endl;      
  

  // CLEAN UP MEMORY
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    delete [] *it;
  }
}
