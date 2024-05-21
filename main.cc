#include <iostream>
#include <mpi.h>

#include "Master.h"
#include "Worker.h"

using namespace std;

int main(int argc, char *argv[])
{
  MPI_Init(&argc, &argv);
  int nodeRank, nodeTotal;
  MPI_Comm_rank(MPI_COMM_WORLD, &nodeRank);
  MPI_Comm_size(MPI_COMM_WORLD, &nodeTotal);

  if ( nodeRank == 0 ) {
    Master masterNode( nodeRank, nodeTotal );
    masterNode.run();
  }
  else {
    Worker workerNode( nodeRank );
    workerNode.run();
  }

  MPI_Finalize();
  
  return 0;
}
