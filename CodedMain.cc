#include <iostream>
#include <mpi.h>

#include "CodedMaster.h"
#include "CodedWorker.h"
#include "CodedRouter.h"

using namespace std;

int main(int argc, char *argv[])
{
  MPI_Init(&argc, &argv);
  int nodeRank, nodeTotal;
  MPI_Comm_rank(MPI_COMM_WORLD, &nodeRank);
  MPI_Comm_size(MPI_COMM_WORLD, &nodeTotal);

  if ( nodeRank == 0 ) {
    CodedMaster masterNode( nodeRank, nodeTotal );
    MPI_Comm comm;
    MPI_Comm_split(MPI_COMM_WORLD, 0, nodeRank, &comm);
    masterNode.run();
  } else if(nodeRank == nodeTotal - 1) {
    CodedRouter routerNode(nodeRank);
    MPI_Comm workerComm;
    MPI_Comm_split(MPI_COMM_WORLD, 1, nodeRank, &workerComm);
    routerNode.setWorkerComm(workerComm);
    routerNode.run();
  } else {
    CodedWorker workerNode( nodeRank );
    MPI_Comm workerComm;
    MPI_Comm_split(MPI_COMM_WORLD, 1, nodeRank, &workerComm);
    workerNode.setWorkerComm( workerComm );
    workerNode.run();
  }

  MPI_Finalize();
  
  return 0;
}
