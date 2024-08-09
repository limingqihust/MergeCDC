#include <iostream>
#include <mpi.h>
#include <unistd.h>
using namespace std;

int main(int argc, char *argv[])
{
  MPI_Init(&argc, &argv);
  int nodeRank, nodeTotal;
  MPI_Comm_rank(MPI_COMM_WORLD, &nodeRank);
  MPI_Comm_size(MPI_COMM_WORLD, &nodeTotal);
  sleep(nodeTotal - nodeRank);
  std::cout << "rank: " << nodeRank << " end" << std::endl;
  MPI_Finalize();
  
  return 0;
}

