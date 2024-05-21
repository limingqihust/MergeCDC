#ifndef _CMR_ROUTER
#define _CMR_ROUTER

#include <mpi.h>
#include <unordered_map>
#include <pthread.h>
#include <unistd.h>

#include "CodedConfiguration.h"
#include "CodeGeneration.h"
#include "Common.h"
#include "Utility.h"
#include "Trie.h"

using namespace std;

class CodedRouter {
public:
    CodedRouter( unsigned int r ): rank( r ) {}
    ~CodedRouter();
    void setWorkerComm( MPI_Comm& comm ) { workerComm = comm; }
    void run();
private:
    unsigned int rank;
    MPI_Comm workerComm;
    CodeGeneration *cg;
    CodedConfiguration *conf;
    void routeShuffleData();
    void genMulticastGroup();
};

#endif