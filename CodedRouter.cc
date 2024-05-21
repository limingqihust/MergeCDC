#include "CodedRouter.h"

using namespace std;

CodedRouter::~CodedRouter() {
    delete cg;
    delete conf;
}

void CodedRouter::run() {
    // RECEIVE CONFIGURATION FROM MASTER
    conf = new CodedConfiguration;
    MPI_Bcast((void *)conf, sizeof(CodedConfiguration), MPI_CHAR, 0, MPI_COMM_WORLD);
    conf->ParseFileDistribution();

    // RECEIVE PARTITIONS FROM MASTER and ignore
    for (unsigned int i = 1; i < conf->getNumReducer(); i++) {
        unsigned char *buff = new unsigned char[conf->getKeySize() + 1];
        MPI_Bcast(buff, conf->getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
    }

    // return 0 to master during every stage
    double rTime = 0;

    // GENERATE CODING SCHEME AND MULTICAST GROUPS
    cg = new CodeGeneration(conf->getFileNum(), conf->getNumReducer(), conf->getLoad(), rank, conf);
    genMulticastGroup();
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // EXECUTE MAP PHASE
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    if(conf->EnableNodeCombination()) {
        for(int i = 1; i <= conf->getNumMapper() - conf->getNumReducer(); i++) {
            MPI_Barrier(workerComm);
            MPI_Barrier(workerComm); 
        }
        if(conf->EnableInnerCode()) {
            for(int i = 1; i <= conf->getNumMapper() - conf->getNumReducer(); i++) {
                MPI_Barrier(workerComm);
                MPI_Barrier(workerComm); 
            }
            for(int i = 1; i <= conf->getNumMapper() - conf->getNumReducer(); i++) {
                MPI_Barrier(workerComm);
                MPI_Barrier(workerComm); 
            }
        }
    }
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // EXECUTE ENCODING PHASE
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // SHUFFLING PHASE
    routeShuffleData();

    // EXECUTE DECODING PHASE
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // REDUCE PHASE
    MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
}

void CodedRouter::routeShuffleData() {
    for (unsigned int i = 1; i <= conf->getNumMapper(); i++) {
        MPI_Barrier(workerComm);
        MPI_Barrier(workerComm);
    }
    unsigned lineSize = conf->getLineSize();
    for(unsigned int activeId = 1; activeId <= conf->getNumReducer(); activeId++) {
        MPI_Barrier(workerComm);
        const vector<NodeSet> &vset = cg->getNodeSubsetSContain(activeId);
        int reducerId = conf->getReducerId(activeId);
        for(auto nsit = vset.begin(); nsit != vset.end(); nsit++) {
            NodeSet ns = *nsit;
            SubsetSId nsid = cg->getSubsetSId(ns);
            int fileNum = cg->getCodedFileNum(ns);
            for(int i = 0; i < fileNum; i++) {
                MPI_Barrier(workerComm);
                // receive data
                unsigned long long dataSize;
                MPI_Recv(&dataSize, 1, MPI_UNSIGNED_LONG_LONG, reducerId, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                unsigned char *data = new unsigned char[dataSize * lineSize];
                MPI_Recv(data, dataSize * lineSize, MPI_UNSIGNED_CHAR, reducerId, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // receive metadata
                unsigned long long metaSize;
                MPI_Recv(&metaSize, 1, MPI_UNSIGNED_LONG_LONG, reducerId, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                unsigned char *metadata = new unsigned char[metaSize];
                MPI_Recv(metadata, metaSize, MPI_UNSIGNED_CHAR, reducerId, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                // multicast to other nodes in ns group
                for(auto it = ns.begin(); it != ns.end(); it++) {
                    int nid = *it;
                    if((unsigned int)(nid) == activeId) {
                        continue;
                    }
                    int destReducerId = conf->getReducerId(nid);
                    MPI_Send(&dataSize, 1, MPI_UNSIGNED_LONG_LONG, destReducerId, 0, MPI_COMM_WORLD);
                    MPI_Send(data, dataSize * lineSize, MPI_UNSIGNED_CHAR, destReducerId, 0, MPI_COMM_WORLD);
                    MPI_Send(&metaSize, 1, MPI_UNSIGNED_LONG_LONG, destReducerId, 0, MPI_COMM_WORLD);
                    MPI_Send(metadata, metaSize, MPI_UNSIGNED_CHAR, destReducerId, 0, MPI_COMM_WORLD);
                }
                delete []data;
                delete []metadata;
                MPI_Barrier(workerComm);
            }
        }
        MPI_Barrier(workerComm);
    }
    return ;
}

void CodedRouter::genMulticastGroup() 
{
//   map<NodeSet, SubsetSId> ssmap = cg->getSubsetSIdMap();
//   for (auto nsit = ssmap.begin(); nsit != ssmap.end(); nsit++)
//   {
//     NodeSet ns = nsit->first;
//     // SubsetSId nsid = nsit->second;
//     int color = 0;
//     MPI::Intracomm mgComm = workerComm.Split(color, rank);
//     // multicastGroupMap[nsid] = mgComm;
//   }
}
