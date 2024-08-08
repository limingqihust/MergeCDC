#include <iostream>
#include <mpi.h>
#include <iomanip>
#include <fstream>
#include <cstdio>
#include <map>
#include <assert.h>
#include <algorithm>
#include <ctime>
#include <cstring>

#include "Worker.h"
#include "Configuration.h"
#include "Common.h"
#include "Utility.h"

using namespace std;

Worker::~Worker()
{
  delete conf;
  for ( auto it = partitionList.begin(); it != partitionList.end(); ++it ) {
    delete [] *it;
  }

  LineList* ll = partitionCollection[ rank - 1 ];
  for( auto lit = ll->begin(); lit != ll->end(); lit++ ) {
    delete [] *lit;
  }
  delete ll;
  
  for ( auto it = localList.begin(); it != localList.end(); ++it ) {
     delete [] *it;
  }

  delete trie;
}

void Worker::run()
{
  // RECEIVE CONFIGURATION FROM MASTER
  conf = new Configuration;
  MPI::COMM_WORLD.Bcast( (void*) conf, sizeof( Configuration ), MPI::CHAR, 0 );


  // RECEIVE PARTITIONS FROM MASTER
  for ( unsigned int i = 1; i < conf->getNumReducer(); i++ ) {
    unsigned char* buff = new unsigned char[ conf->getKeySize() + 1 ];
    MPI::COMM_WORLD.Bcast( buff, conf->getKeySize() + 1, MPI::UNSIGNED_CHAR, 0 );
    partitionList.push_back( buff );
  }


  // EXECUTE MAP PHASE
  clock_t time;
  struct timeval start, end;
  double rTime;
  execMap();

  
  // SHUFFLING PHASE
  unsigned int lineSize = conf->getLineSize();
  for ( unsigned int i = 1; i <= conf->getNumReducer(); i++ ) {
    if ( i == rank ) {
      clock_t txTime = 0;
      unsigned long long tolSize = 0;
      MPI::COMM_WORLD.Barrier();
      time = clock();  	            
      // Sending from node i
      gettimeofday( &start, NULL );
      for ( unsigned int j = 1; j <= conf->getNumReducer(); j++ ) {
	if ( j == i ) {
	  continue;
	}
	TxData& txData = partitionTxData[ j - 1 ];
	txTime -= clock();
	MPI::COMM_WORLD.Send( &( txData.numLine ), 1, MPI::UNSIGNED_LONG_LONG, j, 0 );
	MPI::COMM_WORLD.Send( txData.data, txData.numLine * lineSize, MPI::UNSIGNED_CHAR, j, 0 );
	txTime += clock();
	tolSize += txData.numLine * lineSize + sizeof(unsigned long long);
	delete [] txData.data;
      }
      MPI::COMM_WORLD.Barrier();
      time = clock() - time;
      rTime = double( time ) / CLOCKS_PER_SEC;        
      double txRate = ( tolSize * 8 * 1e-6 ) / ( double( txTime ) / CLOCKS_PER_SEC );
      gettimeofday( &end, NULL );
      rTime = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
      MPI::COMM_WORLD.Send( &rTime, 1, MPI::DOUBLE, 0, 0 );
      MPI::COMM_WORLD.Send( &txRate, 1, MPI::DOUBLE, 0, 0 );
      //cout << rank << ": Avg sending rate is " << ( tolSize * 8 ) / ( rtxTime * 1e6 ) << " Mbps, Data size is " << tolSize / 1e6 << " MByte\n";
    }
    else {
      MPI::COMM_WORLD.Barrier();
      // Receiving from node i
      TxData& rxData = partitionRxData[ i - 1 ];
      MPI::COMM_WORLD.Recv( &( rxData.numLine ), 1, MPI::UNSIGNED_LONG_LONG, i, 0 );
      rxData.data = new unsigned char[ rxData.numLine * lineSize ];
      MPI::COMM_WORLD.Recv( rxData.data, rxData.numLine*lineSize, MPI::UNSIGNED_CHAR, i, 0 );
      MPI::COMM_WORLD.Barrier();
    }
  }


  // UNPACK PHASE
  gettimeofday( &start, NULL );
  time = -clock();
  // append local partition to localList
  for ( auto it = partitionCollection[ rank - 1 ]->begin(); it != partitionCollection[ rank - 1 ]->end(); ++it ) {
    unsigned char* buff = new unsigned char[ conf->getLineSize() ];
    memcpy( buff, *it, conf->getLineSize() );
    localList.push_back( buff );
  }

  // append data from other workers
  for( unsigned int i = 1; i <= conf->getNumReducer(); i++ ) {
    if( i == rank ) {
      continue;
    }
    TxData& rxData = partitionRxData[ i - 1 ];
    for( unsigned long long lc = 0; lc < rxData.numLine; lc++ ) {
      unsigned char* buff = new unsigned char[ lineSize ];
      memcpy( buff, rxData.data + lc*lineSize, lineSize );
      localList.push_back( buff );
    }
    delete [] rxData.data;
  }
  time += clock();
  rTime = double( time ) / CLOCKS_PER_SEC;    
  gettimeofday( &end, NULL );
  rTime = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );      

  heapSort();
  // receiveReduceCodedJob();
  // receiveReduceDupJob();
  MPI::COMM_WORLD.Barrier();
  MPI::COMM_WORLD.Barrier();
  // REDUCE PHASE
  gettimeofday( &start, NULL );
  time = clock();  
  execReduce();
  time = clock() - time;
  rTime = double( time ) / CLOCKS_PER_SEC;    
  gettimeofday( &end, NULL );
  rTime = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );    
  sendDecodedList();
  outputLocalList();
  //printLocalList();
}


void Worker::execMap()
{
  clock_t time = 0;
  double rTime = 0;
  struct timeval start, end;
  gettimeofday( &start, NULL );
  time -= clock();
  
  // READ INPUT FILE AND PARTITION DATA
  char filePath[ MAX_FILE_PATH ];
  sprintf( filePath, "%s_%d", conf->getInputPath(), rank - 1 );
  ifstream inputFile( filePath, ios::in | ios::binary | ios::ate );
  if ( !inputFile.is_open() ) {
    cout << rank << ": Cannot open input file " << conf->getInputPath() << endl;
    assert( false );
  }

  int fileSize = inputFile.tellg();
  unsigned long int lineSize = conf->getLineSize();
  unsigned long int numLine = fileSize / lineSize;
  inputFile.seekg( 0, ios::beg );

  // Build trie
  unsigned char prefix[ conf->getKeySize() ];
  trie = buildTrie( &partitionList, 0, partitionList.size(), prefix, 0, 2 );

  // Create lists of lines
  for ( unsigned int i = 0; i < conf->getNumReducer(); i++ ) {
    partitionCollection.insert( pair< unsigned int, LineList* >( i, new LineList ) );
  }

  // MAP
  // Put each line to associated collection according to partition list
  for ( unsigned long i = 0; i < numLine; i++ ) {
    unsigned char* buff = new unsigned char[ lineSize ];
    inputFile.read( ( char * ) buff, lineSize );
    unsigned int wid = trie->findPartition( buff );
    partitionCollection.at( wid )->push_back( buff );
  }
  inputFile.close();  
  gettimeofday( &end, NULL );
  rTime = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI::COMM_WORLD.Gather(&rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );    


  time = -clock();
  gettimeofday( &start, NULL );
  // Packet partitioned data to a chunk
  for( unsigned int i = 0; i < conf->getNumReducer(); i++ ) {
    if( i == rank - 1 ) {
      continue;
    }
    unsigned long long numLine = partitionCollection[ i ]->size();
    partitionTxData[ i ].data = new unsigned char[ numLine * lineSize ];
    partitionTxData[ i ].numLine = numLine;
    auto lit = partitionCollection[ i ]->begin();
    for( unsigned long long j = 0; j < numLine * lineSize; j += lineSize ) {
      memcpy( partitionTxData[ i ].data + j, *lit, lineSize );
      delete [] *lit;
      lit++;
    }
    delete partitionCollection[ i ];
  }
  time += clock();
  rTime = double( time ) / CLOCKS_PER_SEC;
  gettimeofday( &end, NULL );
  rTime = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );    
}


void Worker::execReduce()
{
  // if( rank == 1) {
  //   cout << rank << ":Sort " << localList.size() << " lines\n";
  // }
  // stable_sort( localList.begin(), localList.end(), Sorter( conf->getKeySize() ) );
  sort( localList.begin(), localList.end(), Sorter( conf->getKeySize() ) );
}


void Worker::printLocalList()
{
  unsigned long int i = 0;
  for ( auto it = localList.begin(); it != localList.end(); ++it ) {
    cout << rank << ": " << i++ << "| ";
    printKey( *it, conf->getKeySize() );
    cout << endl;
  }
}


void Worker::printPartitionCollection()
{
  for ( auto it = partitionCollection.begin(); it != partitionCollection.end(); ++it ) {
    unsigned int c = it->first;
    LineList* list = it->second;
    unsigned long i = 0;
    for ( auto lit = list->begin(); lit != list->end(); ++lit ) {
      cout << rank << ": " << c << "| " << i++ << "| ";
      printKey( *lit, conf->getKeySize() );
      cout << endl;
    }
  }
}


void Worker::outputLocalList()
{
  char buff[ MAX_FILE_PATH ];
  sprintf( buff, "%s_%u", conf->getOutputPath(), rank - 1 );
  ofstream outputFile( buff, ios::out | ios::binary | ios::trunc );
  for ( auto it = localList.begin(); it != localList.end(); ++it ) {
    outputFile.write( ( char* ) *it, conf->getLineSize() );
  }
  outputFile.close();
  //cout << rank << ": outputFile " << buff << " is saved.\n";
}


TrieNode* Worker::buildTrie( PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize, int maxDepth )
{
  if ( prefixSize >= maxDepth || lower == upper ) {
    return new LeafTrieNode( prefixSize, partitionList, lower, upper );
  }
  InnerTrieNode* result = new InnerTrieNode( prefixSize );
  int curr = lower;
  for ( unsigned char ch = 0; ch < 255; ch++ ) {
    prefix[ prefixSize ] = ch;
    lower = curr;
    while( curr < upper ) {
      if( cmpKey( prefix, partitionList->at( curr ), prefixSize + 1 ) ) {
	break;
      }
      curr++;
    }
    result->setChild( ch, buildTrie( partitionList, lower, curr, prefix, prefixSize + 1, maxDepth ) );
  }
  prefix[ prefixSize ] = 255;
  result->setChild( 255, buildTrie( partitionList, curr, upper, prefix, prefixSize + 1, maxDepth ) );
  return result;
}

void Worker::heapSort() {
  double make_heap_time;
  struct timeval make_heap_start, make_heap_end;
  gettimeofday(&make_heap_start, NULL);
  // copy localList to lists
  LineList heap;
  for (auto it = localList.begin(); it != localList.end(); it++) {
    unsigned char* key = new unsigned char[10];
    memcpy(key, *it, conf->getKeySize());
    heap.push_back(key);
  }

  std::make_heap(heap.begin(), heap.end(), [&](const unsigned char* keyA, const unsigned char* keyB) {
    return !cmpKey(keyA, keyB, conf->getKeySize());
  });
  gettimeofday(&make_heap_end, NULL);
  make_heap_time = (make_heap_end.tv_sec*1000000.0 + make_heap_end.tv_usec - make_heap_start.tv_sec*1000000.0 - make_heap_start.tv_usec) / 1000000.0;
  // keys need to send to master
  int size = conf->getNumSamples() / conf->getNumReducer();

  for (int i = 1; i <= conf->getNumReducer(); i++) {
    if (rank == i) {
      struct timeval start, end;
      double time;
      gettimeofday(&start,NULL);
      std::vector<unsigned char*> key_buffer;
      // std::cout << "rank: " << rank << " send to master" << std::endl;
      for (int i = 0; i < size; i++) {
        std::pop_heap(heap.begin(), heap.end(), [&](const unsigned char* keyA, const unsigned char* keyB) {
          return !cmpKey(keyA, keyB, conf->getKeySize());
        }); 
        unsigned char* key = heap.back(); 
        heap.pop_back(); 
        key_buffer.push_back(key);
      }
      // std::cout << "rank: " << rank << " send to master done" << std::endl;
      gettimeofday(&end,NULL);
      for (auto key: key_buffer) {
        MPI::COMM_WORLD.Send(key, conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0 );
        delete [] key;
      }
      time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
      time += make_heap_time;
      MPI::COMM_WORLD.Send(&time, 1, MPI::DOUBLE, 0, 0);
      MPI::COMM_WORLD.Barrier();
    } else {
      MPI::COMM_WORLD.Barrier();
    }
  }
  MPI::COMM_WORLD.Barrier();

}


void Worker::printLineList(LineList list)
{
  unsigned long int i = 0;
  for ( auto it = list.begin(); it != list.end(); ++it ) {
    cout << rank << ": " << i++ << "| ";
    printKey( *it, conf->getKeySize() );
    cout << endl;
  }
}

void Worker::sendDecodedList() {
  int fail_index = 15;
  for (int i = 1; i <= conf->getNumReducer(); i++) {
    if (i == fail_index || i != rank) {
      MPI::COMM_WORLD.Barrier();
      continue;
    }
    int size = conf->getNumSamples() / conf->getNumReducer();
    for (int i = 0; i < size; i++) {
      unsigned char* key = localList[i];
      MPI::COMM_WORLD.Send(key, conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0 );
    }
    MPI::COMM_WORLD.Barrier();
  }
}

void Worker::receiveReduceCodedJob() {
  MPI::COMM_WORLD.Barrier();
  int size = conf->getNumSamples() / conf->getNumReducer();
  for (int i = 1; i <= conf->getNumReducer(); i++) {
    if (i == rank) {
      unsigned char* buffer = new unsigned char[size * conf->getKeySize()];
      MPI::COMM_WORLD.Recv(buffer, size * conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0);
      delete [] buffer;
    }
    MPI::COMM_WORLD.Barrier();
  }

  if (rank == 1) {
    unsigned char* buffer = new unsigned char[size * conf->getKeySize()];
    MPI::COMM_WORLD.Recv(buffer, size * conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0);
    delete [] buffer;
  }
  MPI::COMM_WORLD.Barrier();

  if (rank == 2) {
    unsigned char* buffer = new unsigned char[size * conf->getKeySize()];
    MPI::COMM_WORLD.Recv(buffer, size * conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0);
    delete [] buffer;
  }
  MPI::COMM_WORLD.Barrier();

}

void Worker::receiveReduceDupJob() {
  MPI::COMM_WORLD.Barrier();
  int size = conf->getNumSamples() / conf->getNumReducer();
  for (int i = 1; i <= conf->getNumReducer(); i++) {
    if (i == rank) {
      unsigned char* buffer = new unsigned char[size * conf->getKeySize()];
      MPI::COMM_WORLD.Recv(buffer, size * conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0);
      delete [] buffer;
    }
    MPI::COMM_WORLD.Barrier();
  }

  for (int i = 1; i <= conf->getNumReducer(); i++) {
    if (i == rank) {
      unsigned char* buffer = new unsigned char[size * conf->getKeySize()];
      MPI::COMM_WORLD.Recv(buffer, size * conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0);
      delete [] buffer;
    }
    MPI::COMM_WORLD.Barrier();
  }
}
