#include <iostream>
#include <assert.h>
#include <mpi.h>
#include <iomanip>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include "Master.h"
#include "Common.h"
#include "Configuration.h"
#include "PartitionSampling.h"

using namespace std;

double decode_time = 0.0;
double transfer_time = 0.0;
double encode_total_time = 0.0;
void Master::run()
{
  std::cout << "origin cpu num: 20" << std::endl;
  std::cout << "extra cpu num: 22" << std::endl;

  std::cout << "origin memory num: 20" << std::endl;
  std::cout << "extra momory num: 22" << std::endl;
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
  // cout << rank << ": MAP     | Avg = " << setw(10) << avgTime/numWorker
  //      << "   Max = " << setw(10) << maxTime << endl;


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
  // cout << rank << ": SHUFFLE | Sum = " << setw(10) << avgTime
  //      << "   Rate = " << setw(10) << avgRate/numWorker << " Mbps" << endl;  


  // COMPUTE UNPACK TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  // cout << rank << ": UNPACK  | Avg = " << setw(10) << avgTime/numWorker
  //      << "   Max = " << setw(10) << maxTime << endl;
  
  
  
  heapSort();
  encodeAndSort();
  // assignReduceCodedJob();
  // assignReduceDupJob();
  // COMPUTE REDUCE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  // cout << rank << ": REDUCE  | Avg = " << setw(10) << avgTime/numWorker
  //      << "   Max = " << setw(10) << maxTime << endl;      
  
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
  for (auto key: encodedList2) {
    delete [] key;
  }
  gettimeofday(&total_end,NULL);
  total_time = (total_end.tv_sec*1000000.0 + total_end.tv_usec - total_start.tv_sec*1000000.0 - total_start.tv_usec) / 1000000.0;
  total_time -= transfer_time * conf.getNumReducer();
  std::cout << "total time: " << total_time << std::endl;
  std::cout << "loss: " << encode_total_time / total_time << std::endl;;
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
    MPI::COMM_WORLD.Barrier();
  }
  time /= conf.getNumReducer();
  std::cout << "encode pre time: " << time << std::endl;
  encode_total_time += time;

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
    unsigned char* key2 = new unsigned char[conf.getKeySize()];
    encodedList2.push_back(key2);
  }
  gettimeofday(&start,NULL);

  std::thread thd1([&](){
    for (int i = 0; i < conf.getNumReducer(); i++) {
      for (int j = 0; j < size; j++) {
        for (int k = 0; k < conf.getKeySize(); k++) {
          encodedList[j][k] += heaps[i][j][k];
        }
      }
  }
  });

  std::thread thd2([&](){
    for (int i = 0; i < conf.getNumReducer(); i++) {
      for (int j = 0; j < size; j++) {
        for (int k = 0; k < conf.getKeySize(); k++) {
          encodedList2[j][k] += (i + 1) * heaps[i][j][k];
        }
      }
    }
  });
  thd1.join();
  thd2.join();
  // std::cout << "encoded list:" << std::endl;
  // printLineList(encodedList);
  gettimeofday(&end,NULL);
  time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  time /= conf.encodedListNum;
  std::cout << "encode time: " << time << std::endl;
  encode_total_time += time;
  MPI::COMM_WORLD.Barrier();

  std::sort(encodedList.begin(), encodedList.end(), [&](const unsigned char* keyA, const unsigned char* keyB) {
    return cmpKey(keyA, keyB, conf.getKeySize());
  });
}


void Master::receiveAndDecode() {
  std::vector<LineList> decode_lists;
  struct timeval start, end;
  double time;
  int size = conf.getNumSamples() / conf.getNumReducer();
  unsigned char* buffer = new unsigned char[size * (conf.getKeySize() + conf.getValueSize())];
  MPI::COMM_WORLD.Barrier();

  // receive result of dual redundancy
  gettimeofday(&start,NULL);
  for (int i = 1; i <= conf.getNumReducer(); i++) {
    MPI::COMM_WORLD.Recv(buffer, size * (conf.getKeySize() + conf.getValueSize()), MPI::UNSIGNED_CHAR, i, 0);
    MPI::COMM_WORLD.Barrier();
  }
  gettimeofday(&end, NULL);
  // delete [] buffer;

  time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  time /= conf.getNumReducer();
  std::cout << "transfer time(s): " << time << std::endl;
  std::cout << "chunk size(byte): " << (double)(size * conf.getLineSize()) << std::endl;
  std::cout << "dual redundancy bandwidth(Mb/s): " << (double)(size * conf.getLineSize()) / time / 1000 / 1000 * 8 << std::endl;
  transfer_time = time;
  
  // receive result of code compution
  time = 0.0;
  for (int i = 1; i <= conf.getNumReducer(); i++) {
    gettimeofday(&start,NULL);
    MPI::COMM_WORLD.Recv(buffer, size * (conf.getKeySize() + conf.getValueSize()), MPI::UNSIGNED_CHAR, i, 0);
    MPI::COMM_WORLD.Barrier();
    gettimeofday(&end, NULL);
    time += (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
    // unpack
    LineList decode_list;
    for (int j = 0; j < size; j++) {
      unsigned char* key = new unsigned char[conf.getKeySize()];
      memcpy(key, buffer + j * (conf.getKeySize() + conf.getValueSize()), conf.getKeySize());
      decode_list.push_back(key);
    }
    decode_lists.push_back(decode_list);
    MPI::COMM_WORLD.Barrier();
  }
  delete [] buffer;

  // time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  time /= conf.getNumReducer();
  transfer_time = time;

  // for (int i = 1; i <= conf.getNumReducer(); i++) {
  //   // receive from worker[i]
  //   LineList decode_list;
  //   for (int j = 0; j < size; j++) {
  //     unsigned char* key = new unsigned char[conf.getKeySize()];
  //     MPI::COMM_WORLD.Recv(key, conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0);
  //     decode_list.push_back(key);
  //   }
  //   decode_lists.push_back(decode_list);
  //   MPI::COMM_WORLD.Barrier();
  // }



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
  std::cout << "total decode time(s): " << time + transfer_time << std::endl;
  std::cout << "chunk size(byte): " << (double)(size * conf.getLineSize()) << std::endl;
  std::cout << "code computation bandwidth(Mb/s): " << (double)(size * conf.getLineSize()) / (time + transfer_time) / 1000 / 1000 * 8 << std::endl;
  decode_time = time;

  for (auto decode_list: decode_lists) {
    for (auto key: decode_list) {
      delete [] key;
    }
  }
}

void Master::assignReduceCodedJob() {
  // struct timeval start, end;
  // double time;
  // MPI::COMM_WORLD.Barrier();
  // gettimeofday(&start, NULL);
  // for (int i = 1; i <= conf.getNumReducer(); i++) {
  //   int size = heaps[i].size();
  //   unsigned char* buffer = new unsigned char[size * conf.getKeySize()];
  //   MPI::COMM_WORLD.Send(buffer, size * conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0 );
  //   delete [] buffer;
  //   MPI::COMM_WORLD.Barrier();
  // }
  
  // int size = encodedList.size();
  // unsigned char* buffer = new unsigned char[size * conf.getKeySize()];
  // MPI::COMM_WORLD.Send(buffer, size * conf.getKeySize(), MPI::UNSIGNED_CHAR, 1, 0 );
  // delete [] buffer;
  // MPI::COMM_WORLD.Barrier();

  // size = encodedList2.size();
  // buffer = new unsigned char[size * conf.getKeySize()];
  // MPI::COMM_WORLD.Send(buffer, size * conf.getKeySize(), MPI::UNSIGNED_CHAR, 2, 0 );
  // delete [] buffer;
  // MPI::COMM_WORLD.Barrier();
  // gettimeofday(&end, NULL);
  // time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  // std::cout << "transfer coded reduce job time: " << time << std::endl;
  struct timeval start, end;
  double time;
  gettimeofday(&start, NULL);
  for (int i = 1; i <= conf.getNumReducer(); i++) {
    pid_t pid = fork();
    if (pid == 0) {
      std::string filename = "/root/MergeCDC/Input/tera10G_" + std::to_string(i - 1);
      std::string dst = "root@192.168.0." + std::to_string(i + 1) + ":/root/";
      char * args[] = {(char*)"scp", (char*)filename.c_str(), (char*)dst.c_str(), NULL};
      execvp(args[0], args);
    } else {
      wait(NULL);
    }
  }
  pid_t pid = fork();
  if (pid == 0) {
    std::string filename = "/root/MergeCDC/Input/tera10G_0";
    std::string dst = "root@192.168.0.2:/root/";
    char * args[] = {(char*)"scp", (char*)filename.c_str(), (char*)dst.c_str(), NULL};
    execvp(args[0], args);
  } else {
    wait(NULL);
  }
  pid = fork();
  if (pid == 0) {
    std::string filename = "/root/MergeCDC/Input/tera10G_1";
    std::string dst = "root@192.168.0.3:/root/";
    char * args[] = {(char*)"scp", (char*)filename.c_str(), (char*)dst.c_str(), NULL};
    execvp(args[0], args);
  } else {
    wait(NULL);
  }

  gettimeofday(&end, NULL);
  time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  std::cout << "transfer coded reduce job time: " << time << std::endl;
  MPI::COMM_WORLD.Barrier();
}

void Master::assignReduceDupJob() {
  // struct timeval start, end;
  // double time;
  // MPI::COMM_WORLD.Barrier();
  // gettimeofday(&start, NULL);
  // for (int i = 1; i <= conf.getNumReducer(); i++) {
  //   int size = heaps[i].size();
  //   unsigned char* buffer = new unsigned char[size * conf.getKeySize()];
  //   MPI::COMM_WORLD.Send(buffer, size * conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0 );
  //   delete [] buffer;
  //   MPI::COMM_WORLD.Barrier();
  // }
  
  // for (int i = 1; i <= conf.getNumReducer(); i++) {
  //   int size = heaps[i].size();
  //   unsigned char* buffer = new unsigned char[size * conf.getKeySize()];
  //   MPI::COMM_WORLD.Send(buffer, size * conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0 );
  //   delete [] buffer;
  //   MPI::COMM_WORLD.Barrier();
  // }
  // gettimeofday(&end, NULL);
  // time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  // std::cout << "transfer dup reduce job time: " << time << std::endl;

  struct timeval start, end;
  double time;
  gettimeofday(&start, NULL);
  for (int i = 1; i <= conf.getNumReducer(); i++) {
    pid_t pid = fork();
    if (pid == 0) {
      std::string filename = "/root/MergeCDC/Input/tera10G_" + std::to_string(i - 1);
      std::string dst = "root@192.168.0." + std::to_string(i + 1) + ":/root/";
      char * args[] = {(char*)"scp", (char*)filename.c_str(), (char*)dst.c_str(), NULL};
      execvp(args[0], args);
    } else {
      wait(NULL);
    }
  }

  for (int i = 1; i <= conf.getNumReducer(); i++) {
    pid_t pid = fork();
    if (pid == 0) {
      std::string filename = "/root/MergeCDC/Input/tera10G_" + std::to_string(i - 1);
      std::string dst = "root@192.168.0." + std::to_string(i + 1) + ":/root/";
      char * args[] = {(char*)"scp", (char*)filename.c_str(), (char*)dst.c_str(), NULL};
      execvp(args[0], args);
    } else {
      wait(NULL);
    }
  }
  gettimeofday(&end, NULL);
  time = (end.tv_sec*1000000.0 + end.tv_usec - start.tv_sec*1000000.0 - start.tv_usec) / 1000000.0;
  std::cout << "transfer dup reduce job time: " << time << std::endl;
  MPI::COMM_WORLD.Barrier();
} 
