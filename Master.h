#ifndef _MR_MASTER
#define _MR_MASTER

#include "Configuration.h"

class Master
{
 private:
  Configuration conf;
  unsigned int rank;
  unsigned int totalNode;
  std::vector<LineList> heaps;
  LineList encodedList;
  LineList encodedList2;

 public:
 Master( unsigned int _rank, unsigned int _totalNode ): rank( _rank ), totalNode( _totalNode ) {};
  ~Master() {};

  void run();
  void heapSort();
  void printLineList(LineList list);
  void encodeAndSort();
  void receiveAndDecode();
  void assignReduceCodedJob();
  void assignReduceDupJob();
};

#endif
