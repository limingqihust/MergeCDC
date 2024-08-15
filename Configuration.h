#ifndef _MR_CONFIGURATION
#define _MR_CONFIGURATION

#include <string.h>
#include "Common.h"
#include "Utility.h"
#include <algorithm>
#include <sys/time.h>
class Configuration {

 protected:
  unsigned int numReducer;
  unsigned int numInput;  
  
  char inputPath[1024];
  char outputPath[1024];
  char partitionPath[1024];
  unsigned long numSamples;
  
 public:
  int encodedListNum = 2;;
  Configuration() {
    numReducer = 20;
    numInput = numReducer;    
    
    strcpy(inputPath, "./Input/Input10000");
    strcpy(outputPath, "./Output/Output10000");
    strcpy(partitionPath, "./Partition/Partition10000");
    // strcpy(inputPath, "/root/MergeCDC/Input/tera10G");
    // strcpy(outputPath, "/root/MergeCDC/Output/tera10G");
    // strcpy(partitionPath, "/root/MergeCDC/Partition/tera10G");
    numSamples = 10000;
    // numSamples = 100000000;
  }
  ~Configuration() {}
  const static unsigned int KEY_SIZE = 10;
  const static unsigned int VALUE_SIZE = 90;  
  const static unsigned int WORD_SIZE = 8;
  const unsigned int getWordSize() const { return WORD_SIZE; }
  unsigned int getNumReducer() const { return numReducer; }
  unsigned int getNumInput() const { return numInput; }  
  const char *getInputPath() const { return inputPath; }
  const char *getOutputPath() const { return outputPath; }
  const char *getPartitionPath() const { return partitionPath; }
  unsigned int getKeySize() const { return KEY_SIZE; }
  unsigned int getValueSize() const { return VALUE_SIZE; }
  unsigned int getLineSize() const { return KEY_SIZE + VALUE_SIZE; }
  unsigned long getNumSamples() const { return numSamples; }  
};

#endif
