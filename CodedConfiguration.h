#ifndef _CMR_CONFIGURATION
#define _CMR_CONFIGURATION

#include "Configuration.h"

class CodedConfiguration : public Configuration {

 private:
  unsigned int load;

 public:
 CodedConfiguration(): Configuration() {
    numInput = 10;    // N is assumed to be K choose r     not use
    numReducer = 5;  // K
    load = 3;        // r  

    // strcpy(inputPath, "./Input/tera10G");
    // strcpy(outputPath, "./Output/tera10G-c");
    // strcpy(partitionPath, "./Partition/tera10G");
    // numSamples = 100000000;
    strcpy(inputPath, "./Input/Input10000");
    strcpy(outputPath, "./Output/Output10000");
    strcpy(partitionPath, "./Partition/Output10000");
    numSamples = 10000;
    // strcpy(inputPath, "./Input/Input10000000");
    // strcpy(outputPath, "./Output/Input10000000-c");
    // strcpy(partitionPath, "./Partition/Input10000000");
    // numSamples = 10000000;
  }
  ~CodedConfiguration() {}

  unsigned int getLoad() const { return load; }
};

#endif