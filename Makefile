CC = mpicxx
CFLAGS = -std=c++11 -Wall
DFLAGS = -std=c++11 -Wall -ggdb

all: TeraSort CodedTeraSort Splitter

clean:
	rm -f *.o
	rm -f TeraSort CodedTeraSort Splitter Codegen BroadcastTest 

cleanclean: clean
	rm -f ./Input/*_*
	rm -f ./Output/*_*
	rm -f ./Tmp/*
	rm -f *.*~
	rm -f *~




TeraSort: main.o Master.o Worker.o Trie.o Utility.o PartitionSampling.o 
	$(CC) $(CFLAGS) -g -w -o TeraSort main.o Master.o Worker.o Trie.o Utility.o PartitionSampling.o

CodedTeraSort: CodedMain.o CodedMaster.o CodedWorker.o CodedRouter.o Trie.o Utility.o PartitionSampling.o CodeGeneration.o
	$(CC) $(CFLAGS) -g -w -o CodedTeraSort CodedMain.o CodedMaster.o CodedWorker.o CodedRouter.o Trie.o Utility.o PartitionSampling.o CodeGeneration.o

Splitter: InputSplitter.o Configuration.h CodedConfiguration.h
	$(CC) $(CFLAGS) -g -w -o Splitter Splitter.cc InputSplitter.o



Trie.o: Trie.cc Trie.h
	$(CC) $(CFLAGS) -g -w -c Trie.cc

PartitionSampling.o: PartitionSampling.cc PartitionSampling.h Configuration.h
	$(CC) $(CFLAGS) -g -w -c PartitionSampling.cc

Utility.o: Utility.cc Utility.h
	$(CC) $(CFLAGS) -g -w -c Utility.cc

InputSplitter.o: InputSplitter.cc InputSplitter.h Configuration.h CodedConfiguration.h
	$(CC) $(CFLAGS) -g -w -c InputSplitter.cc

CodeGeneration.o: CodeGeneration.cc CodeGeneration.h
	$(CC) $(CFLAGS) -g -w -c CodeGeneration.cc



main.o: main.cc Configuration.h
	$(CC) $(CFLAGS) -g -w -c main.cc

Master.o: Master.cc Master.h Configuration.h
	$(CC) $(CFLAGS) -g -w -c Master.cc

Worker.o: Worker.cc Worker.h Configuration.h
	$(CC) $(CFLAGS) -g -w -c Worker.cc




CodedMain.o: CodedMain.cc CodedConfiguration.h
	$(CC) $(CFLAGS) -g -w -c CodedMain.cc

CodedMaster.o: CodedMaster.cc CodedMaster.h CodedConfiguration.h
	$(CC) $(CFLAGS) -g -w -c CodedMaster.cc

CodedWorker.o: CodedWorker.cc CodedWorker.h CodedConfiguration.h
	$(CC) $(CFLAGS) -g -w -c CodedWorker.cc
CodedRouter.o: CodedRouter.cc CodedRouter.h CodedConfiguration.h
	$(CC) $(CFLAGS) -g -w -c CodedRouter.cc
