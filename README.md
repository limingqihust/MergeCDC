# MergeCDC
# for node-merging
RUN `cd nodeComb; g++ -o nodeComb *.cc; ./nodeComb` to get the node-merging solution
And you can config the parameters of node-merging in `./nodeComb/main`

# for MergeCDC
Run `make` to compile `MergeCDC`.

Run `./Splitter` to split the input data points.

Run `mpirun -np ${NodeNum} -hostfile ./hostfile/hostfile-r ./CodedTeraSort`.

The above execution creates ${NodeNum} computing processes and 1 master process to sorts data according to CodedTeraSort algorithm.
And you can config the parameters of node-merging in `./Configuration.h` and `./CodedConfiguration.h`