# Multithread RD

This is a Rust implementation of the parallel memory reuse distance algorithm raised in the paper:
> Q. Niu, J. Dinan, Q. Lu and P. Sadayappan, "PARDA: A Fast Parallel Reuse Distance Analysis Algorithm," 2012 IEEE 26th International Parallel and Distributed Processing Symposium, Shanghai, China, 2012, pp. 1284-1294, doi: 10.1109/IPDPS.2012.117.

You can set the number of data elements that one thread should process and define a threshold for the reuse distance you care about in order to save your memory.

# Dependencies
1. [**stack_alg_sim**](https://github.com/dcompiler/dace/tree/main/stack_alg_sim): The various reuse distance statistic algorithm in single-thread mode.
2. [**hist_statis**](https://github.com/Blameying/hist_statics) A simple statistical histogram.
