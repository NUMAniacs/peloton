//===----------------------------------------------------------------------===//
//  Test the read & write cost of memory in different nodes
//  
//  single thread
//
//===----------------------------------------------------------------------===//

#include <numa.h>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <sched.h>
#include <random>

const int K = 1; // iteration times
const int N = 1024 * 1024 * 128; // 512 MB
const int rand_mask = N - 1;
// const int N = 100000000;

// fast pseudo-random number generator
static unsigned long x=123456789, y=362436069, z=521288629;
unsigned long xorshf96(void) {          //period 2^96-1
unsigned long t;
    x ^= x << 16;
    x ^= x >> 5;
    x ^= x << 1;

   t = x;
   x = y;
   y = z;
   z = t ^ x ^ y;

  return z;
}

int main()
{
    // pin the current thread to node 0
    numa_run_on_node(0);

    int* local = (int*) numa_alloc_onnode(sizeof(int) * N, 0); // 512 MB
    int* remote = (int*) numa_alloc_onnode(sizeof(int) * N, 1);
    clock_t start = clock();
    for (int k = 0; k < K; ++k) {
        for (int i = 0; i < N; ++i) {
            // ++local[i];
            // ++local[rand() % N];
            ++local[xorshf96() & rand_mask];
        }
    }
    clock_t end = clock();
    double time = (double) (end-start) / CLOCKS_PER_SEC * 1000.0;   

    printf("cost of local: %lf\n", time);

    start = clock();
    for (int k = 0; k < K; ++k) {
        for (int i = 0; i < N; ++i) {
            // ++remote[i];
            // ++remote[rand() % N];
            ++remote[xorshf96() & rand_mask];
        }
    }
    end = clock();
    time = (double) (end-start) / CLOCKS_PER_SEC * 1000.0;  
    
    printf("cost of remote: %lf\n", time);

    numa_free(local, sizeof(int) * N);
    numa_free(remote, sizeof(int) * N);
    return 0;
}

// kling1@node2:~/micro_benchmark$ g++ numa_alloc_on_nodes.cpp -lnuma -O3 -std=c++11 -o alloc_node
// kling1@node2:~/micro_benchmark$ ./alloc_node
// cost of local: 1506.956000
// cost of remote: 3692.248000