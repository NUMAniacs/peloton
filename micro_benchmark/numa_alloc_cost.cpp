//===----------------------------------------------------------------------===//
//
//  Test the performance of malloc VS numa_alloc
//
//===----------------------------------------------------------------------===//

#include <numa.h>
#include <ctime>
#include <cstdio>
#include <cstdlib>

int main(int argc, char const *argv[])
{
    const int N = 1024;
    const int K = 1024 * 1024;
    int **malloc_array = new int*[K]; // each array has K elements, and each element in the array is 4KB
    int **numa_alloc_array = new int*[K]; // each array is 4GB

    // pin the current thread to node 0
    numa_run_on_node(0);

    // need to touch the allocated memory

    // ==== test allocation time of malloc ====
    int tmp = 0;

    clock_t start = clock();
    for (int i = 0; i < K; ++i) {
        malloc_array[i] = (int*)malloc(sizeof(int) * N);
        
        // touch the allocated memory
        malloc_array[i][0] = -1;

        // read from a different position
        // tmp += malloc_array[i][1];
    }
    clock_t end = clock();
    double time = (double) (end-start) / CLOCKS_PER_SEC * 1000.0;   
    printf("cost of malloc: %lf\n", time);

    // ==== test allocation time of numa_alloc ====
    start = clock();
    for (int i = 0; i < K; ++i) {
        numa_alloc_array[i] = (int*)numa_alloc_onnode(sizeof(int) * N, 0);
        
        // touch the allocated memory
        numa_alloc_array[i][0] = -1;

        // read from a different position
        // tmp += numa_alloc_array[i][1];
    }
    end = clock();
    time = (double) (end-start) / CLOCKS_PER_SEC * 1000.0;  
    printf("cost of numa_alloc: %lf\n", time);  

    // ==== test free time of free ====
    start = clock();
    for (int i = 0; i < K; ++i) {
        free(malloc_array[i]);
    }
    end = clock();
    time = (double) (end-start) / CLOCKS_PER_SEC * 1000.0;  
    printf("cost of free: %lf\n", time);

    // ==== test free time of numa_free ====
    start = clock();
    for (int i = 0; i < K; ++i) {
        numa_free(numa_alloc_array[i], sizeof(int) * N);
    }
    end = clock();
    time = (double) (end-start) / CLOCKS_PER_SEC * 1000.0;  
    printf("cost of numa_free: %lf\n", time);
    printf("tmp: %d\n", tmp);

    delete [] malloc_array;
    delete [] numa_alloc_array;
    return 0;
}

// other findings:
// if we do allocation and free in interleaving way, malloc will be significantly faster than numa_alloc
// for it keeps a free list

// kling1@node2:~/micro_benchmark$ g++ numa_alloc_cost.cpp -O3 -lnuma -o alloc_cost
// kling1@node2:~/micro_benchmark$ ./alloc_cost
// cost of malloc: 1662.470000
// cost of numa_alloc: 3089.130000
// cost of free: 986.107000
// cost of numa_free: 1097.767000
// tmp: 0