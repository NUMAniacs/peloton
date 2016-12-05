//===----------------------------------------------------------------------===//
//  Test the read & write cost of memory in different nodes
//  
//  multi threads
//
//===----------------------------------------------------------------------===//

#include <numa.h>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <sched.h>
#include <pthread.h>
#include <unistd.h>
#include <atomic>

// const int ITERATION = 1; // iteration times
const int N = 1024 * 1024 * 128; // 512 MB
const int rand_mask = N - 1;
// const int N = 100000000;
const int COUNT_THRESHOLD = 1000; // 1 in global_count stands for COUNT_THRESHOLD number of counts

static unsigned int random_seeds[24];

int *array[2]; // array[0] is on node0, array[1] is on node1

volatile bool start_count = false;
volatile bool terminated = false;

std::atomic<int> global_count(0);

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

// threads read from local node
void *local_work(void *cpuID) {
    pthread_t thread;
    thread = pthread_self();

    long c_ID = (long)cpuID;
    // set thread affinity
    cpu_set_t mask;
    int status;
    CPU_ZERO(&mask);
    CPU_SET(c_ID, &mask);
    status = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &mask);
    // status = sched_setaffinity(0, sizeof(mask), &mask); // also works

    int array_index = c_ID % 2; // thread reads from local node

    int tmp = 0;
    long long local_count = 0;

    int k = 0;

    while(!terminated) {
        for (int i = 0; i < N; ++i) {
            if (terminated) {
                break;
            }
            // tmp += array[array_index][i];
            // tmp += array[array_index][rand() % N];
            tmp += array[array_index][xorshf96() & rand_mask];
            // tmp += array[array_index][rand_r(&random_seeds[c_ID]) & rand_mask];
            if (start_count) {
                ++k;
                if (k == COUNT_THRESHOLD) {
                    ++local_count;
                    k = 0;
                }
            }
        }
    }
    printf("CPU ID: %d, local_count: %d, tmp: %d\n", c_ID, local_count, tmp);
    global_count += local_count;
}

// threads read from remote node
void *remote_work(void *cpuID) {
    pthread_t thread;
    thread = pthread_self();

    long c_ID = (long)cpuID;
    // set thread affinity
    cpu_set_t mask;
    int status;
    CPU_ZERO(&mask);
    CPU_SET(c_ID, &mask);
    status = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &mask);

    int array_index = 1 - (c_ID % 2); // thread reads from remote node

    int tmp = 0;
    long long local_count = 0;

    int k = 0;

    while(!terminated) {
        for (int i = 0; i < N; ++i) {
            if (terminated) {
                break;
            }
            // tmp += array[array_index][i];
            // tmp += array[array_index][rand() % N];
            tmp += array[array_index][xorshf96() % N];
            // tmp += array[array_index][rand_r(&random_seeds[c_ID]) & rand_mask];
            if (start_count) {
                ++k;
                if (k == COUNT_THRESHOLD) {
                    ++local_count;
                    k = 0;
                }
            }
        }
    }
    printf("CPU ID: %d, local_count: %d, tmp: %d\n", c_ID, local_count, tmp);
    global_count += local_count;
}

void initialize() {
    array[0] = (int*) numa_alloc_onnode(sizeof(int) * N, 0);
    array[1] = (int*) numa_alloc_onnode(sizeof(int) * N, 1);
}

void clean_up() {
    numa_free(array[0], sizeof(int) * N);
    numa_free(array[1], sizeof(int) * N);
}

int main()
{
    initialize();
    srand(time(NULL));
    for (int i = 0; i < N; ++i) {
        array[0][i] = i;
        array[1][i] = i;
        // array[0][i] = rand();
        // array[0][i] = rand();
    }

    terminated = false;
    start_count = false;
    printf("start\n");
    const int NUM_THREADS = 24;
    pthread_t threads[NUM_THREADS];

    // assign different random seeds for threads used in rand_r() function
    for (int i = 0; i < NUM_THREADS; ++i) {
        random_seeds[i] = rand();
    }

    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_create(&threads[i], NULL, remote_work, (void *)i);
    }

    // warm up
    sleep(1);

    // start counting
    time_t start = time(0);
    start_count = true;
    sleep(10);
    start_count = false;
    time_t end = time(0);
    double time = difftime(end, start) * 1000.0;

    terminated = true;

    for (int i = 0; i < NUM_THREADS; ++i) {
        // wait for undetached threads
        pthread_join(threads[i], NULL);
    }

    printf("time: %lf, total count: %d, ratio: %lf\n", time, (int)global_count, 1.0 * global_count / time);

    clean_up();

    return 0;
}

// // using xorshf96()
// // thread running local_work() // read memory from local node
// // 1 thread
// time: 10000.000000, total count: 756552, ratio: 75.655200
// // 2 threads
// time: 10000.000000, total count: 1563416, ratio: 156.341600
// // 12 threads
// time: 10000.000000, total count: 8599710, ratio: 859.971000
// // 14 threads
// time: 10000.000000, total count: 10005989, ratio: 1000.598900
// // 18 threads
// time: 10000.000000, total count: 12575281, ratio: 1257.528100
// // 23 threads
// time: 10000.000000, total count: 11570858, ratio: 1157.085800
// // 24 threads
// time: 10000.000000, total count: 9322305, ratio: 932.230500

// // thread running remote_work() // read memory from remote note
// // 1 thread
// time: 10000.000000, total count: 426001, ratio: 42.600100
// // 2 threads
// time: 10000.000000, total count: 845079, ratio: 84.507900
// // 12 threads
// time: 10000.000000, total count: 4753722, ratio: 475.372200
// // 14 threads
// time: 10000.000000, total count: 5473118, ratio: 547.311800
// // 18 threads
// time: 10000.000000, total count: 7159255, ratio: 715.925500
// // 23 threads
// time: 10000.000000, total count: 5880617, ratio: 588.061700
// // 24 threads
// time: 10000.000000, total count: 6093335, ratio: 609.333500