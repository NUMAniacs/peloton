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

const int MAX_THREADS = 40;

static unsigned int random_seeds[MAX_THREADS];

int *array[2]; // array[0] is on node0, array[1] is on node1

volatile bool start_count = false;
volatile bool terminated = false;

int THREADS_ID[] = {0, 10, 1, 11, 2, 12, 3, 13, 4, 14, 5, 15, 6, 16, 7, 17, 8, 18, 9, 19, 80, 90, 81, 91, 82, 92, 83, 93, 84, 94, 85, 95, 86, 96, 87, 97, 88, 98, 89, 99};



std::atomic<int> global_count(0);

// fast pseudo-random number generator
// static unsigned long x=123456789, y=362436069, z=521288629;
// unsigned long xorshf96(void) {          //period 2^96-1
// unsigned long t;
//     x ^= x << 16;
//     x ^= x >> 5;
//     x ^= x << 1;

//    t = x;
//    x = y;
//    y = z;
//    z = t ^ x ^ y;

//   return z;
// }

unsigned long xorshf96(unsigned long &x, unsigned long &y, unsigned long &z) {          //period 2^96-1
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
void *local_work(void *parm) {
    pthread_t thread;
    thread = pthread_self();

    long index = (long)parm;
    long cpuID = THREADS_ID[index];
    unsigned long x = rand_r(&random_seeds[index]);
    unsigned long y = rand_r(&random_seeds[index]);
    unsigned long z = rand_r(&random_seeds[index]);

    // set thread affinity
    cpu_set_t mask;
    int status;
    CPU_ZERO(&mask);
    CPU_SET(cpuID, &mask);
    status = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &mask);
    // status = sched_setaffinity(0, sizeof(mask), &mask); // also works

    int array_index = (cpuID / 10) % 8; // thread reads from local node

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
            tmp += array[array_index][xorshf96(x, y, z) & rand_mask];
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
    printf("CPU ID: %d, local_count: %d, tmp: %d\n", cpuID, local_count, tmp);
    global_count += local_count;
}

// threads read from remote node
void *remote_work(void *parm) {
    pthread_t thread;
    thread = pthread_self();

    long index = (long)parm;
    long cpuID = THREADS_ID[index];
    unsigned long x = rand_r(&random_seeds[index]);
    unsigned long y = rand_r(&random_seeds[index]);
    unsigned long z = rand_r(&random_seeds[index]);

    // set thread affinity
    cpu_set_t mask;
    int status;
    CPU_ZERO(&mask);
    CPU_SET(cpuID, &mask);
    status = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &mask);

    int array_index = 1 - ((cpuID / 10) % 8); // thread reads from remote node

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
            tmp += array[array_index][xorshf96(x, y, z) % N];
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
    printf("CPU ID: %d, local_count: %d, tmp: %d\n", cpuID, local_count, tmp);
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

    for (int NUM_THREADS = 1; NUM_THREADS <= MAX_THREADS; NUM_THREADS++) {
        global_count = 0;
        terminated = false;
        start_count = false;
        printf("Start with number of threads: %d\n", NUM_THREADS);
        // const int NUM_THREADS = 24;
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
    }

    

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