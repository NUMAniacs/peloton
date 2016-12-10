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
#include <thread>
#include <vector>
#include <assert.h>
#include <map>

// const int ITERATION = 1; // iteration times
const int N = 1024 * 1024 * 128; // 512 MB
const int rand_mask = N - 1;
// const int N = 100000000;
const int COUNT_THRESHOLD = 1000; // 1 in global_count stands for COUNT_THRESHOLD number of counts

const int MAX_THREADS = 40;

static unsigned int random_seeds[MAX_THREADS];

int **array; // array[0] is on node0, array[1] is on node1
int num_regions;

volatile bool start_count = false;
volatile bool terminated = false;

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

unsigned long xorshf96(unsigned long &x, unsigned long &y, unsigned long &z) { //period 2^96-1
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
  int index = (long)parm;
  unsigned long x = rand_r(&random_seeds[index]);
  unsigned long y = rand_r(&random_seeds[index]);
  unsigned long z = rand_r(&random_seeds[index]);

  // status = sched_setaffinity(0, sizeof(mask), &mask); // also works
  int array_index = numa_node_of_cpu(sched_getcpu()); // thread reads from local node

  register int tmp = 0;
  long long local_count = 0;

  int k = 0;

  while (!terminated) {
    // simulate the extra math here;
    tmp += xorshf96(x, y, z);
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
  printf("CPU ID: %d, local_count: %d, tmp: %d\n", index, local_count, tmp);
  global_count += local_count;
}

// threads read from remote node
void *remote_work(void *parm) {
  int index = (long)parm;

  unsigned long x = rand_r(&random_seeds[index]);
  unsigned long y = rand_r(&random_seeds[index]);
  unsigned long z = rand_r(&random_seeds[index]);

  register int tmp = 0;
  long long local_count = 0;

  int k = 0;

  while (!terminated) {
    tmp += array[xorshf96(x, y, z) % num_regions][xorshf96(x, y, z) % N];
    // tmp += array[array_index][rand_r(&random_seeds[c_ID]) & rand_mask];
    if (start_count) {
      ++k;
      if (k == COUNT_THRESHOLD) {
        ++local_count;
        k = 0;
      }
    }

  }
  printf("CPU ID: %d, local_count: %d, tmp: %d\n", index, local_count, tmp);
  global_count += local_count;
}

void initialize(int num_regions) {
  array = new int*[num_regions];
  for (int i = 0; i < num_regions; i++){
    array[i] = (int*) numa_alloc_onnode(sizeof(int) * N, i);
  }
}

void clean_up() {
  for (int i = 0; i < num_regions; i++){
  numa_free(array[i], sizeof(int) * N);
  }
  delete[] array;
}

int main() {
  if (numa_available() < 0) {
    printf("numa unavailable\n");
    exit(1);
  }
  num_regions = numa_num_configured_nodes();
  int max_threads = std::thread::hardware_concurrency();

  initialize(num_regions);
  srand(time(NULL));
  for (int i = 0; i < N; ++i) {
   for(int region = 0; region < num_regions; region++){
     array[region][i] = rand();
   }
  }

  // build mapping of threads to regions
  std::map<int, std::vector<int>> region_mapping;
  for (int core_id = 0; core_id < max_threads; core_id++) {
    region_mapping[numa_node_of_cpu(core_id)].push_back(core_id);
  }
  // assign different random seeds for threads used in rand_r() function
  for (int i = 0; i < max_threads; ++i) {
    random_seeds[i] = rand();
  }

  for (int num_threads = num_regions; num_threads <= max_threads; num_threads +=
      num_regions) {
    assert(num_threads%num_regions == 0);
    global_count = 0;
    terminated = false;
    start_count = false;
    printf("Start with number of threads: %d\n", num_threads);
    // const int NUM_THREADS = 24;
    pthread_t threads[num_threads];

    int thread_count = 0;

    pthread_attr_t attr;
    cpu_set_t cpus;
    pthread_attr_init(&attr);

    for (int i = 0; i < num_threads/num_regions; i++){
      for(int region; region < num_regions; region++){
        int thread_id = region_mapping[region][i];
        CPU_ZERO(&cpus);
        CPU_SET(thread_id, &cpus);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        pthread_create(&threads[thread_count++], &attr, remote_work, (void *) (long)thread_id);
      }
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

    for (int i = 0; i < num_threads; ++i) {
      // wait for undetached threads
      pthread_join(threads[i], NULL);
    }
    
    printf("time: %lf, total count: %d, ratio: %lf\n", time, (int) global_count,
        1.0 * global_count / time);
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
