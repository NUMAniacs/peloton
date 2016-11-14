//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// partition_pool_test.cpp
//
// Identification: test/common/partition_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/partition_thread_pool.h"
#include "common/harness.h"
#include "common/logger.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Partition Thread Pool Test
//===--------------------------------------------------------------------===//

class PartitionPoolTest : public PelotonTest {};

struct TaskArg {
  const int partition_id;
  TaskArg *self = nullptr;
  TaskArg(const int &i) : partition_id(i) {};
};

void run(int *partition_id, std::atomic<int> *ctr) {
  auto partition_node_id = PL_GET_PARTITION_NODE();
  LOG_DEBUG("Partition node(CPU): %d \t Partition (NumaNode): %d",
            partition_node_id, PL_GET_PARTITION_ID(partition_node_id));
  EXPECT_EQ(*partition_id, PL_GET_PARTITION_ID(partition_node_id));
  delete partition_id;
  ctr->fetch_add(1);
}

TEST_F(PartitionPoolTest, BasicTest) {
  PartitionThreadPool partition_thread_pool;
  int num_cpus = std::thread::hardware_concurrency();
  int num_tasks_per_partition = 100;

  partition_thread_pool.Initialize(num_cpus);
  std::atomic<int> counter(0);

  for (int j = 0; j < num_tasks_per_partition; j++) {
    // interleave task assignment across numa sockets
    for (int i = 0; i < PL_NUM_PARTITIONS(); i++) {
      // to avoid race conditions just for test
      int *partition_id = new int(i);
      partition_thread_pool.SubmitTask(i, run, &(*partition_id), &counter);
    }
  }

  // Wait for the test to finish
  while (counter.load() != num_tasks_per_partition * (PL_NUM_PARTITIONS())) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  partition_thread_pool.Shutdown();
}

}  // End test namespace
}  // End peloton namespace
