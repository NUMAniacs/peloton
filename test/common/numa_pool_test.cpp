//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numa_pool_test.cpp
//
// Identification: test/common/numa_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/numa_thread_pool.h"
#include "common/harness.h"
#include "common/logger.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// NUMA Thread Pool Test
//===--------------------------------------------------------------------===//

class NumaPoolTest : public PelotonTest {};

struct TaskArg {
	const int socket_id;
	TaskArg *self;
	TaskArg(const int &i) : socket_id(i) {};
};

void run(int *socket_id, std::atomic<int> *ctr) {
	auto cpuID = sched_getcpu();
	LOG_DEBUG("CPU:%d NumaNode:%d", cpuID, numa_node_of_cpu(cpuID));
	EXPECT_EQ(*socket_id, numa_node_of_cpu(cpuID));
	delete socket_id;
	ctr->fetch_add(1);
}

TEST_F(NumaPoolTest, BasicTest) {
	NumaThreadPool numa_thread_pool;
	int num_cpus = std::thread::hardware_concurrency();
	int num_tasks_per_socket = 100;

	numa_thread_pool.Initialize(num_cpus);
	std::atomic<int> counter(0);

	for (int j=0; j<num_tasks_per_socket; j++) {
		// interleave task assignment across numa sockets
		for (int i=0; i<=numa_max_node(); i++) {
			// to avoid race conditions just for test
			int *socket_id = new int(i);
			numa_thread_pool.SubmitTask(i, run, &(*socket_id), &counter);
		}
	}

	// Wait for the test to finish
	while (counter.load() != num_tasks_per_socket*(numa_max_node()+1)) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	numa_thread_pool.Shutdown();
}

}  // End test namespace
}  // End peloton namespace