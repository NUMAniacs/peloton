//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// partition_macros.h
//
// Identification: src/include/common/partition_macros.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numa.h>
#include <thread>
#pragma once

// This file defines all NUMA-related interface between peloton and the system
namespace peloton {

// Set this value to 1 if you want to simulate more than one partition
// on a single socket machine. The result of simulate is that each
// core/partition node correspond to a partition
#define SIMULATE_NUMA_PARTITION 0

#if SIMULATE_NUMA_PARTITION
// Get total number of partitions
#define PL_NUM_PARTITIONS() (numa_max_node() + 1)

// Get the partition node id of current worker
#define PL_GET_PARTITION_NODE() sched_getcpu()

// Get the partition id
#define PL_GET_PARTITION_ID(x) numa_node_of_cpu(x)

#else
// XXX Assume hyper-threading, num_partition = num_cores
#define PL_NUM_PARTITIONS() (int)(std::thread::hardware_concurrency() / 2)

// Get the partition node id of current worker (= core id)
#define PL_GET_PARTITION_NODE() sched_getcpu()

// Get the partition id
#define PL_GET_PARTITION_ID(x) x

#endif

}  // End peloton namespace
