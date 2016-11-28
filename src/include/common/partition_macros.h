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

#define PL_PARTITION_FREE(ptr, size) numa_free(ptr, size);

// Set this value to 1 if you want to simulate more than one partition
// on a single socket machine. The result of simulate is that each
// core/partition node correspond to a partition
// XXX Assume hyper-threading, num_partition = num_cores

#define SIMULATE_NUMA_PARTITION 1

#if SIMULATE_NUMA_PARTITION
  // Get total number of partitions
#define PL_NUM_PARTITIONS() 1

// Get the partition node id of current worker (= core id)
#define PL_GET_PARTITION_NODE() sched_getcpu()

// Get the partition id
#define PL_GET_PARTITION_ID(x) numa_node_of_cpu(x)

// Allocate a partition to default region
#define PL_PARTITION_ALLOC(size, partition) \
  numa_alloc_onnode(size, numa_node_of_cpu(partition));

// Get the number of parallel units in each partition
// (Assume homogeneous architecture)
#define PL_GET_PARTITION_SIZE() 1

#else
// Get total number of partitions
#define PL_NUM_PARTITIONS() (numa_max_node() + 1)

// Get the partition node id of current worker
#define PL_GET_PARTITION_NODE() sched_getcpu()

// Get the partition id
#define PL_GET_PARTITION_ID(x) numa_node_of_cpu(x)

// Allocate a partition
#define PL_PARTITION_ALLOC(size, partition) numa_alloc_onnode(size, partition);

// Get the number of parallel units in each partition
// (Assume homogeneous architecture)
#define PL_GET_PARTITION_SIZE() (int)(std::thread::hardware_concurrency() / PL_NUM_PARTITIONS())

#endif

}  // End peloton namespace
