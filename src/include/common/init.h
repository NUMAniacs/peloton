//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// init.h
//
// Identification: src/include/common/init.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "common/thread_pool.h"
#include "common/partition_thread_pool.h"


namespace peloton {

class ThreadPool;

extern ThreadPool thread_pool;

extern PartitionThreadPool partitioned_executor_thread_pool;

extern NumaThreadPool partitioned_executor_thread_pool;

//===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

class PelotonInit {
 public:
  static void Initialize();

  static void Shutdown();

  static void SetUpThread();

  static void TearDownThread();
};

}  // End peloton namespace
