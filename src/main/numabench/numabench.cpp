//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb.cpp
//
// Identification: src/main/ycsb/ycsb.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//w

#include <iostream>
#include <fstream>
#include <iomanip>

#include "common/init.h"
#include "common/logger.h"

#include "concurrency/epoch_manager_factory.h"

#include "benchmark/numabench/numabench_configuration.h"
#include "benchmark/numabench/numabench_loader.h"
#include "benchmark/numabench/numabench_workload.h"

#include "gc/gc_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace numabench {

configuration state;

void RunHelper() {
  int thread_num = state.min_thread_num;

  while (thread_num <= state.max_thread_num) {
    partitioned_executor_thread_pool.Shutdown();
    partitioned_executor_thread_pool.Initialize(thread_num);

    // Run the workload
    RunWorkload();

    // Emit throughput
    WriteOutput(thread_num);
    thread_num += state.thread_step;
  }
}

// Main Entry Point
void RunBenchmark() {

  PelotonInit::Initialize();

  //  I think this happens in Initialize
  //  gc::GCManagerFactory::GetInstance().StartGC();

  // Create the database
  CreateNUMABenchDatabase();

  // Load the databases
  LoadNUMABenchDatabase();


  if (state.one_partition) {

    // ====== No Shuffle =======
    state.random_partition_execution = false;
    state.partition_by_join_key = true;

    // Cuckoo
    state.custom_hashtable = false;
    RunHelper();
    // Custom
    state.custom_hashtable = true;
    RunHelper();

    // ====== Shuffle =======
    state.random_partition_execution = true;
    state.partition_by_join_key = true;

    // Cuckoo
    state.custom_hashtable = false;
    state.partition_by_join_key = true;
    RunHelper();
    // Custom
    state.custom_hashtable = true;
    state.partition_by_join_key = true;
    RunHelper();

  } else {

//    // ====== No Shuffle =======
//    state.random_partition_execution = false;
//
//    // Cuckoo
//    state.custom_hashtable = false;
//    state.partition_by_join_key = false;
//    RunHelper();
//    state.partition_by_join_key = true;
//    RunHelper();
//
//    // Custom
//    state.custom_hashtable = true;
//    state.partition_by_join_key = false;
//    RunHelper();
//    state.partition_by_join_key = true;
//    RunHelper();
//
//    // ====== Shuffle =======
//    state.random_partition_execution = true;

    // Cuckoo
    state.custom_hashtable = false;
    state.partition_by_join_key = false;
    RunHelper();
    state.partition_by_join_key = true;
    RunHelper();

//    // Custom
    state.custom_hashtable = true;
    state.partition_by_join_key = false;
    RunHelper();
    state.partition_by_join_key = true;
    RunHelper();
  }

  concurrency::EpochManagerFactory::GetInstance().StopEpoch();

  gc::GCManagerFactory::GetInstance().StopGC();
}

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::numabench::ParseArguments(
      argc, argv, peloton::benchmark::numabench::state);

  peloton::benchmark::numabench::RunBenchmark();

  return 0;
}
