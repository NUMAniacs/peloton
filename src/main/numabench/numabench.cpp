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
// Main Entry Point
void RunBenchmark() {

  PelotonInit::Initialize();

  if (state.gc_mode == true) {
    gc::GCManagerFactory::Configure(state.gc_backend_count);
  }
//  I think this happens in Initialize
//  gc::GCManagerFactory::GetInstance().StartGC();

  // Create the database
  CreateNUMABenchDatabase();

  // Load the databases
  LoadNUMABenchDatabase();

  // Run the workload
  RunWorkload();
  
  concurrency::EpochManagerFactory::GetInstance().StopEpoch();

  gc::GCManagerFactory::GetInstance().StopGC();

  // Emit throughput
  WriteOutput();
}

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::numabench::ParseArguments(argc, argv,
                                           peloton::benchmark::numabench::state);

  peloton::benchmark::numabench::RunBenchmark();

  return 0;
}
