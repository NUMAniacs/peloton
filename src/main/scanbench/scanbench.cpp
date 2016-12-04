//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scanbench.cpp
//
// Identification: src/main/scanbench/scanbench.cpp
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

#include "benchmark/scanbench/scanbench_config.h"
#include "benchmark/scanbench/scanbench_loader.h"
#include "benchmark/scanbench/scanbench_workload.h"

#include "gc/gc_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace scanbench {

configuration state;

// Main Entry Point
void RunBenchmark() {

  PelotonInit::Initialize();

  // Create the database
  CreateScanBenchDatabase();

  // Load the databases
  LoadScanBenchDatabase();

  // Run the workload
  RunSingleTupleSelectivityScan();

  Run1pcSelectivityScan();

  concurrency::EpochManagerFactory::GetInstance().StopEpoch();

  gc::GCManagerFactory::GetInstance().StopGC();

  PelotonInit::Shutdown();

  // Emit throughput
  WriteOutput();
}

}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::scanbench::ParseArguments(argc, argv,
                                          peloton::benchmark::scanbench::state);

  peloton::benchmark::scanbench::RunBenchmark();

  return 0;
}