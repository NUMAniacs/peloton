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

  std::stringstream ostream, dummy;

  PelotonInit::Initialize();

  // Create the database
  CreateScanBenchDatabase();

  // Load the databases
  LoadScanBenchDatabase();

  // Run the workload, 3 trials
  for (int i=0; i<4; i++) {
    if (i == 0) {
      RunSingleTupleSelectivityScan(dummy);
      Run1pcSelectivityScan(dummy);
      Run10pcSelectivityScan(dummy);
    } else {
      ostream << "\nSingle Tuple Trial " << i << "\n";
      RunSingleTupleSelectivityScan(ostream);
      ostream << "\n1% Selectivity Trial " << i << "\n";
      Run1pcSelectivityScan(ostream);
      ostream << "\n10% Selectivity Trial " << i << "\n";
      Run10pcSelectivityScan(ostream);
    }
  }

  concurrency::EpochManagerFactory::GetInstance().StopEpoch();

  gc::GCManagerFactory::GetInstance().StopGC();

  PelotonInit::Shutdown();

  // Emit throughput
  WriteOutput(ostream);
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