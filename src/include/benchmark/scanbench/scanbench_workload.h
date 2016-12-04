//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scanbench_workload.h
//
// Identification: src/include/benchmark/scanbench/scanbench_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/benchmark_common.h"
#include "benchmark/scanbench/scanbench_config.h"
#include "storage/data_table.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
  class DataTable;
}

namespace benchmark {
namespace scanbench {

extern configuration state;

void RunSingleTupleSelectivityScan();

void Run1pcSelectivityScan();

}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton