//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/include/benchmark/ycsb/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/benchmark_common.h"
#include "benchmark/numabench/numabench_configuration.h"
#include "storage/data_table.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace numabench {

extern configuration state;

void RunHashJoin();

void RunWorkload();



}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
