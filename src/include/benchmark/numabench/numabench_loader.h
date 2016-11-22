//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_loader.h
//
// Identification: src/include/benchmark/ycsb/ycsb_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/numabench/numabench_configuration.h"

namespace peloton {
namespace benchmark {
namespace numabench {

extern configuration state;

void CreateNUMABenchDatabase();

void LoadNUMABenchDatabase();

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
