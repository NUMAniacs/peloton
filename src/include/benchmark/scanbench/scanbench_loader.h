//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scanbench_loader.h
//
// Identification: src/include/benchmark/scanbench/scanbench_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/scanbench/scanbench_config.h"

namespace peloton {
namespace benchmark {
namespace scanbench {

extern configuration state;

void CreateScanBenchDatabase();

void LoadScanBenchDatabase();

}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton