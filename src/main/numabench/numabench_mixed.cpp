//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_mixed.cpp
//
// Identification: src/main/ycsb/ycsb_mixed.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//



#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "benchmark/numabench/numabench_workload.h"
#include "benchmark/numabench/numabench_configuration.h"
#include "benchmark/numabench/numabench_loader.h"



namespace peloton {
namespace benchmark {
namespace numabench {

  
bool RunMixed() {
  RunHashJoin();
  return true;
}

}
}
}
