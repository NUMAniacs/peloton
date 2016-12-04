//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scanbench_config.h
//
// Identification: src/include/benchmark/scanbench/scanbench_config.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <cstring>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "common/types.h"

namespace peloton {
namespace benchmark {
namespace scanbench {

static const oid_t scanbench_database_oid = 100;


class configuration {
public:
  // size of the table
  int scale_factor;

  // use a read only transaction for the hash join
  bool read_only_txn;

  // time of the scan in milliseconds
  double execution_time_ms = 0;

};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void WriteOutput();

}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton