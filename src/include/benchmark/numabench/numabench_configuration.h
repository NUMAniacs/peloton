//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_configuration.h
//
// Identification: src/include/benchmark/ycsb/ycsb_configuration.h
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
#define NUMABENCH_DB_NAME "numabench_db"
namespace peloton {
namespace benchmark {
namespace numabench {

// static const oid_t ycsb_database_oid = 100;
static const oid_t numabench_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

// static const oid_t ycsb_field_length = 100;
static const oid_t numabench_field_length = 100;

class configuration {
 public:
  // size of the table
  int scale_factor;

  // Min number of threads in thread pool
  int min_thread_num;

  // Max number of threads
  int max_thread_num;

  // use a read only transaction for the hash join
  bool read_only_txn;

  // partition the left table by the join key
  bool partition_left;

  // partition the right table by the join key
  bool partition_right;

  // time of the hash join in milliseconds
  long execution_time_ms = 0;

  // Whether to use custom hash table
  bool custom_hashtable;

  // The breakdown of execution time
  std::vector<double> execution_time_breakdown;
};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateScaleFactor(const configuration &state);

void WriteOutput(int thread_num);

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
