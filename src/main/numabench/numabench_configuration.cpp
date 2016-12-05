//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_configuration.cpp
//
// Identification: src/main/ycsb/ycsb_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <iostream>
#include <fstream>

#include "benchmark/numabench/numabench_configuration.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace numabench {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  print help message \n"
          "   -s --scale_factor      :  # of K tuples (default: 1)\n"
          "   -t --read_only         :  use read only transaction (default: false)\n"
          "   -l --partition_left    :  partition left table on join key (default: false)\n"
          "   -r --partition_right   :  partition right table on join key (default: false)\n"
          "   -p --min_thread_num    :  minimum number of threads in thread pool (default: 4)\n"
          "   -q --max_thread_num    :  maxmum number of threads in thread pool (default: 24)\n"
  );
}

static struct option opts[] = {
    { "scale_factor", optional_argument, NULL, 's' },
    { "read_only", optional_argument, NULL, 't' },
    { "partition_left", optional_argument, NULL, 'l' },
    { "partition_right", optional_argument, NULL, 'r' },
    { "min_thread_num", optional_argument, NULL, 'p' },
    { "max_thread_num", optional_argument, NULL, 'q' },
    { NULL, 0, NULL, 0 }
};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "scale_factor", state.scale_factor);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.scale_factor = 1;
  state.read_only_txn = false;
  state.partition_left = false;
  state.partition_right = false;
  state.min_thread_num = 4;
  state.max_thread_num = 24;
  state.custom_hashtable = false;


  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "htlrcs:p:q:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      case 's':
        state.scale_factor = atoi(optarg);
        break;
      case 'p':
        state.min_thread_num = atoi(optarg);
        break;
      case 'q':
        state.max_thread_num = atoi(optarg);
        break;
      case 't':
        state.read_only_txn = true;
        break;
      case 'l':
        state.partition_left = true;
        break;
      case 'r':
        state.partition_right = true;
        break;
      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
    }
  }

  // Print configuration
  ValidateScaleFactor(state);

  
}
void WriteOutput(int thread_num) {
  std::ofstream out("outputfile.summary", std::ios_base::app | std::ios_base::out);

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %s %s %s %s %d :: %ld",
           state.scale_factor,
           state.read_only_txn ? "true" : "false",
           state.partition_left ? "true" : "false",
           state.partition_right ? "true" : "false",
           state.custom_hashtable ? "true" : "false",
           thread_num,
           state.execution_time_ms);

  out << state.scale_factor << " ";
  out << (state.read_only_txn ? "true" : "false") << " ";
  out << (state.partition_left ? "true" : "false") << " ";
  out << (state.partition_right ? "true" : "false") << " ";
  out << (state.custom_hashtable ? "true" : "false") << " ";
  out << thread_num << " ";
  out << state.execution_time_ms << "\n";

  out.flush();
  out.close();
}


}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
