//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scanbench_config.cpp
//
// Identification: src/main/scanbench/scanbench_config.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <ctime>

#include "benchmark/scanbench/scanbench_config.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace scanbench {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
              "   -h --help              :  print help message \n"
              "   -s --scale_factor      :  # of M tuples (default: 1)\n"
              "   -t --read_only         :  don't use read only transaction (default: true)\n"
              "   -u --no_part           :  run NUMA unaware workload (default: false)\n"
  );
}

static struct option opts[] = {
    { "scale_factor", optional_argument, NULL, 's' },
    { "read_only", optional_argument, NULL, 't' },
    { "no_partition", optional_argument, NULL, 'u'},
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
  state.read_only_txn = true;
  state.numa_aware = true;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hts:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
      case 's':
        state.scale_factor = atoi(optarg);
        break;
      case 't':
        state.read_only_txn = false;
        break;
      case 'u':
        state.numa_unaware = true;
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


void WriteOutput(std::stringstream& ostream) {
  std::ofstream out("outputfile.summary."+ std::to_string(std::time(nullptr)));

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %s %s:: %f",
           state.scale_factor,
           state.read_only_txn ? "true" : "false",
           state.numa_aware ? "true" : "false",
           state.execution_time_ms);

  out << state.scale_factor << " ";
  out << (state.read_only_txn ? "true" : "false") << " ";
  out << (state.numa_aware ? "true" : "false") << " ";
  out << ostream.str();
  out.flush();
  out.close();
}

}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton