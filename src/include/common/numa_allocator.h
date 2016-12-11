//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// partition_macros.h
//
// Identification: src/include/common/numa_allocator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numa.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/partition_macros.h"


#pragma once

// This file defines all NUMA-related interface between peloton and the system
namespace peloton {
namespace common{

// 50MB we want to allocate rarely
#define DEFAULT_NUMA_MALLOC_SIZE 1024*1024*50


struct MemData {
  size_t region_size;
  size_t space_left;
  char *mem_ptr;
};

class NumaAllocator {
public:
  static void Init() {
    instance = new NumaAllocator();
    // initialize
    for(int i = 0; i < (int)std::thread::hardware_concurrency(); i++){
      instance->memory_data_[i] = std::vector<MemData>();
    }
  }

  // used for mallocing on the local node
  static void *malloc_numa(size_t size) {
    return instance->malloc_numa_(size);
  }

  static void cleanup(){
    instance->cleanup_();
  }

private:
  NumaAllocator() {
    printf("in Numa Allocator constructor");
  }

  void * malloc_numa_(size_t size) {
    // get core
    // this is okay because we are pinned
    int core = sched_getcpu();
    int index = ((int)memory_data_[core].size()) - 1;
    if (index == -1 || memory_data_[core][index].space_left < size){
      size_t alloc_size = std::max((size_t)DEFAULT_NUMA_MALLOC_SIZE, size);
      void * new_memory = PL_PARTITION_ALLOC(alloc_size, PL_GET_PARTITION_ID(core));
      memory_data_[core].push_back(MemData{alloc_size, alloc_size, (char *)new_memory});
      index++;
    }
    auto &memdata = memory_data_[core][index];
    void * ptr = memdata.mem_ptr+(memdata.region_size-memdata.space_left);
    memdata.space_left -= size;
    return ptr;
  }

  // everything you allocated better be gone or else this will fuck you up
  void cleanup_() {
    for (auto &thread : memory_data_) {
      for (auto &data : thread.second) {
        numa_free(data.mem_ptr, data.region_size);
      }
      thread.second.clear();
    }
  }

private:
  // thread_id to information about allocation
  std::unordered_map<int, std::vector<MemData>> memory_data_;

  static NumaAllocator* instance;

};


}  // End common namespace
}  // End peloton namespace
