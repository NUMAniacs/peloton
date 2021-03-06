//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// partition_thread_pool.h
//
// Identification: src/include/common/partition_thread_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numa.h>
#include <unordered_map>
#include "thread_pool.h"
#include "common/partition_macros.h"

namespace peloton {

// wrapper for a numa thread pool
class PartitionThreadPool {
 private:
  int pool_size_;
  std::unordered_map<int, ThreadPool> thread_pool_map_;

 public:
  inline PartitionThreadPool() : pool_size_(0) {
    srand(time(NULL));
  };

  // TODO: extend to accept pool_size?
  // Creates a separate thread pool for each NUMA socket
  void Initialize(const int pool_size) {
    pool_size_ = pool_size;
    // partition id -> partition node ids
    std::unordered_map<int, std::vector<int>> partition_node_id_map;
    int partition_id;
    for (int i = 0; i < (int)std::thread::hardware_concurrency(); i++) {
      partition_id = PL_GET_PARTITION_ID(i);
      partition_node_id_map[partition_id].push_back(i);
    }
    std::unordered_map<int, std::vector<int>> initialize_map;
    int inserted_count = 0;
    // assume each node is symmetic (has the same number of threads)
    int partition_size = PL_GET_PARTITION_SIZE();
    for (int thread_in_partition = 0; thread_in_partition < partition_size; thread_in_partition++){
      for (int partition = 0; partition < PL_NUM_PARTITIONS() && inserted_count < pool_size_; partition++){
        inserted_count++;
        initialize_map[partition].push_back(partition_node_id_map[partition][thread_in_partition]);
      }
      if (inserted_count >= pool_size_){
        break;
      }
    }

    for (auto itr = initialize_map.begin();
         itr != initialize_map.end(); itr++) {
      if (itr->second.size() == 0) continue;
      thread_pool_map_[itr->first].InitializePinned(itr->second);
    }
  }

  // submit task to numa thread pool.
  // it accepts the numa socket ID, function and a set of function
  // parameters as parameters.
  template <typename FunctionType, typename... ParamTypes>
  void SubmitTask(int partition_id, FunctionType &&func,
                  const ParamTypes &&... params) {
    // add task to thread pool of given numa socket
    auto &io_service = thread_pool_map_[partition_id].GetIOService();
    io_service.post(std::bind(func, params...));
  }

  template <typename FunctionType, typename... ParamTypes>
  void SubmitTaskRandom(FunctionType &&func, const ParamTypes &&... params) {
    int rand_index = rand() % thread_pool_map_.size();
    auto random_it = std::next(std::begin(thread_pool_map_), rand_index);
    // submit task to a random numa socket
    auto &io_service = random_it->second.GetIOService();
    io_service.post(std::bind(func, params...));
  };

  // Shuts down all thread pools one by one
  void Shutdown() {
    for (auto itr = thread_pool_map_.begin(); itr != thread_pool_map_.end();
         itr++) {
      itr->second.Shutdown();
    }
    thread_pool_map_.clear();
  }
};
}
