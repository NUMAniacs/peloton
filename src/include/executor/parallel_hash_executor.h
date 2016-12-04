//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_hash_executor.h
//
// Identification: src/include/executor/parallel_hash_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <unordered_set>

#include "common/types.h"
#include "executor/abstract_executor.h"
#include "executor/hashmap.h"
#include "executor/logical_tile.h"
#include "common/container_tuple.h"
#include <boost/functional/hash.hpp>
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace executor {

/**
 * @brief Hash executor.
 *
 */
class ParallelHashExecutor : public AbstractExecutor {

 public:
  // <tile_itr, tuple_itr, task_itr>
  typedef std::tuple<size_t, oid_t, size_t> LookupValue;

  // TODO Make LookupValue a template param
  // A wrapper over std::vector with spin locks
  struct ConcurrentVector {
    Spinlock lock;
    std::vector<LookupValue> lookup_values;

    void Insert(std::tuple<size_t, oid_t, size_t> element) {
      lock.Lock();
      lookup_values.push_back(element);
      lock.Unlock();
    }

    const std::vector<LookupValue> &GetVector() const { return lookup_values; }
  };

  ParallelHashExecutor(const ParallelHashExecutor &) = delete;
  ParallelHashExecutor &operator=(const ParallelHashExecutor &) = delete;
  ParallelHashExecutor(const ParallelHashExecutor &&) = delete;
  ParallelHashExecutor &operator=(const ParallelHashExecutor &&) = delete;

  explicit ParallelHashExecutor(const planner::AbstractPlan *node,
                                ExecutorContext *executor_context);

  // TODO Let cuckoohash_map and Hashmap implement the same interface so we dont
  // have so many switch statement..
  typedef cuckoohash_map<
      expression::ContainerTuple<LogicalTile>,           // Key
      std::shared_ptr<ConcurrentVector>,                 // T
      expression::ContainerTupleHasher<LogicalTile>,     // Hash
      expression::ContainerTupleComparator<LogicalTile>  // Pred
      > CuckooHashMapType;

  typedef Hashmap<expression::ContainerTuple<LogicalTile>,            // Key
                  std::shared_ptr<ConcurrentVector>,                  // T
                  expression::ContainerTupleHasher<LogicalTile>,      // Hash
                  expression::ContainerTupleComparator<LogicalTile>,  // Pred
                  4,  // Bucket size
                  1   // Probe step size
                  > CustomHashMapType;

  inline CuckooHashMapType &GetCuckooHashTable() { return cuckoo_hash_table_; }

  inline CustomHashMapType &GetCustomHashTable() { return custom_hash_table_; }

  inline const std::vector<oid_t> &GetHashKeyIds() const { return column_ids_; }

  // Execute the hash task
  static void ExecuteTask(std::shared_ptr<AbstractTask> hash_task);

  inline size_t GetTotalNumTuples() const { return total_num_tuples_.load(); }

  inline void IncrementNumTuple(size_t num_tuples) {
    total_num_tuples_.fetch_add(num_tuples);
  }

  inline void Reserve(size_t num_tuples) {
    if (use_custom_hash_table) {
      custom_hash_table_.reserve(num_tuples);
    } else {
      cuckoo_hash_table_.reserve(num_tuples);
    }
  }

 public:
  /** @brief Input tiles from child node */
  std::shared_ptr<LogicalTileLists> child_tiles;

  bool use_custom_hash_table = false;

 protected:
  // Initialize the values of the hash keys from plan node
  void InitHashKeys();

  bool DInit();

  bool DExecute();

 private:
  /** @brief Cuckoo Hash table */
  CuckooHashMapType cuckoo_hash_table_;

  /** @brief Custom Hash table */
  CustomHashMapType custom_hash_table_;

  std::vector<oid_t> column_ids_;

  bool initialized_ = false;

  std::atomic<size_t> total_num_tuples_;
};

} /* namespace executor */
} /* namespace peloton */
