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
class ParallelHashExecutor : public AbstractExecutor, public Trackable {
 public:
  /** @brief Type definitions for hash table */
  typedef std::unordered_set<std::pair<size_t, oid_t>,
                             boost::hash<std::pair<size_t, oid_t>>> HashSet;

  // A wrapper over std::unordered_set with spin locks
  struct ConcurrentSet {
    Spinlock lock;
    HashSet unordered_set;

    void Insert(std::pair<size_t, oid_t> element) {
      lock.Lock();
      unordered_set.insert(element);
      lock.Unlock();
    }

    const HashSet &GetSet() const { return unordered_set; }
  };

  ParallelHashExecutor(const ParallelHashExecutor &) = delete;
  ParallelHashExecutor &operator=(const ParallelHashExecutor &) = delete;
  ParallelHashExecutor(const ParallelHashExecutor &&) = delete;
  ParallelHashExecutor &operator=(const ParallelHashExecutor &&) = delete;

  explicit ParallelHashExecutor(const planner::AbstractPlan *node,
                                ExecutorContext *executor_context);

  // TODO We should reserve the size of the cuckoomap if we know the number of
  // tuples to insert
  typedef cuckoohash_map<
      expression::ContainerTuple<LogicalTile>,           // Key
      std::shared_ptr<ConcurrentSet>,                    // T
      expression::ContainerTupleHasher<LogicalTile>,     // Hash
      expression::ContainerTupleComparator<LogicalTile>  // Pred
      > ParallelHashMapType;

  inline ParallelHashMapType &GetHashTable() { return hash_table_; }

  inline const std::vector<oid_t> &GetHashKeyIds() const { return column_ids_; }

  // Execute the hash task
  static void ExecuteTask(std::shared_ptr<AbstractTask> hash_task);

  // TODO This is a hack. Remove me when we hook up hash executor with seq scan
  // executor
  void SetChildTiles(std::shared_ptr<LogicalTileLists> child_tiles) {
    child_tiles_ = child_tiles;
  }

 protected:
  // Initialize the values of the hash keys from plan node
  void InitHashKeys();

  bool DInit();

  bool DExecute();

 private:
  /** @brief Hash table */
  ParallelHashMapType hash_table_;

  /** @brief Input tiles from child node */
  std::shared_ptr<LogicalTileLists> child_tiles_;

  std::vector<oid_t> column_ids_;

  size_t result_itr = 0;

  size_t task_itr = 0;

  bool initialized_ = false;
};

} /* namespace executor */
} /* namespace peloton */
