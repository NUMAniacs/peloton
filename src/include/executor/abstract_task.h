//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_Task.h
//
// Identification: src/include/Task/abstract_Task.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include "executor/logical_tile.h"
#include "storage/data_table.h"

#define INVALID_TASK_ID -1

#define INVALID_NUM_TASK -1

// Should we set the granularity by number of tile groups or number of tuples??
#define TASK_TILEGROUP_COUNT 100
#define TASK_TUPLE_COUNT TASK_TILEGROUP_COUNT *DEFAULT_TUPLES_PER_TILEGROUP


namespace peloton {

namespace planner {
class Dependent;
class AbstractPlan;
}

namespace executor {
class ParallelHashExecutor;
class ParallelSeqScanExecutor;
class Trackable;
class ExecutorContext;
}

namespace concurrency {
class Transaction;
}

namespace executor {

/*
 * Type for a list of pointers to tile groups
 */
typedef std::vector<std::shared_ptr<storage::TileGroup>> TileGroupPtrList;

// Type for a list of result logical tiles
typedef std::vector<std::unique_ptr<executor::LogicalTile>> LogicalTileList;
typedef std::vector<LogicalTileList> LogicalTileLists;

enum TaskType {
  TASK_PARTITION_UNAWARE = 0,
  TASK_INSERT = 1,
  TASK_SEQ_SCAN = 2,
  TASK_HASH = 3,
  TASK_HASHJOIN = 4,
};

/*
 * Type for a list of pointers to tile groups
 */
typedef std::vector<std::shared_ptr<storage::TileGroup>> TileGroupPtrList;

class AbstractTask {
 public:
  virtual ~AbstractTask() {}

  virtual TaskType GetTaskType() = 0;

  explicit AbstractTask(const planner::AbstractPlan *node,
                        std::shared_ptr<LogicalTileLists> result_tile_lists)
      : node(node), result_tile_lists(result_tile_lists) {}

  // Initialize the task with callbacks
  void Init(std::shared_ptr<Trackable> trackable, planner::Dependent *dependent,
            size_t num_tasks, concurrency::Transaction *txn);

  // Plan node corresponding to this task.
  const planner::AbstractPlan *node;

  // The shared result vector for each task. All the intermediate result are
  // buffered here
  std::shared_ptr<LogicalTileLists> result_tile_lists;

  // The trackable to call after task completes
  std::shared_ptr<Trackable> trackable = nullptr;

  // The callback to call after dependency completes
  planner::Dependent *dependent = nullptr;

  // Whether the task is initialized
  bool initialized = false;

  // The txn for this task
  concurrency::Transaction *txn = nullptr;
};

// The *abstract* task class for partition-aware / parallel tasks
class PartitionAwareTask : public AbstractTask {
 public:
  virtual ~PartitionAwareTask() {}

  explicit PartitionAwareTask(
      const planner::AbstractPlan *node, size_t task_id, size_t partition_id,
      std::shared_ptr<LogicalTileLists> result_tile_lists)
      : AbstractTask(node, result_tile_lists),
        task_id(task_id),
        partition_id(partition_id) {}

  inline LogicalTileList &GetResultTileList() {
    return (*result_tile_lists)[task_id];
  }

  static size_t ReChunkResultTiles(
      AbstractTask *task,
      std::shared_ptr<executor::LogicalTileLists> &result_tile_lists);

  // The id of this task
  size_t task_id;

  // The target partition's id
  size_t partition_id;
};

// The default task class for queries which don't need parallelism
class PartitionUnawareTask : public AbstractTask {
 public:
  ~PartitionUnawareTask() {}

  TaskType GetTaskType() { return TASK_PARTITION_UNAWARE; }

  explicit PartitionUnawareTask(
      const planner::AbstractPlan *node,
      std::shared_ptr<LogicalTileLists> result_tile_lists)
      : AbstractTask(node, result_tile_lists) {}
};

// The class for insert tasks
class InsertTask : public PartitionAwareTask {
 public:
  ~InsertTask() {}

  TaskType GetTaskType() { return TASK_INSERT; }

  /*
   * @param bulk_insert_count: The total bulk insert count in insert plan node
   */
  explicit InsertTask(const planner::AbstractPlan *node,
                      size_t bulk_insert_count,
                      size_t task_id = INVALID_TASK_ID,
                      size_t partition_id = INVALID_PARTITION_ID)
      : PartitionAwareTask(node, task_id, partition_id, nullptr) {
    // By default we insert all the tuples
    tuple_bitmap.resize(bulk_insert_count, true);
  }

  // The bitmap of tuples to insert
  std::vector<bool> tuple_bitmap;
};

// The class for hash tasks
class HashJoinTask : public PartitionAwareTask {
 public:
  ~HashJoinTask() {}

  TaskType GetTaskType() { return TASK_HASHJOIN; }

  explicit HashJoinTask(const planner::AbstractPlan *node,
                        std::shared_ptr<ParallelHashExecutor> hash_executor,
                        size_t task_id, size_t partition_id,
                        std::shared_ptr<LogicalTileLists> result_tile_lists)
      : PartitionAwareTask(node, task_id, partition_id, result_tile_lists),
        hash_executor(hash_executor) {}

  // Keep a reference to the hash executor so that it's not free'd during
  // execution
  std::shared_ptr<ParallelHashExecutor> hash_executor;
};

// The class for hash tasks
class HashTask : public PartitionAwareTask {
 public:
  ~HashTask() {}

  TaskType GetTaskType() { return TASK_HASH; }

  explicit HashTask(const planner::AbstractPlan *node,
                    std::shared_ptr<ParallelHashExecutor> hash_executor,
                    size_t task_id, size_t partition_id,
                    std::shared_ptr<LogicalTileLists> result_tile_lists)
      : PartitionAwareTask(node, task_id, partition_id, result_tile_lists),
        hash_executor(hash_executor) {}

  // Keep a reference to the hash executor so that it's not free'd during
  // execution
  std::shared_ptr<ParallelHashExecutor> hash_executor;
};

// The class for parallel seq scan tasks
class SeqScanTask : public PartitionAwareTask {
 public:
  // The list of pointers to the tile groups managed by this task
  TileGroupPtrList tile_group_ptrs;

 public:
  ~SeqScanTask() {}

  TaskType GetTaskType() { return TASK_SEQ_SCAN; }

  /**
   * @brief Constructor for seqscan Task.
   * @param node Sequential scan node corresponding to this Task.
   */
  explicit SeqScanTask(const planner::AbstractPlan *node, size_t task_id,
                       size_t partition_id,
                       std::shared_ptr<LogicalTileLists> result_tile_lists)
      : PartitionAwareTask(node, task_id, partition_id, result_tile_lists) {}
};

}  // namespace executor
}  // namespace peloton
