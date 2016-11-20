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

// TODO move me to type.h

// Default partition id
#define DEFAULT_PARTITION_ID 0

#define INVALID_TASK_ID -1

// Should we set the granularity by number of tile groups or number of tuples??
#define TASK_TILEGROUP_COUNT 100

namespace peloton {

namespace bridge {
class Notifiable;
}

namespace planner {
class AbstractPlan;
}

namespace executor {

/*
 * Type for a list of pointers to tile groups
 */
typedef std::vector<std::shared_ptr<storage::TileGroup>> TileGroupPtrList;

// Type for a list of result logical tiles
typedef std::vector<std::unique_ptr<executor::LogicalTile>> ResultTileList;
typedef std::vector<ResultTileList> ResultTileLists;

class AbstractTask {
 public:
  virtual ~AbstractTask() {}

  explicit AbstractTask(const planner::AbstractPlan *node, int partition_id,
                        size_t task_id,
                        std::shared_ptr<ResultTileLists> result_tiles)
      : node(node),
        partition_id(partition_id),
        task_id(task_id),
        result_tiles(result_tiles) {}

  inline void Init(bridge::Notifiable *callback, int num_tasks) {
    this->callback = callback;
    result_tiles->resize(num_tasks);
    initialized = true;
  }

  inline void Init() { initialized = true; }

 public:
  /** @brief Plan node corresponding to this Task. */
  const planner::AbstractPlan *node = nullptr;

  // The target partition's id
  int partition_id;

  // ID of this task
  size_t task_id;

  // The callback to call after task completes
  bridge::Notifiable *callback = nullptr;

  // The shared result vector for each task. All the intermediate result are
  // buffered here. (further used by joins)

  // TODO Logical tiles should also be NUMA aware, we should also keep the
  // partition id in logical tile, too
  std::shared_ptr<ResultTileLists> result_tiles;

  // Whether the task is initialized
  bool initialized = false;
};

class InsertTask : public AbstractTask {
 public:
  /**
   * @brief Constructor for insert Task.
   * @param node Insert node corresponding to this Task.
   */
  explicit InsertTask(const planner::AbstractPlan *node, int bulk_insert_count,
                      int partition_id = DEFAULT_PARTITION_ID)
      : AbstractTask(node, partition_id, INVALID_TASK_ID, nullptr) {
    // By default we insert all the tuples
    tuple_bitmap.resize(bulk_insert_count, true);
    // We don't care about the intermediate result of inserts
    Init();
  }

 public:
  // The bitmap of tuples to insert
  std::vector<bool> tuple_bitmap;
};

class DefaultTask : public AbstractTask {

 public:
  /**
   * @brief Constructor for default tasks.
   * @param node Sequential scan node corresponding to this Task.
   */
  explicit DefaultTask(const planner::AbstractPlan *node, size_t num_partitions)
      : AbstractTask(node, rand() % num_partitions, INVALID_TASK_ID, nullptr) {
    Init();
  }
};

class SeqScanTask : public AbstractTask {
 public:
  // The list of pointers to the tile groups managed by this task
  TileGroupPtrList tile_group_ptrs;

 public:
  /**
   * @brief Constructor for seqscan Task.
   * @param node Sequential scan node corresponding to this Task.
   */
  explicit SeqScanTask(const planner::AbstractPlan *node, size_t task_id,
                       int partition_id,
                       std::shared_ptr<ResultTileLists> result_tiles)
      : AbstractTask(node, partition_id, task_id, result_tiles) {}
};

}  // namespace executor
}  // namespace peloton
