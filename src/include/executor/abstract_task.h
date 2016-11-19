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
#define DEFAULT_PARTITION_ID 0
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

class AbstractTask {
 public:
  virtual ~AbstractTask() {}

  explicit AbstractTask(const planner::AbstractPlan *node,
                        int partition_id = DEFAULT_PARTITION_ID)
      : node(node), partition_id(partition_id) {}

  /** @brief Plan node corresponding to this Task. */
  const planner::AbstractPlan *node = nullptr;

  // The target partition's id
  int partition_id = DEFAULT_PARTITION_ID;

  // The callback to call after task completes
  bridge::Notifiable *callback = nullptr;

  // The results
  std::vector<std::unique_ptr<executor::LogicalTile>> results;
};

class InsertTask : public AbstractTask {
 public:
  /**
   * @brief Constructor for insert Task.
   * @param node Insert node corresponding to this Task.
   */
  explicit InsertTask(const planner::AbstractPlan *node, int bulk_insert_count,
                      int partition_id = DEFAULT_PARTITION_ID)
      : AbstractTask(node, partition_id) {
    // By default we insert all the tuples
    tuple_bitmap.resize(bulk_insert_count, true);
  }

  // The bitmap of tuples to insert
  std::vector<bool> tuple_bitmap;
};

class SeqScanTask : public AbstractTask {
 public:
  // The list of pointers to the tile groups managed by this task
  TileGroupPtrList tile_group_ptrs;
  // ID of this task
  size_t task_id;
 public:
  /**
   * @brief Constructor for seqscan Task.
   * @param node Sequential scan node corresponding to this Task.
   */
  explicit SeqScanTask(const planner::AbstractPlan *node, size_t task_id,
                       int partition_id = DEFAULT_PARTITION_ID)
      : AbstractTask(node, partition_id), task_id(task_id){}
};

}  // namespace executor
}  // namespace peloton