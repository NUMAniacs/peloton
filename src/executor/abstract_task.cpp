//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_task.cpp
//
// Identification: src/executor/abstract_task.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"
#include "common/partition_macros.h"
#include "executor/abstract_executor.h"
#include "executor/executor_context.h"
#include "executor/abstract_task.h"

namespace peloton {
namespace executor {

// Initialize the task with callbacks
void AbstractTask::Init(std::shared_ptr<Trackable> trackable,
                        planner::Dependent *dependent, size_t num_tasks,
                        concurrency::Transaction *txn) {
  this->trackable = trackable;
  this->dependent = dependent;
  this->txn = txn;
  if (result_tile_lists != nullptr) {
    result_tile_lists->resize(num_tasks);
  }
  initialized = true;
}

size_t PartitionAwareTask::ReChunkResultTiles(
    AbstractTask *task,
    std::shared_ptr<executor::LogicalTileLists> &result_tile_lists) {

  // Get the total number of partition
  size_t num_partitions = PL_NUM_PARTITIONS();

  // Count variables for sanity check
  size_t total_num_tuples_from_child = 0;
  size_t total_num_tuples_after_rechunk = 0;

  // Group the results based on partitions
  LOG_DEBUG("Re-group results into %d partitions", (int)num_partitions);
  executor::LogicalTileLists partitioned_result_tile_lists(num_partitions);
  for (auto &result_tile_list : *(task->result_tile_lists.get())) {
    for (auto &result_tile : result_tile_list) {
      total_num_tuples_from_child += result_tile->GetTupleCount();
      size_t partition = result_tile->GetPartition();
      // TODO Handle non-partitioned tables
      partitioned_result_tile_lists[partition]
          .emplace_back(result_tile.release());
    }
  }

  // Populate tasks for each partition and re-chunk the tiles
  for (size_t partition = 0; partition < num_partitions; partition++) {
    executor::LogicalTileList next_result_tile_list;

    for (auto &result_tile : partitioned_result_tile_lists[partition]) {
      // TODO we should re-chunk based on TASK_TUPLE_COUNT
      total_num_tuples_after_rechunk += result_tile->GetTupleCount();
      next_result_tile_list.push_back(std::move(result_tile));
      // Reached the limit of each chunk
      if (next_result_tile_list.size() >= TASK_TILEGROUP_COUNT) {
        result_tile_lists->push_back(std::move(next_result_tile_list));
        next_result_tile_list = executor::LogicalTileList();
      }
    }
    // Check the remaining result tiles
    if (next_result_tile_list.size() > 0) {
      result_tile_lists->push_back(std::move(next_result_tile_list));
    }
  }

  LOG_DEBUG("Number of tasks after re-chunk: %d",
            (int)result_tile_lists->size());
  LOG_DEBUG("Number of tuples from child: %d",
            (int)total_num_tuples_from_child);
  PL_ASSERT(total_num_tuples_after_rechunk == total_num_tuples_from_child);
  return total_num_tuples_after_rechunk;
}

}  // namespace executor
}  // namespace peloton
