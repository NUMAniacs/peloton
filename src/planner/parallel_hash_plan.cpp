
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_join_plan.cpp
//
// Identification: /peloton/src/planner/hash_join_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"
#include "expression/abstract_expression.h"
#include "planner/project_info.h"
#include "common/partition_macros.h"
#include "planner/parallel_hash_plan.h"
#include "executor/abstract_task.h"

namespace peloton {
namespace planner {

// This is really the implementation for Dependency Complete
std::shared_ptr<executor::ParallelHashExecutor>
ParallelHashPlan::DependencyComplete(
    std::shared_ptr<executor::AbstractTask> task, bool hack) {

  (void)hack;
  // TODO Move the logic of incrementing completed task count to another class
  // Increment the number of tasks completed
  // int task_num = tasks_complete_.fetch_add(1);
  // This is the last task
  // if (task_num == total_tasks_ - 1) {

  // TODO Get the total number of partition
  size_t num_partitions = 1;  // PL_NUM_PARTITIONS();

  // Group the results based on partitions
  executor::LogicalTileLists partitioned_result_tile_lists(num_partitions);
  for (auto &result_tile_list : *(task->result_tile_lists.get())) {
    for (auto &result_tile : result_tile_list) {
      size_t partition = result_tile->GetPartition();
      // XXX  Hack default value
      partition = 0;
      partitioned_result_tile_lists[partition]
          .emplace_back(result_tile.release());
    }
  }
  // Populate tasks for each partition and re-chunk the tiles
  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());
  for (size_t partition = 0; partition < num_partitions; partition++) {
    executor::LogicalTileList next_result_tile_list;

    for (auto &result_tile : partitioned_result_tile_lists[partition]) {
      // TODO we should re-chunk based on number of tuples?
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

  size_t num_tasks = result_tile_lists->size();

  // A list of all tasks to execute
  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;

  // Construct the hash executor
  std::shared_ptr<executor::ParallelHashExecutor> hash_executor(
      new executor::ParallelHashExecutor(this, nullptr, num_tasks));
  hash_executor->Init();

  // TODO Add dummy child node to retrieve result from
  // hash_executor.AddChild(&right_table_scan_executor);

  for (size_t task_id = 0; task_id < num_tasks; task_id++) {
    // Construct a hash task
    // XXX Hard code partition
    size_t partition = 0;
    std::shared_ptr<executor::AbstractTask> next_task(new executor::HashTask(
        this, hash_executor, task_id, partition, result_tile_lists));
    // next_task->Init(next_callback, num_tasks);
    tasks.push_back(next_task);
  }

  // TODO Launch the new tasks by submitting the tasks to thread pool
  for (auto &task : tasks) {
    executor::HashTask *hash_task =
        static_cast<executor::HashTask *>(task.get());
    hash_task->hash_executor->ExecuteTask(task);
  }
  // XXX This is a hack to let join test pass
  hash_executor->SetChildTiles(result_tile_lists);
  return std::move(hash_executor);
}
}
}
