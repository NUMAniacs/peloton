
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
#include "executor/parallel_seq_scan_executor.h"
#include "executor/executor_context.h"
#include "common/init.h"

namespace peloton {
namespace planner {

/*
 * Helper used for parallel join test. When force_single_partition is set to
 * true, it execute only one hash task instead of multiple ones
 */
std::shared_ptr<executor::ParallelHashExecutor>
ParallelHashPlan::DependencyCompleteHelper(
    std::shared_ptr<executor::AbstractTask> task, bool force_single_partition) {

  // Populate tasks for each partition and re-chunk the tiles
  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());

  size_t total_num_tuples = executor::PartitionAwareTask::ReChunkResultTiles(
      task.get(), result_tile_lists, force_single_partition);

  size_t num_tasks = result_tile_lists->size();

  // A list of all tasks to execute
  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;

  auto context = executor::PartitionAwareTask::CopyContext(task.get());

  // Construct the hash executor
  std::shared_ptr<executor::ParallelHashExecutor> hash_executor(
      new executor::ParallelHashExecutor(this, context.get()));

  // Reserve space for hash table
  hash_executor->Reserve(total_num_tuples);
  hash_executor->Init();

  for (size_t task_id = 0; task_id < num_tasks; task_id++) {
    // Construct a hash task
    size_t partition = INVALID_PARTITION_ID;
    if (force_single_partition) {
      partition = 0;
    } else {
      PL_ASSERT(result_tile_lists->at(task_id).size() > 0);
      partition = result_tile_lists->at(task_id)[0]->GetPartition();
    }

    std::shared_ptr<executor::AbstractTask> next_task;
    if (partition != INVALID_PARTITION_ID) {
      next_task.reset(new executor::HashTask(
          this, hash_executor, context, task_id, partition, result_tile_lists));
    }
    next_task->Init(hash_executor.get(), task->dependent->parent_dependent,
                    num_tasks);
    tasks.push_back(next_task);
  }

  if (num_tasks == 0) {
    // No task to do. Immediately notify dependent
    task->dependent->parent_dependent->DependencyComplete(task);
  } else {
    for (auto task : tasks) {
      executor::HashTask *hash_task =
          static_cast<executor::HashTask *>(task.get());
      partitioned_executor_thread_pool.SubmitTask(
          hash_task->partition_id, executor::ParallelHashExecutor::ExecuteTask,
          std::move(task));
    }
  }

  LOG_DEBUG("%d hash tasks submitted", (int)num_tasks);

  // XXX This is a hack to let join test pass
  hash_executor->SetChildTiles(result_tile_lists);
  return std::move(hash_executor);
}
}
}
