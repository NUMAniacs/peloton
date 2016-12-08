
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

void ParallelHashPlan::DependencyComplete(
    std::shared_ptr<executor::AbstractTask> task) {

  // Populate tasks for each partition and re-chunk the tiles
  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());
  this->RecordTaskGenStart();

  // Rechunk the tasks
  std::vector<size_t> rechunk_tuples_per_partition = executor::PartitionAwareTask::ReChunkResultTiles(
      task.get(), result_tile_lists);
  std::vector<size_t> tuples_reservation_per_partition;
  if (partition_by_same_key) {
    tuples_reservation_per_partition.resize(rechunk_tuples_per_partition.size());
    for (size_t i = 0; i < rechunk_tuples_per_partition.size(); ++i) {
      tuples_reservation_per_partition[i] = 2 * rechunk_tuples_per_partition[i];
    }
  } else {
    tuples_reservation_per_partition.resize(1);
    for (size_t i = 0; i < rechunk_tuples_per_partition.size(); ++i) {
      tuples_reservation_per_partition[0] += rechunk_tuples_per_partition[i];
    }
    tuples_reservation_per_partition[0] = 2 * tuples_reservation_per_partition[0];
  }
  size_t num_tasks = result_tile_lists->size();

  // A list of all tasks to execute
  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;

  // Construct the hash executor
  std::shared_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(task->txn));
  std::shared_ptr<executor::ParallelHashExecutor> hash_executor(
      new executor::ParallelHashExecutor(this, context.get()));
  // Set the result of hash executor
  hash_executor->child_tiles = result_tile_lists;

  // Reserve space for hash table
  hash_executor->Init();
  hash_executor->Reserve(tuples_reservation_per_partition);

  // Construct trackable object
  std::shared_ptr<executor::Trackable> trackable(
      new executor::Trackable(num_tasks));

  for (size_t task_id = 0; task_id < num_tasks; task_id++) {
    // Construct a hash task
    size_t partition = INVALID_PARTITION_ID;

    PL_ASSERT(result_tile_lists->at(task_id).size() > 0);
    partition = result_tile_lists->at(task_id)[0]->GetPartition();

    std::shared_ptr<executor::AbstractTask> next_task;
    if (partition != INVALID_PARTITION_ID) {
      next_task.reset(new executor::HashTask(this, hash_executor, task_id,
                                             partition, result_tile_lists));
    }
    next_task->Init(trackable, task->dependent->parent_dependent, num_tasks,
                    task->txn);
    tasks.push_back(next_task);
  }

  // === End of task generation ====-
  this->RecordTaskGenEnd();
  this->RecordTaskExecutionStart();

  if (num_tasks == 0) {
    // No task to do. Immediately notify dependent
    task->dependent->parent_dependent->DependencyComplete(task);
    return;
  }

  for (auto new_task : tasks) {
    executor::HashTask *hash_task =
        static_cast<executor::HashTask *>(new_task.get());
    if (this->random_partition_execution) {
      partitioned_executor_thread_pool.SubmitTaskRandom(
          executor::ParallelHashExecutor::ExecuteTask, std::move(new_task));
    } else {
      partitioned_executor_thread_pool.SubmitTask(
          hash_task->partition_id, executor::ParallelHashExecutor::ExecuteTask,
          std::move(new_task));
    }
  }
  LOG_DEBUG("%d hash tasks submitted", (int)num_tasks);
}
}
}
