
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// parallel_hash_join_plan.cpp
//
// Identification: /peloton/src/planner/parallel_hash_join_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/partition_macros.h"
#include "expression/abstract_expression.h"
#include "planner/project_info.h"
#include "planner/parallel_seq_scan_plan.h"
#include "planner/parallel_hash_join_plan.h"
#include "executor/parallel_hash_join_executor.h"
#include "executor/parallel_seq_scan_executor.h"
#include "executor/abstract_task.h"
#include "executor/parallel_hash_executor.h"
#include "executor/executor_context.h"
#include "common/init.h"

namespace peloton {
namespace planner {

ParallelHashJoinPlan::ParallelHashJoinPlan(
    PelotonJoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    const std::shared_ptr<const catalog::Schema> &proj_schema)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {}

ParallelHashJoinPlan::ParallelHashJoinPlan(
    PelotonJoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    const std::shared_ptr<const catalog::Schema> &proj_schema,
    const std::vector<oid_t> &outer_hashkeys)  // outer_hashkeys is added for
                                               // IN-subquery
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {
  outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
}

void ParallelHashJoinPlan::DependencyComplete(
    std::shared_ptr<executor::AbstractTask> task) {

  executor::HashTask *hash_task = static_cast<executor::HashTask *>(task.get());

  std::shared_ptr<executor::LogicalTileLists> seq_scan_result_tile_lists(
      new executor::LogicalTileLists());
  // A list of all tasks to execute
  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;

  // TODO We should generate the tasks by walking down the tree
  // XXX Assume the left child is the seq scan plan
  planner::ParallelSeqScanPlan *parallel_seq_scan_plan =
      static_cast<planner::ParallelSeqScanPlan *>(GetChildren()[0].get());
  parallel_seq_scan_plan->GenerateTasks(tasks, seq_scan_result_tile_lists);

  // Construct trackable object
  size_t num_tasks = tasks.size();
  std::shared_ptr<executor::Trackable> trackable(
      new executor::Trackable(num_tasks));

  std::shared_ptr<executor::LogicalTileLists> hash_join_result_tile_lists(
      new executor::LogicalTileLists());

  // No task to do. Immediately notify dependent
  if (num_tasks == 0) {
    task->dependent->parent_dependent->DependencyComplete(task);
    return;
  }

  for (auto new_task : tasks) {
    executor::SeqScanTask *seq_scan_task =
        static_cast<executor::SeqScanTask *>(new_task.get());
    std::shared_ptr<executor::HashJoinTask> hash_join_task(
        new executor::HashJoinTask(
            this, hash_task->hash_executor, seq_scan_task->task_id,
            seq_scan_task->partition_id, hash_join_result_tile_lists));

    // Initialize tasks
    hash_join_task->Init(trackable, this->parent_dependent, num_tasks,
                         task->txn);
    seq_scan_task->Init(nullptr, nullptr, num_tasks, task->txn);

    // Passing the hash task for hash join executor so that we have a
    // reference to the hash table
    partitioned_executor_thread_pool.SubmitTask(
        seq_scan_task->partition_id,
        executor::ParallelHashJoinExecutor::ExecuteTask, std::move(new_task),
        std::move(hash_join_task));
  }
  LOG_DEBUG("%d hash join tasks submitted", (int)num_tasks);
}
}
}
