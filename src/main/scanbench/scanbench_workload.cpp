//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scanbench_workload.cpp
//
// Identification: src/main/scanbench/scanbench_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "expression/conjunction_expression.h"

#include "benchmark/scanbench/scanbench_workload.h"
#include "benchmark/scanbench/scanbench_config.h"
#include "benchmark/scanbench/scanbench_loader.h"

#include "planner/parallel_seq_scan_plan.h"

#include "executor/parallel_seq_scan_executor.h"

#include "executor/plan_executor.h"

#include "common/logger.h"

#include "catalog/catalog.h"

namespace peloton {
namespace benchmark {
namespace scanbench {

extern storage::DataTable *scan_table;

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;


void RunSingleTupleSelectivityScan() {

  int result_tuple_count = 0;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Create the plan node
  TargetList target_list;
  // ================================
  //             Plans
  // ================================

  // WHERE <second_column> = 10
  auto predicate = new expression::ComparisonExpression(
          EXPRESSION_TYPE_COMPARE_EQUAL,
          new expression::TupleValueExpression(common::Type::INTEGER, 0, 1),
          new expression::ConstantValueExpression(
              common::ValueFactory::GetIntegerValue(10)));

  // Create parallel seq scan node on right table
  std::unique_ptr<planner::ParallelSeqScanPlan> scan_node(
      new planner::ParallelSeqScanPlan(scan_table, predicate,
                                       std::vector<oid_t>( { 0, 1, 2 })));

  // ================================
  //         Executors
  // ================================

  auto start = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());

  // Create executor context with empty txn
  auto txn = state.read_only_txn ? txn_manager.BeginReadonlyTransaction() :
             txn_manager.BeginTransaction();

  std::shared_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Vector of seq scan tasks
  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;

  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());
  scan_node->GenerateTasks(tasks, result_tile_lists);

  LOG_DEBUG("Number of seq scan tasks created: %ld", tasks.size());

  // create blocking wait for all scan tasks to finish
  bridge::BlockingWait wait;

  // Create trackable for seq scan
  std::shared_ptr<executor::Trackable> trackable(
      new executor::Trackable(tasks.size()));


  // Launch all the tasks
  for (size_t i = 0; i < tasks.size(); i++) {
    auto partition_aware_task = static_cast<executor::PartitionAwareTask *>(tasks[i].get());
    partition_aware_task->Init(trackable, &wait, tasks.size(), txn);
    partitioned_executor_thread_pool.SubmitTask(
        partition_aware_task->partition_id,
        executor::ParallelSeqScanExecutor::ExecuteTask,
        std::move(tasks[i]));
  }

  wait.WaitForCompletion();

  auto end = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());

  state.execution_time_ms = (end-start)/1000;
  LOG_INFO("Result_Tuples: %d", result_tuple_count);
  LOG_INFO("Parallel Hash Join took %fms", state.execution_time_ms);
}

}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton