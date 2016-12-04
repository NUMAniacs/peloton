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

#include "expression/comparison_expression.h"

#include "benchmark/scanbench/scanbench_workload.h"
#include "benchmark/scanbench/scanbench_config.h"
#include "benchmark/scanbench/scanbench_loader.h"

#include "planner/parallel_seq_scan_plan.h"
#include "executor/parallel_seq_scan_executor.h"
#include "executor/plan_executor.h"
#include "common/logger.h"
#include "catalog/catalog.h"
#include "common/exception.h"

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

void ValidateResult(std::shared_ptr<executor::LogicalTileLists> result_tile_lists,
  size_t expected) {
  size_t total_tuples = 0;
  for (auto &partition_tiles : *result_tile_lists) {
    for (auto &tile : partition_tiles) {
      total_tuples += tile->GetTupleCount();
    }
  }

  if (total_tuples != expected)
    throw Exception("Incorrect number of tuples returned. Expected:" + std::to_string(expected)
                    + " Received:" +  std::to_string(total_tuples));
}

void AbstractSelectivityScan(expression::AbstractExpression* predicate,
                             size_t expected_tuples) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Create the plan node
  TargetList target_list;
  // ================================
  //             Plans
  // ================================

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

  std::unordered_map<int, std::tuple<double, size_t, size_t>> exec_histograms;


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

  auto status = txn->GetResult();
  switch (status) {
    case Result::RESULT_SUCCESS:
      // Commit
      LOG_TRACE("Commit Transaction");
      txn_manager.CommitTransaction(txn);
      break;

    case Result::RESULT_FAILURE:
    default:
      // Abort
      LOG_TRACE("Abort Transaction");
      txn_manager.AbortTransaction(txn);
      throw Exception("Transaction should not abort");
  }

  auto end = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());


  ValidateResult(result_tile_lists, expected_tuples);

  for (auto &task : tasks) {
    double old_exec_time;
    size_t old_tg_count, old_answer_tuples;
    auto new_tg_count = static_cast<executor::SeqScanTask *>(task.get())->tile_group_ptrs.size();
    std::tie(old_exec_time, old_tg_count,
             old_answer_tuples) = exec_histograms[task->cpu_id];

    exec_histograms[task->cpu_id] = std::make_tuple(
        old_exec_time+task->exec_time, old_tg_count+new_tg_count,
        old_answer_tuples+task->num_tuples);
  }

  std::stringstream histogram;
  double highest_time = 0;
  for (auto itr=exec_histograms.begin(); itr!=exec_histograms.end(); itr++) {
    auto exec_time = std::get<0>(itr->second);
    histogram << itr->first << " " << exec_time << " "
    << std::get<1>(itr->second) << " " << std::get<2>(itr->second) << std::endl;
    if (exec_time > highest_time)
      highest_time = exec_time;
  }

  LOG_ERROR("\n%sHighest time:%f", histogram.str().c_str(), highest_time);

  state.execution_time_ms = (end-start)/1000;
  LOG_INFO("Parallel Sequential Scan took %fms", state.execution_time_ms);
}

void RunSingleTupleSelectivityScan() {
  // WHERE <second_column> = 10
  expression::AbstractExpression *predicate = new expression::ComparisonExpression(
          EXPRESSION_TYPE_COMPARE_EQUAL,
          new expression::TupleValueExpression(common::Type::INTEGER, 0, 1),
          new expression::ConstantValueExpression(
              common::ValueFactory::GetIntegerValue(10)));
  AbstractSelectivityScan(predicate, 1);
}

void Run1pcSelectivityScan() {
  // WHERE <second_column> < 10
  expression::AbstractExpression *predicate = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_LESSTHAN,
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 1),
      new expression::ConstantValueExpression(
          common::ValueFactory::GetIntegerValue(10)));
  AbstractSelectivityScan(predicate, (SCAN_TABLE_SIZE * state.scale_factor)/100);
}


}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton