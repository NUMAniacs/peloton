//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.cpp
//
// Identification: src/main/ycsb/ycsb_workload.cpp
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

#include "benchmark/numabench/numabench_workload.h"
#include "benchmark/numabench/numabench_configuration.h"
#include "benchmark/numabench/numabench_loader.h"

#include "planner/parallel_seq_scan_plan.h"
#include "planner/parallel_hash_plan.h"
#include "planner/parallel_hash_join_plan.h"

#include "executor/parallel_seq_scan_executor.h"

#include "executor/plan_executor.h"

#include "common/logger.h"

#include "catalog/catalog.h"

namespace peloton {
namespace benchmark {
namespace numabench {

extern storage::DataTable *left_table;
extern storage::DataTable *right_table;

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;

void RunWorkload() {
  // Execute the workload to build the log
  RunHashJoin();
}

void RunHashJoin() {

  int result_tuple_count = 0;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Create the plan node
  TargetList target_list;
  DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // direct map
  direct_map_list.push_back(std::make_pair(0, std::make_pair(0, 0)));
  direct_map_list.push_back(std::make_pair(1, std::make_pair(0, 1)));
  direct_map_list.push_back(std::make_pair(2, std::make_pair(1, 0)));
  direct_map_list.push_back(std::make_pair(3, std::make_pair(1, 1)));
  direct_map_list.push_back(std::make_pair(3, std::make_pair(1, 2)));

  auto projection =
      std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
          std::move(target_list), std::move(direct_map_list)));

  expression::TupleValueExpression *left_table_expr =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 1);
  expression::TupleValueExpression *right_table_expr =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 2);

  auto predicate = std::unique_ptr<expression::AbstractExpression>(
      new expression::ComparisonExpression(EXPRESSION_TYPE_COMPARE_EQUAL,
                                           left_table_expr, right_table_expr));

  // schema
  // TODO: who is the primary key?????
  auto p_id_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "p_id", true);
  auto p_partkey_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "p_partkey", true);
  auto l_id_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "l_id", true);
  auto l_shipdate_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "l_shipdate", true);
  auto l_partkey_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "l_partkey", true);

  auto schema = std::shared_ptr<catalog::Schema>(new catalog::Schema(
      {p_id_col, p_partkey_col, l_id_col, l_shipdate_col, l_partkey_col}));

  // ================================
  //             Plans
  // ================================

  // this is inefficient but is closer to what TPC-H Q14 actually does
  auto right_predicate = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND,
      new expression::ComparisonExpression(
          EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
          new expression::TupleValueExpression(common::Type::INTEGER, 0, 2),
          new expression::ConstantValueExpression(
              common::ValueFactory::GetIntegerValue(0))),
      new expression::ComparisonExpression(
          EXPRESSION_TYPE_COMPARE_LESSTHAN,
          new expression::TupleValueExpression(common::Type::INTEGER, 0, 2),
          new expression::ConstantValueExpression(
              common::ValueFactory::GetIntegerValue(state.selectivity))));

  // Create parallel seq scan node on right table
  std::unique_ptr<planner::ParallelSeqScanPlan> right_seq_scan_node(
      new planner::ParallelSeqScanPlan(right_table, right_predicate,
                                       std::vector<oid_t>({0, 1, 2})));
  right_seq_scan_node->random_partition_execution =
      state.random_partition_execution;

  // Create hash plan node expressions
  expression::AbstractExpression *right_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  hash_keys.emplace_back(right_table_attr_1);

  // Create hash planner node
  std::unique_ptr<planner::ParallelHashPlan> hash_plan_node(
      new planner::ParallelHashPlan(hash_keys, state.custom_hashtable, state.partition_by_join_key));
  hash_plan_node->random_partition_execution = state.random_partition_execution;

  // Create parallel seq scan node on left table
  std::unique_ptr<planner::ParallelSeqScanPlan> left_seq_scan_node(
      new planner::ParallelSeqScanPlan(left_table, nullptr,
                                       std::vector<oid_t>({0, 1})));

  // Create hash join plan node.
  std::unique_ptr<planner::ParallelHashJoinPlan> hash_join_plan_node(
      new planner::ParallelHashJoinPlan(JOIN_TYPE_INNER, std::move(predicate),
                                        std::move(projection), schema));
  hash_join_plan_node->AddChild(std::move(left_seq_scan_node));
  hash_join_plan_node->random_partition_execution =
      state.random_partition_execution;

  // Create a blocking wait at the top of hash executor because the hash
  // join executor is not ready yet..
  std::unique_ptr<bridge::BlockingWait> wait(new bridge::BlockingWait());

  // Set the dependent of hash plan MANUALLY
  right_seq_scan_node->parent_dependent = hash_plan_node.get();
  hash_plan_node->parent_dependent = hash_join_plan_node.get();
  hash_join_plan_node->parent_dependent = wait.get();

  // ================================
  //         Executors
  // ================================

  auto begin = std::chrono::high_resolution_clock::now();
  // Create executor context with empty txn
  auto txn = state.read_only_txn ? txn_manager.BeginReadonlyTransaction()
                                 : txn_manager.BeginTransaction();

  std::shared_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // === Start of right seq scan tasks generation ====
  right_seq_scan_node->RecordTaskGenStart();
  std::vector<std::shared_ptr<executor::AbstractTask>> seq_scan_tasks;
  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());

  right_seq_scan_node->GenerateTasks(seq_scan_tasks,result_tile_lists);
  LOG_ERROR("INITIAL TASKS GENERATED");
  // Create trackable for seq scan
  size_t num_seq_scan_tasks = seq_scan_tasks.size();
  std::shared_ptr<executor::Trackable> trackable(
      new executor::Trackable(num_seq_scan_tasks));

  LOG_DEBUG("Number of seq scan tasks created: %lu", seq_scan_tasks.size());
  right_seq_scan_node->RecordTaskGenEnd();
  // === End of right seq scan tasks generation ====

  // === Begin of seq scan tasks generation ====
  right_seq_scan_node->RecordTaskExecutionStart();
  // Launch all the tasks
  for (size_t i = 0; i < num_seq_scan_tasks; i++) {
    auto partition_aware_task =
        std::dynamic_pointer_cast<executor::PartitionAwareTask>(
            seq_scan_tasks[i]);
    partition_aware_task->Init(trackable, right_seq_scan_node->parent_dependent,
                               num_seq_scan_tasks, txn);

    if (state.random_partition_execution) {
      partitioned_executor_thread_pool.SubmitTaskRandom(
          executor::ParallelSeqScanExecutor::ExecuteTask,
          std::move(seq_scan_tasks[i]));
    } else {
      partitioned_executor_thread_pool.SubmitTask(
          partition_aware_task->partition_id,
          executor::ParallelSeqScanExecutor::ExecuteTask,
          std::move(seq_scan_tasks[i]));
    }
  }
  wait->WaitForCompletion();
  // Validate hash join result tiles
  {
    auto child_tiles = wait->last_task->result_tile_lists;
    auto num_tasks = child_tiles->size();
    // For all tasks
    for (size_t task_itr = 0; task_itr < num_tasks; task_itr++) {
      auto &target_tile_list = (*child_tiles)[task_itr];
      // For all tiles of this task
      for (size_t tile_itr = 0; tile_itr < target_tile_list.size();
           tile_itr++) {
        if (target_tile_list[tile_itr]->GetTupleCount() > 0) {
          auto result_tile = target_tile_list[tile_itr].get();
          result_tuple_count += result_tile->GetTupleCount();
          LOG_TRACE("%s", result_tile->GetInfo().c_str());
        }
      }
    }
  }
  // ==== End of result collecting ===

  auto end = std::chrono::high_resolution_clock::now();

  state.execution_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
          .count();
  LOG_ERROR("Result_Tuples: %d", result_tuple_count);
  LOG_ERROR("Parallel Hash Join took %ldms", state.execution_time_ms);
  auto &breakdowns = state.execution_time_breakdown;
  breakdowns.push_back(right_seq_scan_node->GetTaskGenTimeMS());
  breakdowns.push_back(right_seq_scan_node->GetTaskExecutionTimeMS());
  breakdowns.push_back(hash_plan_node->GetTaskGenTimeMS());
  breakdowns.push_back(hash_plan_node->GetTaskExecutionTimeMS());
  breakdowns.push_back(hash_join_plan_node->GetTaskGenTimeMS());
  breakdowns.push_back(hash_join_plan_node->GetTaskExecutionTimeMS());
}

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
