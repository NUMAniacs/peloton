//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_test.cpp
//
// Identification: test/executor/join_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/harness.h"
#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"

#include "executor/parallel_hash_join_executor.h"
#include "executor/parallel_hash_executor.h"
#include "executor/parallel_seq_scan_executor.h"
#include "executor/merge_join_executor.h"
#include "executor/nested_loop_join_executor.h"
#include "executor/plan_executor.h"

#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/expression_util.h"

#include "planner/parallel_hash_join_plan.h"
#include "planner/parallel_seq_scan_plan.h"
#include "planner/parallel_hash_plan.h"
#include "planner/merge_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/abstract_dependent.h"
#include "common/partition_macros.h"

#include "storage/data_table.h"
#include "storage/tile.h"

#include "concurrency/transaction_manager_factory.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"
#include "executor/parallel_join_tests_util.h"
#include "executor/parallel_seq_scan_tests_util.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

// XXX This is NOT tested in parallel.

class ParallelJoinTests : public PelotonTest {};

// Column ids to be added to logical tile after scan.
const std::vector<oid_t> column_ids({0, 1, 3});

std::vector<PlanNodeType> join_algorithms = {PLAN_NODE_TYPE_PARALLEL_HASHJOIN};

// Whether use custom hashmap or cuckoo hash map
std::vector<bool> use_custom_hashmap = {true, false};

std::vector<PelotonJoinType> join_types = {JOIN_TYPE_INNER
                                           //  , JOIN_TYPE_LEFT,
                                           // JOIN_TYPE_RIGHT,
                                           // JOIN_TYPE_OUTER
};

void ExecuteJoinTest(PlanNodeType join_algorithm, PelotonJoinType join_type,
                     oid_t join_test_type, bool use_custom_hashmap);

enum JOIN_TEST_TYPE {
  BASIC_TEST = 0,
};

TEST_F(ParallelJoinTests, BasicTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    ExecuteJoinTest(join_algorithm, JOIN_TYPE_INNER, BASIC_TEST, false);
  }
}

TEST_F(ParallelJoinTests, CustomHashBasicTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    ExecuteJoinTest(join_algorithm, JOIN_TYPE_INNER, BASIC_TEST, true);
  }
}

// XXX Currently we only support inner join now
// TEST_F(ParallelJoinTests, JoinTypesTest) {
//  // Go over all join algorithms
//  for (auto join_algorithm : join_algorithms) {
//    LOG_INFO("JOIN ALGORITHM :: %s",
//             PlanNodeTypeToString(join_algorithm).c_str());
//    // Go over all join types
//    for (auto join_type : join_types) {
//      LOG_INFO("JOIN TYPE :: %d", join_type);
//      // Execute the join test
//      ExecuteJoinTest(join_algorithm, join_type, BASIC_TEST);
//    }
//  }
//}

void ExecuteJoinTest(PlanNodeType join_algorithm, PelotonJoinType join_type,
                     oid_t join_test_type, bool use_custom_hashmap) {
  // start executor pool
  ExecutorPoolHarness::GetInstance();

  //===--------------------------------------------------------------------===//
  // Setup left and right tables
  //===--------------------------------------------------------------------===//

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t left_table_tile_group_count = 3;
  size_t right_table_tile_group_count = 2;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Left table has 3 tile groups
  std::unique_ptr<storage::DataTable> left_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(
      left_table.get(), tile_group_size * left_table_tile_group_count, false,
      false, false, txn);

  // Right table has 2 tile groups
  std::unique_ptr<storage::DataTable> right_table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(
      right_table.get(), tile_group_size * right_table_tile_group_count, false,
      false, false, txn);

  txn_manager.CommitTransaction(txn);

  LOG_TRACE("%s", left_table->GetInfo().c_str());
  LOG_TRACE("%s", right_table->GetInfo().c_str());

  //===--------------------------------------------------------------------===//
  // Setup join plan nodes and executors and run them
  //===--------------------------------------------------------------------===//

  oid_t result_tuple_count = 0;
  oid_t tuples_with_null = 0;

  // Begin txn
  txn = txn_manager.BeginTransaction();

  auto projection = JoinTestsUtil::CreateProjection();
  // setup the projection schema
  auto schema = JoinTestsUtil::CreateJoinSchema();

  // Construct predicate
  std::unique_ptr<const expression::AbstractExpression> predicate(
      JoinTestsUtil::CreateJoinPredicate());

  switch (join_algorithm) {

    case PLAN_NODE_TYPE_PARALLEL_HASHJOIN: {

      // ================================
      //             Plans
      // ================================

      // Create parallel seq scan node on right table
      std::unique_ptr<planner::ParallelSeqScanPlan> right_seq_scan_node(
          new planner::ParallelSeqScanPlan(right_table.get(), nullptr,
                                           column_ids));

      // Create hash plan node expressions
      expression::AbstractExpression *right_table_attr_1 =
          new expression::TupleValueExpression(common::Type::INTEGER, 1, 1);

      std::vector<std::unique_ptr<const expression::AbstractExpression>>
          hash_keys;
      hash_keys.emplace_back(right_table_attr_1);

      std::vector<std::unique_ptr<const expression::AbstractExpression>>
          right_hash_keys;
      right_hash_keys.emplace_back(
          std::unique_ptr<expression::AbstractExpression>{
              new expression::TupleValueExpression(common::Type::INTEGER, 1,
                                                   1)});

      // Create hash planner node
      std::unique_ptr<planner::ParallelHashPlan> hash_plan_node(
          new planner::ParallelHashPlan(hash_keys, use_custom_hashmap));

      // Create parallel seq scan node on left table
      std::unique_ptr<planner::ParallelSeqScanPlan> left_seq_scan_node(
          new planner::ParallelSeqScanPlan(left_table.get(), nullptr,
                                           column_ids));

      // Create hash join plan node.
      std::unique_ptr<planner::ParallelHashJoinPlan> hash_join_plan_node(
          new planner::ParallelHashJoinPlan(join_type, std::move(predicate),
                                            std::move(projection), schema));
      hash_join_plan_node->AddChild(std::move(left_seq_scan_node));

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

      // Create executor context with empty txn
      std::shared_ptr<executor::ExecutorContext> context(
          new executor::ExecutorContext(txn));

      // Vector of seq scan tasks
      std::vector<std::shared_ptr<executor::AbstractTask>> seq_scan_tasks;
      ParallelSeqScanTestsUtil::GenerateMultiTileGroupTasks(
          right_table.get(), right_seq_scan_node.get(), seq_scan_tasks);

      // Create trackable for seq scan
      size_t num_seq_scan_tasks = seq_scan_tasks.size();
      std::shared_ptr<executor::Trackable> trackable(
          new executor::Trackable(num_seq_scan_tasks));

      // Launch all the tasks
      for (size_t i = 0; i < num_seq_scan_tasks; i++) {
        auto partition_aware_task =
            std::dynamic_pointer_cast<executor::PartitionAwareTask>(
                seq_scan_tasks[i]);
        partition_aware_task->Init(trackable, hash_plan_node.get(),
                                   num_seq_scan_tasks, txn);
        partitioned_executor_thread_pool.SubmitTask(
            partition_aware_task->partition_id,
            executor::ParallelSeqScanExecutor::ExecuteTask,
            std::move(seq_scan_tasks[i]));
      }

      wait->WaitForCompletion();

      executor::HashJoinTask *hash_join_task =
          static_cast<executor::HashJoinTask *>(wait->last_task.get());
      // Validate hash join result tiles
      {
        auto child_tiles = hash_join_task->result_tile_lists;
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
              tuples_with_null +=
                  JoinTestsUtil::CountTuplesWithNullFields(result_tile);
              JoinTestsUtil::ValidateJoinLogicalTile(result_tile);
              LOG_TRACE("%s", result_tile->GetInfo().c_str());
            }
          }
        }
      }

    } break;

    default:
      throw Exception("Unsupported join algorithm : " +
                      std::to_string(join_algorithm));
      break;
  }
  // Commit the txn
  txn_manager.CommitTransaction(txn);

  //===--------------------------------------------------------------------===//
  // Execute test
  //===--------------------------------------------------------------------===//

  if (join_test_type == BASIC_TEST) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 5);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 5);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }
  }
}

}  // namespace test
}  // namespace peloton
