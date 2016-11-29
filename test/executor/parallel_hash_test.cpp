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
#include "planner/parallel_seq_scan_plan.h"
#include "executor/plan_executor.h"

#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/expression_util.h"

#include "planner/parallel_hash_join_plan.h"
#include "planner/parallel_hash_plan.h"
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

namespace peloton {
namespace test {

class ParallelHashTests : public PelotonTest {};

/**
 * @brief Set of tuple_ids that will satisfy the predicate in our test cases.
 */
const std::set<oid_t> g_tuple_ids({0, 3});

// Column ids to be added to logical tile after scan.
const std::vector<oid_t> column_ids({0, 1, 3});

TEST_F(ParallelHashTests, BasicTest) {
  // start executor pool
  ExecutorPoolHarness::GetInstance();

  // Create table
  size_t active_tile_group_count =
      PL_NUM_PARTITIONS() * PL_GET_PARTITION_SIZE() * 4;
  std::unique_ptr<storage::DataTable> table(
      ParallelSeqScanTestsUtil::CreateTable(active_tile_group_count));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  //===--------------------------------------------------------------------===//
  // Setup hash plan nodes and executors and run them
  //===--------------------------------------------------------------------===//

  // Create a blocking wait at the top
  std::unique_ptr<bridge::BlockingWait> wait(new bridge::BlockingWait(1));

  // Create parallel seq scan node
  std::unique_ptr<planner::ParallelSeqScanPlan> seq_scan_node(
      new planner::ParallelSeqScanPlan(
          table.get(), ParallelSeqScanTestsUtil::CreatePredicate(g_tuple_ids),
          column_ids));

  // Create hash keys for hash plan node
  expression::AbstractExpression *table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  hash_keys.emplace_back(table_attr_1);

  // Create hash plan node
  std::unique_ptr<planner::ParallelHashPlan> hash_plan_node(
      new planner::ParallelHashPlan(hash_keys));

  // Set the dependent of plans MANUALLY
  seq_scan_node->parent_dependent = hash_plan_node.get();
  hash_plan_node->parent_dependent = wait.get();

  // Create executor context
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Create seq scan executor
  // We probably don't need this executor instantiated
  std::shared_ptr<executor::ParallelSeqScanExecutor> seq_scan_executor(
      new executor::ParallelSeqScanExecutor(seq_scan_node.get(),
                                            context.get()));

  // Vector of seq scan tasks
  std::vector<std::shared_ptr<executor::AbstractTask>> seq_scan_tasks;
  ParallelSeqScanTestsUtil::GenerateMultiTileGroupTasks(
      table.get(), seq_scan_node.get(), seq_scan_tasks);

  size_t num_seq_scan_tasks = seq_scan_tasks.size();
  for (size_t i = 0; i < num_seq_scan_tasks; i++) {
    auto partition_aware_task =
        std::dynamic_pointer_cast<executor::PartitionAwareTask>(
            seq_scan_tasks[i]);
    partition_aware_task->Init(seq_scan_executor.get(), hash_plan_node.get(),
                               num_seq_scan_tasks);
    partitioned_executor_thread_pool.SubmitTask(
        partition_aware_task->partition_id,
        executor::ParallelSeqScanExecutor::ExecuteTask,
        std::move(seq_scan_tasks[i]));
  }

  wait->WaitForCompletion();
  txn_manager.CommitTransaction(txn);

  // TODO Because we don't have the reference to the hash executor, so we cannot
  // validate the number of tuples in the hash table now..
  //  while (true) {
  //    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  //    size_t num_tuples = hash_executor->GetTotalNumTuples();
  //    EXPECT_TRUE(tile_group_size * tile_group_count >= num_tuples);
  //    // All tuples have been processed
  //    if (tile_group_size * tile_group_count == num_tuples) {
  //      break;
  //    }
  //  }
}

}  // namespace test
}  // namespace peloton
