//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_seq_scan_test.cpp
//
// Identification: test/executor/parallel_seq_scan_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/harness.h"
#include "common/macros.h"
#include "catalog/schema.h"
#include "common/types.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/parallel_seq_scan_executor.h"
#include "executor/plan_executor.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "planner/seq_scan_plan.h"
#include "planner/parallel_seq_scan_plan.h"
#include "storage/data_table.h"
#include "storage/tile_group_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/parallel_seq_scan_tests_util.h"
#include "executor/mock_executor.h"
#include "executor/abstract_task.h"
#include "common/harness.h"

#include "common/partition_macros.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class ParallelSeqScanTests : public PelotonTest {};

namespace {

/**
 * @brief Set of tuple_ids that will satisfy the predicate in our test cases.
 */
const std::set<oid_t> g_tuple_ids({0, 3});

/**
* @brief Convenience method to extract next tile from executor.
* @param executor Executor to be tested.
*
* @return Logical tile extracted from executor.
*/
executor::LogicalTile *GetNextTile(executor::AbstractExecutor &executor) {
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_THAT(result_tile, NotNull());
  return result_tile.release();
}

/**
 * @brief Generates single tile-group tasks
 */
void GenerateSingleTileGroupTasks(
    storage::DataTable *table, planner::AbstractPlan *node,
    std::vector<std::shared_ptr<executor::AbstractTask>> &tasks) {
  // The result of the logical tiles for all tasks
  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());
  for (size_t p = 0; p < table->GetPartitionCount(); p++) {
    for (size_t tile_group_offset_ = 0;
         tile_group_offset_ < table->GetPartitionTileGroupCount(p);
         tile_group_offset_++) {
      executor::SeqScanTask *seq_scan_task =
          new executor::SeqScanTask(node, tasks.size(), p, result_tile_lists);
      seq_scan_task->tile_group_ptrs.push_back(
          table->GetTileGroupFromPartition(p, tile_group_offset_));
      tasks.push_back(std::shared_ptr<executor::AbstractTask>(seq_scan_task));
    }
  }
}

/**
 * @brief Runs actual test as thread function
 */
void RunTest(ParallelScanArgs **args) {
  UNUSED_ATTRIBUTE auto partition_aware_task =
      static_cast<executor::PartitionAwareTask *>((*args)->task.get());
  LOG_DEBUG("Partition ID:%ld", partition_aware_task->partition_id);
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext((*args)->txn));

  // set the task for this executor
  context->SetTask((*args)->task);
  executor::ParallelSeqScanExecutor executor((*args)->node, context.get());

  EXPECT_TRUE(executor.Init());

  while (executor.Execute() == true) {
    (*args)->result_tiles.emplace_back(GetNextTile(executor));
  }

  EXPECT_FALSE(executor.Execute());
  (*args)->p.set_value(true);
}

void ValidateResults(
    std::vector<std::shared_ptr<executor::LogicalTile>> &result_tiles,
    int expected_num_tiles, int expected_num_cols) {
  // Check correctness of result tiles.
  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_EQ(expected_num_cols, result_tiles[i]->GetColumnCount());

    // Only two tuples per tile satisfy our predicate.
    EXPECT_EQ(g_tuple_ids.size(), result_tiles[i]->GetTupleCount());

    // Verify values.
    std::set<oid_t> expected_tuples_left(g_tuple_ids);
    for (oid_t new_tuple_id : *(result_tiles[i])) {
      // We divide by 10 because we know how PopulatedValue() computes.
      // Bad style. Being a bit lazy here...

      common::Value value1 = (result_tiles[i]->GetValue(new_tuple_id, 0));
      int old_tuple_id = value1.GetAs<int32_t>() / 10;

      EXPECT_EQ(1, expected_tuples_left.erase(old_tuple_id));

      int val1 = ExecutorTestsUtil::PopulatedValue(old_tuple_id, 1);
      common::Value value2 = (result_tiles[i]->GetValue(new_tuple_id, 1));
      EXPECT_EQ(val1, value2.GetAs<int32_t>());
      int val2 = ExecutorTestsUtil::PopulatedValue(old_tuple_id, 3);

      // expected_num_cols - 1 is a hacky way to ensure that
      // we are always getting the last column in the original table.
      // For the tile group test case, it'll be 2 (one column is removed
      // during the scan as part of the test case).
      // For the logical tile test case, it'll be 3.
      common::Value string_value =
          (common::ValueFactory::GetVarcharValue(std::to_string(val2)));
      common::Value val =
          (result_tiles[i]->GetValue(new_tuple_id, expected_num_cols - 1));
      common::Value cmp = (val.CompareEquals(string_value));
      EXPECT_TRUE(cmp.IsTrue());
    }
    EXPECT_EQ(0, expected_tuples_left.size());
  }
}

void ParallelScanTestBody(
    std::unique_ptr<storage::DataTable> table, planner::AbstractPlan *node,
    const size_t num_columns,
    std::vector<std::shared_ptr<executor::AbstractTask>> &tasks) {
  bool status = true;

  std::vector<std::shared_ptr<ParallelScanArgs>> args;
  std::vector<std::shared_ptr<executor::LogicalTile>> result_tiles;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  bridge::BlockingWait wait(tasks.size());

  for (size_t i = 0; i < tasks.size(); i++) {
    auto partition_aware_task =
        std::dynamic_pointer_cast<executor::PartitionAwareTask>(tasks[i]);

    partition_aware_task->Init(&wait, &wait, tasks.size());
    args.push_back(std::shared_ptr<ParallelScanArgs>(
        new ParallelScanArgs(txn, node, tasks[i], false)));
    partitioned_executor_thread_pool.SubmitTask(
        partition_aware_task->partition_id, RunTest, &args[i]->self);
  }

  for (size_t i = 0; i < tasks.size(); i++) {
    status &= args[i]->f.get();
    // concatenate result tiles
    result_tiles.insert(result_tiles.end(), args[i]->result_tiles.begin(),
                        args[i]->result_tiles.end());
  }
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(status, true);
  ValidateResults(result_tiles, table->GetTileGroupCount(), num_columns);
}

// Sequential scan of table with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// partitioning changes midway.
TEST_F(ParallelSeqScanTests, SimpleParallelScanTest) {
  // Start the thread pool
  ExecutorPoolHarness::GetInstance();

  // Create table.
  std::unique_ptr<storage::DataTable> table(
      ParallelSeqScanTestsUtil::CreateTable(PL_NUM_PARTITIONS() * 2));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  // Create plan node.
  planner::ParallelSeqScanPlan node(
      table.get(), ParallelSeqScanTestsUtil::CreatePredicate(g_tuple_ids),
      column_ids);
  // Vector of tasks
  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;
  GenerateSingleTileGroupTasks(table.get(), &node, tasks);

  ParallelScanTestBody(std::move(table), &node, column_ids.size(), tasks);
}

// Sequential scan of table with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// partitioning changes midway.
TEST_F(ParallelSeqScanTests, MultiTileGroupParallelScanTest) {
  // Start the thread pool
  ExecutorPoolHarness::GetInstance();

  // Create table.
  size_t active_tile_group_count =
      PL_NUM_PARTITIONS() * PL_GET_PARTITION_SIZE() * 4;
  std::unique_ptr<storage::DataTable> table(
      ParallelSeqScanTestsUtil::CreateTable(active_tile_group_count));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  // Create plan node.
  planner::ParallelSeqScanPlan node(
      table.get(), ParallelSeqScanTestsUtil::CreatePredicate(g_tuple_ids),
      column_ids);
  // Vector of tasks
  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;
  ParallelSeqScanTestsUtil::GenerateMultiTileGroupTasks(table.get(), &node,
                                                        tasks);

  ParallelScanTestBody(std::move(table), &node, column_ids.size(), tasks);
}
}
}  // namespace test
}  // namespace peloton
