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

namespace peloton {
namespace test {

class ParallelHashTests : public PelotonTest {};

TEST_F(ParallelHashTests, BasicTest) {
  // start executor pool
  ExecutorPoolHarness::GetInstance();

  // Create a table and wrap it in logical tile
  size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
  size_t tile_group_count = 10;
  size_t num_seq_scan_tasks = PL_NUM_PARTITIONS();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Table has 10 tile groups
  std::unique_ptr<storage::DataTable> table(
      ExecutorTestsUtil::CreateTable(tile_group_size));
  ExecutorTestsUtil::PopulateTable(table.get(),
                                   tile_group_size * tile_group_count, false,
                                   false, false, txn);

  txn_manager.CommitTransaction(txn);

  LOG_TRACE("%s", table->GetInfo().c_str());

  // Result of seq scans
  std::shared_ptr<executor::LogicalTileLists> table_logical_tile_lists(
      new executor::LogicalTileLists());

  std::vector<std::unique_ptr<executor::LogicalTile>> table_logical_tile_ptrs;

  // Wrap the input tables with logical tiles
  for (size_t table_tile_group_itr = 0; table_tile_group_itr < tile_group_count;
       table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            table->GetTileGroup(table_tile_group_itr), UNDEFINED_NUMA_REGION));
    table_logical_tile_ptrs.push_back(std::move(table_logical_tile));
  }

  //===--------------------------------------------------------------------===//
  // Setup child tile results
  //===--------------------------------------------------------------------===//
  {
    size_t tile_begin_itr = 0;
    size_t num_tile_group_per_task = tile_group_count / num_seq_scan_tasks;
    // Split the populated right table tiles into multiple tasks
    for (size_t task_id = 0; task_id < num_seq_scan_tasks; task_id++) {

      // XXX Assume partition == task_id
      size_t partition = task_id;
      if (task_id != num_seq_scan_tasks - 1) {
        // The first few tasks have the same number of tiles
        ParallelJoinTestsUtil::PopulateTileResults(
            tile_begin_itr, num_tile_group_per_task, table_logical_tile_lists,
            task_id, partition, table_logical_tile_ptrs);
        tile_begin_itr += num_tile_group_per_task;
      } else {
        // The last task
        ParallelJoinTestsUtil::PopulateTileResults(
            tile_begin_itr, tile_group_count - tile_begin_itr,
            table_logical_tile_lists, task_id, partition,
            table_logical_tile_ptrs);
      }
    }
  }

  //===--------------------------------------------------------------------===//
  // Setup hash plan nodes and executors and run them
  //===--------------------------------------------------------------------===//

  // Create hash plan node
  expression::AbstractExpression *table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  hash_keys.emplace_back(table_attr_1);

  // Create hash plan node
  planner::ParallelHashPlan hash_plan_node(hash_keys);

  // Create seq scan executor
  std::shared_ptr<executor::ParallelSeqScanExecutor> seq_scan_executor(
      new executor::ParallelSeqScanExecutor(nullptr, nullptr));

  // Create hash executor
  std::shared_ptr<executor::ParallelHashExecutor> hash_executor(
      new executor::ParallelHashExecutor(&hash_plan_node, nullptr));

  std::vector<std::shared_ptr<executor::AbstractTask>> seq_scan_tasks;
  for (size_t task_id = 0; task_id < num_seq_scan_tasks; task_id++) {

    // Create dummy seq scan task
    std::shared_ptr<executor::AbstractTask> task(new executor::SeqScanTask(
        &hash_plan_node, INVALID_TASK_ID, INVALID_PARTITION_ID,
        table_logical_tile_lists));

    // Init task with num tasks
    task->Init(seq_scan_executor.get(), &hash_plan_node, num_seq_scan_tasks);

    // Insert to the list
    seq_scan_tasks.push_back(task);
  }

  seq_scan_executor->SetNumTasks(num_seq_scan_tasks);

  // Loop until the last seq scan task completes
  for (size_t task_id = 0; task_id < num_seq_scan_tasks; task_id++) {
    auto task = seq_scan_tasks[task_id];
    if (task->trackable->TaskComplete()) {
      PL_ASSERT(task_id == num_seq_scan_tasks - 1);
      hash_executor = hash_plan_node.DependencyCompleteHelper(task, false);
    }
  }

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    size_t num_tuples = hash_executor->GetTotalNumTuples();
    EXPECT_TRUE(tile_group_size * tile_group_count >= num_tuples);
    // All tuples have been processed
    if (tile_group_size * tile_group_count == num_tuples) {
      break;
    }
  }
}

}  // namespace test
}  // namespace peloton
