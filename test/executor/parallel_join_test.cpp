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
#include "executor/merge_join_executor.h"
#include "executor/nested_loop_join_executor.h"

#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/expression_util.h"

#include "planner/parallel_hash_join_plan.h"
#include "planner/parallel_hash_plan.h"
#include "planner/merge_join_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/abstract_callback.h"

#include "storage/data_table.h"
#include "storage/tile.h"

#include "concurrency/transaction_manager_factory.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

// XXX This is simply a copy of join test. Not tested in parallel.

class ParallelJoinTests : public PelotonTest {};

std::vector<PlanNodeType> join_algorithms = {  // PLAN_NODE_TYPE_MERGEJOIN,
    PLAN_NODE_TYPE_PARALLEL_HASHJOIN};

std::vector<PelotonJoinType> join_types = {JOIN_TYPE_INNER, JOIN_TYPE_LEFT,
                                           JOIN_TYPE_RIGHT, JOIN_TYPE_OUTER};

void ExecuteJoinTest(PlanNodeType join_algorithm, PelotonJoinType join_type,
                     oid_t join_test_type);

void PopulateTileResults(
    size_t tile_group_begin_itr, size_t tile_group_count,
    std::shared_ptr<executor::LogicalTileLists> table_logical_tile_lists,
    size_t task_id, executor::LogicalTileList &table_logical_tile_ptrs);

enum JOIN_TEST_TYPE {
  BASIC_TEST = 0,
  BOTH_TABLES_EMPTY = 1,
  COMPLICATED_TEST = 2,
  SPEED_TEST = 3,
  LEFT_TABLE_EMPTY = 4,
  RIGHT_TABLE_EMPTY = 5,
};

TEST_F(ParallelJoinTests, BasicTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    ExecuteJoinTest(join_algorithm, JOIN_TYPE_INNER, BASIC_TEST);
  }
}

TEST_F(ParallelJoinTests, EmptyTablesTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    ExecuteJoinTest(join_algorithm, JOIN_TYPE_INNER, BOTH_TABLES_EMPTY);
  }
}

TEST_F(ParallelJoinTests, JoinTypesTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      ExecuteJoinTest(join_algorithm, join_type, BASIC_TEST);
    }
  }
}

TEST_F(ParallelJoinTests, ComplicatedTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      ExecuteJoinTest(join_algorithm, join_type, COMPLICATED_TEST);
    }
  }
}

TEST_F(ParallelJoinTests, LeftTableEmptyTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      ExecuteJoinTest(join_algorithm, join_type, LEFT_TABLE_EMPTY);
    }
  }
}

TEST_F(ParallelJoinTests, RightTableEmptyTest) {
  // Go over all join algorithms
  for (auto join_algorithm : join_algorithms) {
    LOG_INFO("JOIN ALGORITHM :: %s",
             PlanNodeTypeToString(join_algorithm).c_str());
    // Go over all join types
    for (auto join_type : join_types) {
      LOG_INFO("JOIN TYPE :: %d", join_type);
      // Execute the join test
      ExecuteJoinTest(join_algorithm, join_type, RIGHT_TABLE_EMPTY);
    }
  }
}

TEST_F(ParallelJoinTests, JoinPredicateTest) {
  oid_t join_test_types = 1;

  // Go over all join test types
  for (oid_t join_test_type = 0; join_test_type < join_test_types;
       join_test_type++) {
    LOG_INFO("JOIN TEST_F ------------------------ :: %u", join_test_type);

    // Go over all join algorithms
    for (auto join_algorithm : join_algorithms) {
      LOG_INFO("JOIN ALGORITHM :: %s",
               PlanNodeTypeToString(join_algorithm).c_str());
      // Go over all join types
      for (auto join_type : join_types) {
        LOG_INFO("JOIN TYPE :: %d", join_type);
        // Execute the join test
        ExecuteJoinTest(join_algorithm, join_type, join_test_type);
      }
    }
  }
}

// XXX We're not supposed to do performance test in unit tests..
// TEST_F(ParallelJoinTests, SpeedTest) {
//  ExecuteJoinTest(PLAN_NODE_TYPE_PARALLEL_HASHJOIN, JOIN_TYPE_OUTER,
// SPEED_TEST);
//  ExecuteJoinTest(PLAN_NODE_TYPE_MERGEJOIN, JOIN_TYPE_OUTER, SPEED_TEST);
//}

void ExecuteJoinTest(PlanNodeType join_algorithm, PelotonJoinType join_type,
                     oid_t join_test_type) {
  //===--------------------------------------------------------------------===//
  // Mock table scan executors
  //===--------------------------------------------------------------------===//

  MockExecutor left_table_scan_executor, right_table_scan_executor;

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

  if (join_test_type == COMPLICATED_TEST) {
    // Modify some values in left and right tables for complicated test
    auto left_source_tile = left_table->GetTileGroup(2)->GetTile(0);
    auto right_dest_tile = right_table->GetTileGroup(1)->GetTile(0);
    auto right_source_tile = left_table->GetTileGroup(0)->GetTile(0);

    auto source_tile_tuple_count = left_source_tile->GetAllocatedTupleCount();
    auto source_tile_column_count = left_source_tile->GetColumnCount();

    // LEFT - 3 rd tile --> RIGHT - 2 nd tile
    for (oid_t tuple_itr = 3; tuple_itr < source_tile_tuple_count;
         tuple_itr++) {
      for (oid_t col_itr = 0; col_itr < source_tile_column_count; col_itr++) {
        common::Value val = (left_source_tile->GetValue(tuple_itr, col_itr));
        right_dest_tile->SetValue(val, tuple_itr, col_itr);
      }
    }

    // RIGHT - 1 st tile --> RIGHT - 2 nd tile
    // RIGHT - 2 nd tile --> RIGHT - 2 nd tile
    for (oid_t col_itr = 0; col_itr < source_tile_column_count; col_itr++) {
      common::Value val1 = (right_source_tile->GetValue(4, col_itr));
      right_dest_tile->SetValue(val1, 0, col_itr);
      common::Value val2 = (right_dest_tile->GetValue(3, col_itr));
      right_dest_tile->SetValue(val2, 2, col_itr);
    }
  }

  // Result of seq scans
  std::shared_ptr<executor::LogicalTileLists> right_table_logical_tile_lists(
      new executor::LogicalTileLists());

  std::vector<std::unique_ptr<executor::LogicalTile>>
      left_table_logical_tile_ptrs;
  std::vector<std::unique_ptr<executor::LogicalTile>>
      right_table_logical_tile_ptrs;

  // Wrap the input tables with logical tiles
  for (size_t left_table_tile_group_itr = 0;
       left_table_tile_group_itr < left_table_tile_group_count;
       left_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> left_table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            left_table->GetTileGroup(left_table_tile_group_itr)));
    left_table_logical_tile_ptrs.push_back(std::move(left_table_logical_tile));
  }

  for (size_t right_table_tile_group_itr = 0;
       right_table_tile_group_itr < right_table_tile_group_count;
       right_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
        executor::LogicalTileFactory::WrapTileGroup(
            right_table->GetTileGroup(right_table_tile_group_itr)));
    right_table_logical_tile_ptrs.push_back(
        std::move(right_table_logical_tile));
  }

  // Left scan executor returns logical tiles from the left table
  EXPECT_CALL(left_table_scan_executor, DInit()).WillOnce(Return(true));

  //===--------------------------------------------------------------------===//
  // Setup left table
  //===--------------------------------------------------------------------===//
  if (join_test_type == BASIC_TEST || join_test_type == COMPLICATED_TEST ||
      join_test_type == SPEED_TEST) {

    JoinTestsUtil::ExpectNormalTileResults(left_table_tile_group_count,
                                           &left_table_scan_executor,
                                           left_table_logical_tile_ptrs);

  } else if (join_test_type == BOTH_TABLES_EMPTY) {
    JoinTestsUtil::ExpectEmptyTileResult(&left_table_scan_executor);
  } else if (join_test_type == LEFT_TABLE_EMPTY) {
    JoinTestsUtil::ExpectEmptyTileResult(&left_table_scan_executor);
  } else if (join_test_type == RIGHT_TABLE_EMPTY) {
    if (join_type == JOIN_TYPE_INNER || join_type == JOIN_TYPE_RIGHT) {
      JoinTestsUtil::ExpectMoreThanOneTileResults(&left_table_scan_executor,
                                                  left_table_logical_tile_ptrs);
    } else {
      JoinTestsUtil::ExpectNormalTileResults(left_table_tile_group_count,
                                             &left_table_scan_executor,
                                             left_table_logical_tile_ptrs);
    }
  }

  //===--------------------------------------------------------------------===//
  // Setup right table
  //===--------------------------------------------------------------------===//

  if (join_test_type == BASIC_TEST || join_test_type == COMPLICATED_TEST ||
      join_test_type == SPEED_TEST) {
    size_t task_id = 0;
    size_t tile_begin_itr = 0;
    PopulateTileResults(tile_begin_itr, right_table_tile_group_count,
                        right_table_logical_tile_lists, task_id,
                        right_table_logical_tile_ptrs);
  } else if (join_test_type == BOTH_TABLES_EMPTY) {
    // Populate an empty result
    size_t task_id = 0;
    size_t tile_begin_itr = 0;
    size_t result_count = 0;
    PopulateTileResults(tile_begin_itr, result_count,
                        right_table_logical_tile_lists, task_id,
                        right_table_logical_tile_ptrs);

  } else if (join_test_type == LEFT_TABLE_EMPTY) {
    if (join_type == JOIN_TYPE_INNER || join_type == JOIN_TYPE_LEFT) {
      // For hash join, we always build the hash table from right child
      if (join_algorithm == PLAN_NODE_TYPE_PARALLEL_HASHJOIN) {
        size_t task_id = 0;
        size_t tile_begin_itr = 0;
        PopulateTileResults(tile_begin_itr, right_table_tile_group_count,
                            right_table_logical_tile_lists, task_id,
                            right_table_logical_tile_ptrs);
      } else {
        // TODO Handle the case for other join types
        // ExpectMoreThanOneTileResults(&right_table_scan_executor,
        //                           right_table_logical_tile_ptrs);
        PL_ASSERT(false);
      }

    } else if (join_type == JOIN_TYPE_OUTER || join_type == JOIN_TYPE_RIGHT) {
      size_t task_id = 0;
      size_t tile_begin_itr = 0;
      PopulateTileResults(tile_begin_itr, right_table_tile_group_count,
                          right_table_logical_tile_lists, task_id,
                          right_table_logical_tile_ptrs);
    }
  } else if (join_test_type == RIGHT_TABLE_EMPTY) {
    // Populate an empty result
    size_t task_id = 0;
    size_t tile_begin_itr = 0;
    size_t result_count = 0;
    PopulateTileResults(tile_begin_itr, result_count,
                        right_table_logical_tile_lists, task_id,
                        right_table_logical_tile_ptrs);
  }

  //===--------------------------------------------------------------------===//
  // Setup join plan nodes and executors and run them
  //===--------------------------------------------------------------------===//

  oid_t result_tuple_count = 0;
  oid_t tuples_with_null = 0;
  auto projection = JoinTestsUtil::CreateProjection();
  // setup the projection schema
  auto schema = JoinTestsUtil::CreateJoinSchema();

  // Construct predicate
  std::unique_ptr<const expression::AbstractExpression> predicate(
      JoinTestsUtil::CreateJoinPredicate());

  // Differ based on join algorithm
  switch (join_algorithm) {

    case PLAN_NODE_TYPE_MERGEJOIN: {
      // Create join clauses
      std::vector<planner::MergeJoinPlan::JoinClause> join_clauses;
      join_clauses = JoinTestsUtil::CreateJoinClauses();

      // Create merge join plan node
      planner::MergeJoinPlan merge_join_node(join_type, std::move(predicate),
                                             std::move(projection), schema,
                                             join_clauses);

      // Construct the merge join executor
      executor::MergeJoinExecutor merge_join_executor(&merge_join_node,
                                                      nullptr);

      // Construct the executor tree
      merge_join_executor.AddChild(&left_table_scan_executor);
      merge_join_executor.AddChild(&right_table_scan_executor);

      // Run the merge join executor
      EXPECT_TRUE(merge_join_executor.Init());
      while (merge_join_executor.Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_logical_tile(
            merge_join_executor.GetOutput());

        if (result_logical_tile != nullptr) {
          result_tuple_count += result_logical_tile->GetTupleCount();
          tuples_with_null += JoinTestsUtil::CountTuplesWithNullFields(
              result_logical_tile.get());
          JoinTestsUtil::ValidateJoinLogicalTile(result_logical_tile.get());
          LOG_TRACE("%s", result_logical_tile->GetInfo().c_str());
        }
      }

    } break;

    case PLAN_NODE_TYPE_PARALLEL_HASHJOIN: {
      // Create hash plan node
      expression::AbstractExpression *right_table_attr_1 =
          new expression::TupleValueExpression(common::Type::INTEGER, 1, 1);

      std::vector<std::unique_ptr<const expression::AbstractExpression>>
          hash_keys;
      hash_keys.emplace_back(right_table_attr_1);

      std::vector<std::unique_ptr<const expression::AbstractExpression>>
          left_hash_keys;
      left_hash_keys.emplace_back(
          std::unique_ptr<expression::AbstractExpression>{
              new expression::TupleValueExpression(common::Type::INTEGER, 0,
                                                   1)});

      std::vector<std::unique_ptr<const expression::AbstractExpression>>
          right_hash_keys;
      right_hash_keys.emplace_back(
          std::unique_ptr<expression::AbstractExpression>{
              new expression::TupleValueExpression(common::Type::INTEGER, 1,
                                                   1)});

      // Create hash plan node
      planner::ParallelHashPlan hash_plan_node(hash_keys);

      // Create hash join plan node.
      planner::ParallelHashJoinPlan hash_join_plan_node(
          join_type, std::move(predicate), std::move(projection), schema);

      // Set the dependent parent
      hash_plan_node.SetDependentParent(&hash_join_plan_node);

      // Assume the last seq scan task has finished
      size_t task_id = 0;
      size_t partition_id = 0;
      // XXX Passing a nullptr to make compiler happy
      std::shared_ptr<executor::AbstractTask> task(
          new executor::SeqScanTask(&hash_plan_node, task_id, partition_id,
                                    right_table_logical_tile_lists));

      //  XXX Temporary reference to make sure hash executor is not destructed..
      std::shared_ptr<executor::ParallelHashExecutor> hash_executor =
          hash_plan_node.TaskComplete(task, true);

      // Construct the hash join executor
      executor::ParallelHashJoinExecutor hash_join_executor(
          &hash_join_plan_node, nullptr);

      // Construct the executor tree
      hash_join_executor.AddChild(&left_table_scan_executor);
      PL_ASSERT(hash_executor != nullptr);
      hash_join_executor.AddChild(hash_executor.get());

      // Init the executor tree
      EXPECT_TRUE(hash_join_executor.Init());

      while (hash_join_executor.Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_logical_tile(
            hash_join_executor.GetOutput());

        if (result_logical_tile != nullptr) {
          result_tuple_count += result_logical_tile->GetTupleCount();
          tuples_with_null += JoinTestsUtil::CountTuplesWithNullFields(
              result_logical_tile.get());
          JoinTestsUtil::ValidateJoinLogicalTile(result_logical_tile.get());
          LOG_TRACE("%s", result_logical_tile->GetInfo().c_str());
        }
      }

    } break;

    default:
      throw Exception("Unsupported join algorithm : " +
                      std::to_string(join_algorithm));
      break;
  }

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

  } else if (join_test_type == BOTH_TABLES_EMPTY) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }

  } else if (join_test_type == COMPLICATED_TEST) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 17);
        EXPECT_EQ(tuples_with_null, 7);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 17);
        EXPECT_EQ(tuples_with_null, 7);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }

  } else if (join_test_type == LEFT_TABLE_EMPTY) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 10);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 10);
        EXPECT_EQ(tuples_with_null, 10);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }
  } else if (join_test_type == RIGHT_TABLE_EMPTY) {
    // Check output
    switch (join_type) {
      case JOIN_TYPE_INNER:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_LEFT:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 15);
        break;

      case JOIN_TYPE_RIGHT:
        EXPECT_EQ(result_tuple_count, 0);
        EXPECT_EQ(tuples_with_null, 0);
        break;

      case JOIN_TYPE_OUTER:
        EXPECT_EQ(result_tuple_count, 15);
        EXPECT_EQ(tuples_with_null, 15);
        break;

      default:
        throw Exception("Unsupported join type : " + std::to_string(join_type));
        break;
    }
  }
}

void PopulateTileResults(
    size_t tile_group_begin_itr, size_t tile_group_count,
    std::shared_ptr<executor::LogicalTileLists> table_logical_tile_lists,
    size_t task_id, executor::LogicalTileList &table_logical_tile_ptrs) {

  while (task_id >= table_logical_tile_lists->size()) {
    table_logical_tile_lists->push_back(executor::LogicalTileList());
  }
  // Move the logical tiles for the task with task_id
  for (size_t tile_group_itr = tile_group_begin_itr;
       tile_group_itr < tile_group_count; tile_group_itr++) {
    (*table_logical_tile_lists)[task_id]
        .emplace_back(table_logical_tile_ptrs[tile_group_itr].release());
  }
}
}  // namespace test
}  // namespace peloton
