//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_tests_util.cpp
//
// Identification: test/executor/join_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/join_tests_util.h"

#include "common/types.h"
#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "executor/executor_tests_util.h"
#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/expression_util.h"
#include "common/container_tuple.h"
#include "common/harness.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

// Create join predicate
expression::AbstractExpression *JoinTestsUtil::CreateJoinPredicate() {
  expression::AbstractExpression *predicate = nullptr;

  // LEFT.1 == RIGHT.1

  expression::TupleValueExpression *left_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 1);
  expression::TupleValueExpression *right_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 1);

  predicate = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_EQUAL, left_table_attr_1, right_table_attr_1);

  return predicate;
}

std::vector<planner::MergeJoinPlan::JoinClause>
JoinTestsUtil::CreateJoinClauses() {
  std::vector<planner::MergeJoinPlan::JoinClause> join_clauses;
  auto left = expression::ExpressionUtil::TupleValueFactory(
      common::Type::INTEGER, 0, 1);
  auto right = expression::ExpressionUtil::TupleValueFactory(
      common::Type::INTEGER, 1, 1);
  bool reversed = false;
  join_clauses.emplace_back(left, right, reversed);
  return join_clauses;
}

std::shared_ptr<const peloton::catalog::Schema>
JoinTestsUtil::CreateJoinSchema() {
  return std::shared_ptr<const peloton::catalog::Schema>(
      new catalog::Schema({ExecutorTestsUtil::GetColumnInfo(1),
                           ExecutorTestsUtil::GetColumnInfo(1),
                           ExecutorTestsUtil::GetColumnInfo(0),
                           ExecutorTestsUtil::GetColumnInfo(0)}));
}

std::unique_ptr<const planner::ProjectInfo> JoinTestsUtil::CreateProjection() {
  // Create the plan node
  TargetList target_list;
  DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // direct map
  DirectMap direct_map1 = std::make_pair(0, std::make_pair(0, 1));
  DirectMap direct_map2 = std::make_pair(1, std::make_pair(1, 1));
  DirectMap direct_map3 = std::make_pair(2, std::make_pair(1, 0));
  DirectMap direct_map4 = std::make_pair(3, std::make_pair(0, 0));

  direct_map_list.push_back(direct_map1);
  direct_map_list.push_back(direct_map2);
  direct_map_list.push_back(direct_map3);
  direct_map_list.push_back(direct_map4);

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

// Create complicated join predicate
expression::AbstractExpression *
JoinTestsUtil::CreateComplicatedJoinPredicate() {
  expression::AbstractExpression *predicate = nullptr;

  // LEFT.1 == RIGHT.1

  expression::TupleValueExpression *left_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 1);
  expression::TupleValueExpression *right_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 1);

  expression::ComparisonExpression *comp_a =
      new expression::ComparisonExpression(
          EXPRESSION_TYPE_COMPARE_EQUAL, left_table_attr_1, right_table_attr_1);

  // LEFT.3 > 50.0

  expression::TupleValueExpression *left_table_attr_3 =
      new expression::TupleValueExpression(common::Type::DECIMAL, 0, 1);
  expression::ConstantValueExpression *const_val_1 =
      new expression::ConstantValueExpression(
          common::ValueFactory::GetDoubleValue(50.0));
  expression::ComparisonExpression *comp_b =
      new expression::ComparisonExpression(EXPRESSION_TYPE_COMPARE_GREATERTHAN,
                                           left_table_attr_3, const_val_1);

  predicate = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, comp_a, comp_b);

  return predicate;
}

oid_t JoinTestsUtil::CountTuplesWithNullFields(
    executor::LogicalTile *logical_tile) {
  PL_ASSERT(logical_tile);

  // Get column count
  auto column_count = logical_tile->GetColumnCount();
  oid_t tuples_with_null = 0;

  // Go over the tile
  for (auto logical_tile_itr : *logical_tile) {
    const expression::ContainerTuple<executor::LogicalTile> join_tuple(
        logical_tile, logical_tile_itr);

    // Go over all the fields and check for null values
    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
      common::Value val = (join_tuple.GetValue(col_itr));
      if (val.IsNull()) {
        tuples_with_null++;
        break;
      }
    }
  }

  return tuples_with_null;
}

void JoinTestsUtil::ValidateJoinLogicalTile(
    executor::LogicalTile *logical_tile) {
  PL_ASSERT(logical_tile);

  // Get column count
  auto column_count = logical_tile->GetColumnCount();

  // Check # of columns
  EXPECT_EQ(column_count, 4);

  // Check the attribute values
  // Go over the tile
  for (auto logical_tile_itr : *logical_tile) {
    const expression::ContainerTuple<executor::LogicalTile> join_tuple(
        logical_tile, logical_tile_itr);

    // Check the join fields
    common::Value left_tuple_join_attribute_val = (join_tuple.GetValue(0));
    common::Value right_tuple_join_attribute_val = (join_tuple.GetValue(1));
    common::Value cmp = (left_tuple_join_attribute_val.CompareEquals(
        right_tuple_join_attribute_val));
    EXPECT_TRUE(cmp.IsNull() || cmp.IsTrue());
  }
}
void JoinTestsUtil::ExpectEmptyTileResult(MockExecutor *table_scan_executor) {
  // Expect zero result tiles from the child
  EXPECT_CALL(*table_scan_executor, DExecute()).WillOnce(Return(false));
}

void JoinTestsUtil::ExpectMoreThanOneTileResults(
    MockExecutor *table_scan_executor,
    std::vector<std::unique_ptr<executor::LogicalTile>> &
        table_logical_tile_ptrs) {
  // Expect more than one result tiles from the child, but only get one of them
  EXPECT_CALL(*table_scan_executor, DExecute()).WillOnce(Return(true));
  EXPECT_CALL(*table_scan_executor, GetOutput())
      .WillOnce(Return(table_logical_tile_ptrs[0].release()));
}

void JoinTestsUtil::ExpectNormalTileResults(
    size_t table_tile_group_count, MockExecutor *table_scan_executor,
    std::vector<std::unique_ptr<executor::LogicalTile>> &
        table_logical_tile_ptrs) {
  // Return true for the first table_tile_group_count times
  // Then return false after that
  {
    testing::Sequence execute_sequence;
    for (size_t table_tile_group_itr = 0;
         table_tile_group_itr < table_tile_group_count + 1;
         table_tile_group_itr++) {
      // Return true for the first table_tile_group_count times
      if (table_tile_group_itr < table_tile_group_count) {
        EXPECT_CALL(*table_scan_executor, DExecute())
            .InSequence(execute_sequence)
            .WillOnce(Return(true));
      } else  // Return false after that
      {
        EXPECT_CALL(*table_scan_executor, DExecute())
            .InSequence(execute_sequence)
            .WillOnce(Return(false));
      }
    }
  }
  // Return the appropriate logical tiles for the first table_tile_group_count
  // times
  {
    testing::Sequence get_output_sequence;
    for (size_t table_tile_group_itr = 0;
         table_tile_group_itr < table_tile_group_count;
         table_tile_group_itr++) {
      EXPECT_CALL(*table_scan_executor, GetOutput())
          .InSequence(get_output_sequence)
          .WillOnce(
               Return(table_logical_tile_ptrs[table_tile_group_itr].release()));
    }
  }
}

}  // namespace test
}  // namespace peloton
