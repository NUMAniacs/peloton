//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_join_tests_util.cpp
//
// Identification: test/executor/parallel_join_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/parallel_seq_scan_tests_util.h"

#include "common/types.h"
#include "executor/executor_tests_util.h"
#include "executor/logical_tile.h"
#include "common/harness.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

namespace peloton {
namespace test {

/**
 * @brief Convenience method to create table for test.
 *
 * @return Table generated for test.
 */
storage::DataTable *ParallelSeqScanTestsUtil::CreateTable(
    size_t active_tile_group_count) {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  // have double the number of active tile groups as partitions
  storage::DataTable::SetActiveTileGroupCount(active_tile_group_count);
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable());

  // populate all tile groups
  for (size_t p = 0; p < table->GetPartitionCount(); p++) {
    for (size_t tile_group_offset_ = 0;
         tile_group_offset_ < table->GetPartitionTileGroupCount(p);
         tile_group_offset_++) {
      ExecutorTestsUtil::PopulateTiles(
          table->GetTileGroupFromPartition(p, tile_group_offset_), tuple_count);
    }
  }

  return table.release();
}

/**
* @brief Convenience method to create predicate for test.
* @param tuple_ids Set of tuple ids that we want the predicate to match with.
*
* The predicate matches any tuple with ids in the specified set.
* This assumes that the table was populated with PopulatedValue() in
* ExecutorTestsUtil.
*
* Each OR node has an equality node to its right and another OR node to
* its left. The leftmost leaf is a FALSE constant value expression.
*
* In each equality node, we either use (arbitrarily taking reference from the
* parity of the loop iteration) the first field or last field of the tuple.
*/
expression::AbstractExpression *ParallelSeqScanTestsUtil::CreatePredicate(
    const std::set<oid_t> &tuple_ids) {
  PL_ASSERT(tuple_ids.size() >= 1);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConstantValueFactory(
          common::ValueFactory::GetBooleanValue(0));

  bool even = false;
  for (oid_t tuple_id : tuple_ids) {
    even = !even;

    // Create equality expression comparison tuple value and constant value.
    // First, create tuple value expression.
    expression::AbstractExpression *tuple_value_expr = nullptr;

    tuple_value_expr = even ? expression::ExpressionUtil::TupleValueFactory(
                                  common::Type::INTEGER, 0, 0)
                            : expression::ExpressionUtil::TupleValueFactory(
                                  common::Type::VARCHAR, 0, 3);

    // Second, create constant value expression.
    expression::AbstractExpression *constant_value_expr;
    if (even) {
      auto constant_value = common::ValueFactory::GetIntegerValue(
          ExecutorTestsUtil::PopulatedValue(tuple_id, 0));
      constant_value_expr =
          expression::ExpressionUtil::ConstantValueFactory(constant_value);
    } else {
      auto constant_value = common::ValueFactory::GetVarcharValue(
          std::to_string(ExecutorTestsUtil::PopulatedValue(tuple_id, 3)));
      constant_value_expr =
          expression::ExpressionUtil::ConstantValueFactory(constant_value);
    }

    // Finally, link them together using an equality expression.
    expression::AbstractExpression *equality_expr =
        expression::ExpressionUtil::ComparisonFactory(
            EXPRESSION_TYPE_COMPARE_EQUAL, tuple_value_expr,
            constant_value_expr);

    // Join equality expression to other equality expression using ORs.
    predicate = expression::ExpressionUtil::ConjunctionFactory(
        EXPRESSION_TYPE_CONJUNCTION_OR, predicate, equality_expr);
  }

  return predicate;
}

void ParallelSeqScanTestsUtil::GenerateMultiTileGroupTasks(
    storage::DataTable *table, planner::AbstractPlan *node,
    std::vector<std::shared_ptr<executor::AbstractTask>> &tasks) {
  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());
  for (size_t p = 0; p < table->GetPartitionCount(); p++) {
    auto partition_tilegroup_count = table->GetPartitionTileGroupCount(p);
    size_t task_tilegroup_count =
        (partition_tilegroup_count + PL_GET_PARTITION_SIZE() - 1) /
        PL_GET_PARTITION_SIZE();
    for (size_t i = 0; i < partition_tilegroup_count;
         i += task_tilegroup_count) {
      executor::SeqScanTask *seq_scan_task =
          new executor::SeqScanTask(node, tasks.size(), p, result_tile_lists);
      for (size_t tile_group_offset_ = i;
           tile_group_offset_ < i + task_tilegroup_count &&
               tile_group_offset_ < partition_tilegroup_count;
           tile_group_offset_++) {
        seq_scan_task->tile_group_ptrs.push_back(
            table->GetTileGroupFromPartition(p, tile_group_offset_));
      }
      tasks.push_back(std::shared_ptr<executor::AbstractTask>(seq_scan_task));
    }
  }
}

}  // namespace test
}  // namespace peloton
