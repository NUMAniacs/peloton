//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_seq_scan_plan.cpp
//
// Identification: src/planner/parallel_seq_scan_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/parallel_seq_scan_plan.h"
#include "catalog/catalog.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/types.h"
#include "expression/expression_util.h"
#include "storage/data_table.h"

#include "parser/statement_select.h"

namespace peloton {
namespace planner {

//===--------------------------------------------------------------------===//
// Serialization/Deserialization
//===--------------------------------------------------------------------===//

/**
 * The ParallelSeqScanPlan has the following members:
 *   database_id, table_id, predicate, column_id, parent(might be NULL)
 * TODO: ParallelSeqScanPlan doesn't have children, so we don't need to handle
 *it
 *
 * Therefore a ParallelSeqScanPlan is serialized as:
 * [(int) total size]
 * [(int8_t) plan type]
 * [(int) database_id]
 * [(int) table_id]
 * [(int) num column_id]
 * [(int) column id...]
 * [(int8_t) expr type]     : if invalid, predicate is null
 * [(bytes) predicate]      : predicate is Expression
 * [(int8_t) plan type]     : if invalid, parent is null
 * [(bytes) parent]         : parent is also a plan
 *
 * TODO: parent_ seems never be set or used
 */

bool ParallelSeqScanPlan::SerializeTo(SerializeOutput &output) {
  // A placeholder for the total size written at the end
  int start = output.Position();
  output.WriteInt(-1);

  // Write the ParallelSeqScanPlan type
  PlanNodeType plan_type = GetPlanNodeType();
  output.WriteByte(static_cast<int8_t>(plan_type));

  // Write database id and table id
  if (!GetTable()) {
    // The plan is not completed
    return false;
  }
  oid_t database_id = GetTable()->GetDatabaseOid();
  oid_t table_id = GetTable()->GetOid();

  output.WriteInt(static_cast<int>(database_id));
  output.WriteInt(static_cast<int>(table_id));

  // If column has 0 item, just write the columnid_count with 0
  int columnid_count = GetColumnIds().size();
  output.WriteInt(columnid_count);

  // If column has 0 item, nothing happens here
  for (int it = 0; it < columnid_count; it++) {
    oid_t col_id = GetColumnIds()[it];
    output.WriteInt(static_cast<int>(col_id));
  }

  // Write predicate
  if (GetPredicate() == nullptr) {
    // Write the type
    output.WriteByte(static_cast<int8_t>(EXPRESSION_TYPE_INVALID));
  } else {
    // Write the expression type
    ExpressionType expr_type = GetPredicate()->GetExpressionType();
    output.WriteByte(static_cast<int8_t>(expr_type));
  }

  // Write parent, but parent seems never be set or used right now
  if (GetParent() == nullptr) {
    // Write the type
    output.WriteByte(static_cast<int8_t>(PLAN_NODE_TYPE_INVALID));
  } else {
    // Write the parent type
    PlanNodeType parent_type = GetParent()->GetPlanNodeType();
    output.WriteByte(static_cast<int8_t>(parent_type));

    // Write parent
    GetParent()->SerializeTo(output);
  }

  // Write the total length
  int32_t sz = static_cast<int32_t>(output.Position() - start - sizeof(int));
  PL_ASSERT(sz > 0);
  output.WriteIntAt(start, sz);

  return true;
}

/**
* Therefore a ParallelSeqScanPlan is serialized as:
* [(int) total size]
* [(int8_t) plan type]
* [(int) database_id]
* [(int) table_id]
* [(int) num column_id]
* [(int) column id...]
* [(int8_t) expr type]     : if invalid, predicate is null
* [(bytes) predicate]      : predicate is Expression
* [(int8_t) plan type]     : if invalid, parent is null
* [(bytes) parent]         : parent is also a plan
*/
bool ParallelSeqScanPlan::DeserializeFrom(SerializeInput &input) {
  // Read the size of ParallelSeqScanPlan class
  input.ReadInt();

  // Read the type
  UNUSED_ATTRIBUTE PlanNodeType plan_type =
      (PlanNodeType)input.ReadEnumInSingleByte();
  PL_ASSERT(plan_type == GetPlanNodeType());

  // Read database id
  oid_t database_oid = input.ReadInt();

  // Read table id
  oid_t table_oid = input.ReadInt();

  // Get table and set it to the member
  storage::DataTable *target_table = static_cast<storage::DataTable *>(
      catalog::Catalog::GetInstance()->GetTableWithOid(database_oid,
                                                       table_oid));
  SetTargetTable(target_table);

  // Read the number of column_id and set them to column_ids_
  oid_t columnid_count = input.ReadInt();
  for (oid_t it = 0; it < columnid_count; it++) {
    oid_t column_id = input.ReadInt();
    SetColumnId(column_id);
  }

  // Read the type
  ExpressionType expr_type = (ExpressionType)input.ReadEnumInSingleByte();

  // Predicate deserialization
  if (expr_type != EXPRESSION_TYPE_INVALID) {
    switch (expr_type) {
      //            case EXPRESSION_TYPE_COMPARE_IN:
      //                predicate_ =
      //                std::unique_ptr<EXPRESSION_TYPE_COMPARE_IN>(new
      //                ComparisonExpression (101));
      //                predicate_.DeserializeFrom(input);
      //              break;

      default: {
        LOG_ERROR(
            "Expression deserialization :: Unsupported EXPRESSION_TYPE: %u ",
            expr_type);
        break;
      }
    }
  }

  // Read the type of parent
  PlanNodeType parent_type = (PlanNodeType)input.ReadEnumInSingleByte();

  // Parent deserialization
  if (parent_type != PLAN_NODE_TYPE_INVALID) {
    switch (expr_type) {
      //            case EXPRESSION_TYPE_COMPARE_IN:
      //                predicate_ =
      //                std::unique_ptr<EXPRESSION_TYPE_COMPARE_IN>(new
      //                ComparisonExpression (101));
      //                predicate_.DeserializeFrom(input);
      //              break;

      default: {
        LOG_ERROR("Parent deserialization :: Unsupported PlanNodeType: %u ",
                  expr_type);
        break;
      }
    }
  }

  return true;
}
/**
*
* ParallelSeqScanPlan is serialized as:
* [(int) total size]
* [(int8_t) plan type]
* [(int) database_id]
* [(int) table_id]
* [(int) num column_id]
* [(int) column id...]
* [(int8_t) expr type]     : if invalid, predicate is null
* [(bytes) predicate]      : predicate is Expression
* [(int8_t) plan type]     : if invalid, parent is null
* [(bytes) parent]         : parent is also a plan
*
* So, the fixed size part is:
*      [(int) total size]   4 +
*      [(int8_t) plan type] 1 +
*      [(int) database_id]  4 +
*      [(int) table_id]     4 +
*      [(int) num column_id]4 +
*      [(int8_t) expr type] 1 +
*      [(int8_t) plan type] 1 =
*     the variant part is :
*      [(int) column id...]: num column_id * 4
*      [(bytes) predicate] : predicate->GetSerializeSize()
*      [(bytes) parent]    : parent->GetSerializeSize()
*/
int ParallelSeqScanPlan::SerializeSize() {
  // Fixed size. see the detail above
  int size_fix = sizeof(int) * 4 + 3;
  int size_columnids = ColumnIds().size() * sizeof(int);
  int size = size_fix + size_columnids;

  if (Predicate()) {
    size = size + Predicate()->SerializeSize();
  }
  if (Parent()) {
    size = size + Parent()->SerializeSize();
  }

  return size;
}

oid_t ParallelSeqScanPlan::GetColumnID(std::string col_name) {
  auto &columns = GetTable()->GetSchema()->GetColumns();
  oid_t index = -1;
  for (oid_t i = 0; i < columns.size(); ++i) {
    if (columns[i].column_name == col_name) {
      index = i;
      break;
    }
  }
  return index;
}

void ParallelSeqScanPlan::SetParameterValues(
    std::vector<common::Value> *values) {
  LOG_TRACE("Setting parameter values in Parallel Sequential Scan");

  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
}

void ParallelSeqScanPlan::GenerateTasks(
    std::vector<std::shared_ptr<executor::AbstractTask>> &tasks,
    std::shared_ptr<executor::LogicalTileLists> result_tile_lists) {
  auto target_table = GetTable();
  auto partition_count = target_table->GetPartitionCount();

  // Assuming that we always have at least one partition,
  // deploy partitioned seq scan
  for (size_t i = 0; i < partition_count; i++) {
    auto num_tile_groups = target_table->GetPartitionTileGroupCount(i);
    LOG_ERROR("Partition ID:%ld NumTasks:%ld", i, num_tile_groups);
    size_t num_tile_groups_per_task =
        (num_tile_groups + TASK_TILEGROUP_COUNT - 1) / TASK_TILEGROUP_COUNT;
    for (size_t j = 0; j < num_tile_groups; j += num_tile_groups_per_task) {
      executor::TileGroupPtrList tile_group_ptrs;
      for (size_t k = j;
           k < j + num_tile_groups_per_task && k < num_tile_groups; k++) {
        if (target_table->GetTileGroupFromPartition(i, k)
                ->GetActiveTupleCount() > 0) {
          // append the next tile group
          tile_group_ptrs.push_back(
              target_table->GetTileGroupFromPartition(i, k));
        }
      }
      if (tile_group_ptrs.size() > 0) {
        // create a new task only when there's tile groups to scan
        executor::SeqScanTask *seq_scan_task =
            new executor::SeqScanTask(this, tasks.size(), i, result_tile_lists);
        seq_scan_task->tile_group_ptrs = tile_group_ptrs;
        tasks.push_back(std::shared_ptr<executor::AbstractTask>(seq_scan_task));
      }
    }
  }
}

}  // namespace planner
}  // namespace peloton
