//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_seq_scan_plan.h
//
// Identification: src/include/planner/parallel_seq_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_scan_plan.h"
#include "common/logger.h"
#include "common/serializer.h"
#include "common/types.h"
#include "expression/abstract_expression.h"
#include "executor/abstract_task.h"

namespace peloton {

namespace parser {
struct SelectStatement;
}
namespace storage {
class DataTable;
}

namespace planner {

class ParallelSeqScanPlan : public AbstractScan, public Dependent {
 public:
  ParallelSeqScanPlan(const ParallelSeqScanPlan &) = delete;
  ParallelSeqScanPlan &operator=(const ParallelSeqScanPlan &) = delete;
  ParallelSeqScanPlan(ParallelSeqScanPlan &&) = delete;
  ParallelSeqScanPlan &operator=(ParallelSeqScanPlan &&) = delete;

  ParallelSeqScanPlan(storage::DataTable *table,
                      expression::AbstractExpression *predicate,
                      const std::vector<oid_t> &column_ids,
                      bool is_for_update = false)
      : AbstractScan(table, predicate, column_ids) {
    LOG_DEBUG("Creating a Parallel Sequential Scan Plan");
    SetForUpdateFlag(is_for_update);
  }

  ParallelSeqScanPlan(parser::SelectStatement *select_node);

  ParallelSeqScanPlan() : AbstractScan() {}

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_PARALLEL_SEQSCAN;
  }

  const std::string GetInfo() const { return "SeqScan"; }

  void SetParameterValues(std::vector<common::Value> *values);

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(SerializeOutput &output);
  bool DeserializeFrom(SerializeInput &input);

  /* For init SerializeOutput */
  int SerializeSize();

  oid_t GetColumnID(std::string col_name);

  std::unique_ptr<AbstractPlan> Copy() const {
    AbstractPlan *new_plan = new ParallelSeqScanPlan(
        this->GetTable(), this->GetPredicate()->Copy(), this->GetColumnIds());
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

  void DependencyComplete(
      UNUSED_ATTRIBUTE std::shared_ptr<executor::AbstractTask> task) {}

  void GenerateTasks(
      std::vector<std::shared_ptr<executor::AbstractTask>> &tasks,
      std::shared_ptr<executor::LogicalTileLists> result_tile_lists);
};

}  // namespace planner
}  // namespace peloton
