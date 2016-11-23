//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_hash_join_plan.h
//
// Identification: src/include/planner/parallel_hash_join_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_join_plan.h"

namespace peloton {
namespace expression {
class AbstractExpression;
}
namespace planner {

class ParallelHashJoinPlan : public AbstractJoinPlan {
 public:
  ParallelHashJoinPlan(const ParallelHashJoinPlan &) = delete;
  ParallelHashJoinPlan &operator=(const ParallelHashJoinPlan &) = delete;
  ParallelHashJoinPlan(ParallelHashJoinPlan &&) = delete;
  ParallelHashJoinPlan &operator=(ParallelHashJoinPlan &&) = delete;

  ParallelHashJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema);

  ParallelHashJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      const std::vector<oid_t> &outer_hashkeys);

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_PARALLEL_HASHJOIN;
  }

  const std::string GetInfo() const { return "ParallelHashJoin"; }

  const std::vector<oid_t> &GetOuterHashIds() const {
    return outer_column_ids_;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        GetPredicate()->Copy());
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));
    ParallelHashJoinPlan *new_plan = new ParallelHashJoinPlan(
        GetJoinType(), std::move(predicate_copy),
        std::move(GetProjInfo()->Copy()), schema_copy, outer_column_ids_);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  std::vector<oid_t> outer_column_ids_;
};

}  // namespace planner
}  // namespace peloton
