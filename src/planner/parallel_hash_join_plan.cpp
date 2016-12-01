
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// parallel_hash_join_plan.cpp
//
// Identification: /peloton/src/planner/parallel_hash_join_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/partition_macros.h"
#include "expression/abstract_expression.h"
#include "planner/project_info.h"
#include "planner/parallel_seq_scan_plan.h"
#include "planner/parallel_hash_join_plan.h"
#include "executor/parallel_hash_join_executor.h"
#include "executor/parallel_seq_scan_executor.h"
#include "executor/abstract_task.h"
#include "executor/parallel_hash_executor.h"
#include "executor/executor_context.h"
#include "common/init.h"

namespace peloton {
namespace planner {

ParallelHashJoinPlan::ParallelHashJoinPlan(
    PelotonJoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {}

ParallelHashJoinPlan::ParallelHashJoinPlan(
    PelotonJoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema,
    const std::vector<oid_t> &outer_hashkeys)  // outer_hashkeys is added for
                                               // IN-subquery
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {
  outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
}
}
}
