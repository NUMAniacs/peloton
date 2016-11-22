//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// join_tests_util.h
//
// Identification: test/include/executor/join_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/mock_executor.h"
#include "planner/merge_join_plan.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}

namespace planner {
class ProjectInfo;
}

namespace storage {
class DataTable;
class Tuple;
}

namespace test {

class JoinTestsUtil {
 public:
  // Create join predicate
  static expression::AbstractExpression *CreateJoinPredicate();

  // Create projection
  static std::unique_ptr<const planner::ProjectInfo> CreateProjection();

  static std::vector<planner::MergeJoinPlan::JoinClause> CreateJoinClauses();

  static std::shared_ptr<const peloton::catalog::Schema> CreateJoinSchema();

  // Create complicated join predicate
  static expression::AbstractExpression *CreateComplicatedJoinPredicate();

  static oid_t CountTuplesWithNullFields(executor::LogicalTile *logical_tile);

  static void ValidateJoinLogicalTile(executor::LogicalTile *logical_tile);

  static void ExpectEmptyTileResult(MockExecutor *table_scan_executor);

  static void ExpectMoreThanOneTileResults(
      MockExecutor *table_scan_executor,
      std::vector<std::unique_ptr<executor::LogicalTile>> &
          table_logical_tile_ptrs);

  static void ExpectNormalTileResults(
      size_t table_tile_group_count, MockExecutor *table_scan_executor,
      std::vector<std::unique_ptr<executor::LogicalTile>> &
          table_logical_tile_ptrs);
};

}  // namespace test
}  // namespace peloton
