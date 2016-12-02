//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_join_tests_util.h
//
// Identification: test/include/executor/parallel_join_tests_util.h
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
#include "executor/abstract_task.h"

#include "storage/data_table.h"

namespace peloton {

namespace test {

class ParallelSeqScanTestsUtil {
 public:
  static storage::DataTable *CreateTable(size_t active_tile_group_count);

  static expression::AbstractExpression *CreatePredicate(
      const std::set<oid_t> &tuple_ids);

  static void GenerateMultiTileGroupTasks(
      storage::DataTable *table, planner::AbstractPlan *node,
      std::vector<std::shared_ptr<executor::AbstractTask>> &tasks);
};

}  // namespace test
}  // namespace peloton
