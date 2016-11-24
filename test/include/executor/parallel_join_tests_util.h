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

namespace peloton {

namespace test {

class ParallelJoinTestsUtil {
 public:
  /*
   * Populate the logical tile lists with logical tiles for specific task.
   */
  static void PopulateTileResults(
      size_t tile_group_begin_itr, size_t tile_group_count,
      std::shared_ptr<executor::LogicalTileLists> table_logical_tile_lists,
      size_t task_id, size_t partition,
      executor::LogicalTileList &table_logical_tile_ptrs);
};

}  // namespace test
}  // namespace peloton
