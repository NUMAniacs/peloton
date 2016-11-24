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

#include "executor/parallel_join_tests_util.h"

#include "common/types.h"

#include "expression/abstract_expression.h"
#include "executor/logical_tile.h"
#include "common/harness.h"

namespace peloton {
namespace test {

void ParallelJoinTestsUtil::PopulateTileResults(
    size_t tile_group_begin_itr, size_t tile_group_count,
    std::shared_ptr<executor::LogicalTileLists> table_logical_tile_lists,
    size_t task_id, size_t partition,
    executor::LogicalTileList &table_logical_tile_ptrs) {

  while (task_id >= table_logical_tile_lists->size()) {
    table_logical_tile_lists->push_back(executor::LogicalTileList());
  }
  // Move the logical tiles for the task with task_id
  auto &target_tile_list = (*table_logical_tile_lists)[task_id];
  for (size_t tile_group_itr = tile_group_begin_itr;
       tile_group_itr < tile_group_begin_itr + tile_group_count;
       tile_group_itr++) {
    auto logical_tile = table_logical_tile_ptrs[tile_group_itr].release();
    logical_tile->SetPartition(partition);
    target_tile_list.emplace_back(logical_tile);
  }
}
}  // namespace test
}  // namespace peloton
