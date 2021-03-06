//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_executor.h
//
// Identification: src/include/executor/hash_join_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <vector>

#include "executor/abstract_parallel_join_executor.h"
#include "planner/parallel_hash_join_plan.h"
#include "executor/parallel_hash_executor.h"

namespace peloton {
namespace executor {

class ParallelHashJoinExecutor : public AbstractParallelJoinExecutor {
  ParallelHashJoinExecutor(const ParallelHashJoinExecutor &) = delete;
  ParallelHashJoinExecutor &operator=(const ParallelHashJoinExecutor &) =
      delete;

 public:
  explicit ParallelHashJoinExecutor(const planner::AbstractPlan *node,
                                    ExecutorContext *executor_context);

  static void ExecuteTask(std::shared_ptr<AbstractTask> task_bottom,
                          std::shared_ptr<AbstractTask> task_top);

 protected:
  bool DInit();

  bool DExecute();

 private:
  ParallelHashExecutor *hash_executor_ = nullptr;

  // Record offsets to locate right tiles from different tasks
  // We have this variable because we're re-using the BuildOutputTile function
  // which assumes serial execution.
  std::vector<size_t, common::StlNumaAllocator<size_t>> tile_offsets_;

  std::deque<LogicalTile *, common::StlNumaAllocator<LogicalTile *>>
      buffered_output_tiles;

  // logical tile iterators
  size_t left_logical_tile_itr_ = 0;
  size_t right_logical_tile_itr_ = 0;
};

}  // namespace executor
}  // namespace peloton
