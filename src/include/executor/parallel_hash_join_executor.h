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

#include "executor/abstract_join_executor.h"
#include "planner/parallel_hash_join_plan.h"
#include "executor/parallel_hash_executor.h"

namespace peloton {
namespace executor {

class ParallelHashJoinExecutor : public AbstractJoinExecutor {
  ParallelHashJoinExecutor(const ParallelHashJoinExecutor &) = delete;
  ParallelHashJoinExecutor &operator=(const ParallelHashJoinExecutor &) =
      delete;

 public:
  explicit ParallelHashJoinExecutor(const planner::AbstractPlan *node,
                                    ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  ParallelHashExecutor *hash_executor_ = nullptr;

  bool hashed_ = false;

  std::deque<LogicalTile *> buffered_output_tiles;
  std::vector<std::unique_ptr<LogicalTile>> right_tiles_;

  // logical tile iterators
  size_t left_logical_tile_itr_ = 0;
  size_t right_logical_tile_itr_ = 0;
};

}  // namespace executor
}  // namespace peloton
