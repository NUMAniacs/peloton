//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_hash_executor.cpp
//
// Identification: src/executor/parallel_hash_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "common/logger.h"
#include "common/value.h"
#include "executor/logical_tile.h"
#include "executor/parallel_hash_executor.h"
#include "planner/parallel_hash_plan.h"
#include "expression/tuple_value_expression.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
ParallelHashExecutor::ParallelHashExecutor(const planner::AbstractPlan *node,
                                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context), total_num_tuples_(0) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool ParallelHashExecutor::DInit() {
  if (initialized_ == false) {
    // Initialize executor state
    result_itr = 0;

    // Initialize the hash keys
    InitHashKeys();
  }
  initialized_ = true;
  return true;
}

void ParallelHashExecutor::InitHashKeys() {
  const planner::ParallelHashPlan &node =
      GetPlanNode<planner::ParallelHashPlan>();
  /* *
   * HashKeys is a vector of TupleValue expr
   * from which we construct a vector of column ids that represent the
   * attributes of the underlying table.
   * The hash table is built on top of these hash key attributes
   * */
  auto &hashkeys = node.GetHashKeys();

  // Construct a logical tile
  for (auto &hashkey : hashkeys) {
    PL_ASSERT(hashkey->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE);
    auto tuple_value =
        reinterpret_cast<const expression::TupleValueExpression *>(
            hashkey.get());
    auto column_id = tuple_value->GetColumnId();
    column_ids_.push_back(column_id);
  }
}

void ParallelHashExecutor::ExecuteTask(std::shared_ptr<AbstractTask> task) {
  PL_ASSERT(task->GetTaskType() == TASK_HASH);
  executor::HashTask *hash_task = static_cast<executor::HashTask *>(task.get());
  auto hash_executor = hash_task->hash_executor;

  // Construct the hash table by going over each child logical tile and hashing
  auto task_id = hash_task->task_id;
  auto child_tiles = hash_task->result_tile_lists;
  auto &hash_table = hash_executor->GetHashTable();
  auto &column_ids = hash_executor->GetHashKeyIds();
  size_t num_tuples = 0;

  for (size_t tile_itr = 0; tile_itr < (*child_tiles)[task_id].size();
       tile_itr++) {

    LOG_DEBUG("Advance to next tile. Task id: %d, tile_itr: %d", (int)task_id,
              (int)tile_itr);
    auto tile = (*child_tiles)[task_id][tile_itr].get();

    // Go over all tuples in the logical tile
    for (oid_t tuple_id : *tile) {
      // Key : container tuple with a subset of tuple attributes
      // Value : < child_tile offset, tuple offset >

      ParallelHashMapType::key_type key(tile, tuple_id, &column_ids);
      std::shared_ptr<ConcurrentSet> value;
      auto status = hash_table.find(key, value);
      // Not found
      if (status == false) {
        LOG_TRACE("key not found %d", (int)tuple_id);
        value.reset(new ConcurrentSet());
        auto success = hash_table.insert(key, value);
        if (success == false) {
          success = hash_table.find(key, value);
        }
        PL_ASSERT(success);
      } else {
        // Found key
        LOG_TRACE("key found %d", (int)tuple_id);
      }
      value->Insert(std::make_tuple(tile_itr, tuple_id, task_id));
      PL_ASSERT(hash_table.contains(key));

      // Increment the number of tuples hashed
      num_tuples++;
    }
  }
  LOG_DEBUG("Task %d hashed %d tuples", (int)task_id, (int)num_tuples);
  hash_executor->IncrementNumTuple(num_tuples);

  if (task->trackable->TaskComplete()) {
    LOG_INFO("All the hash tasks have completed");
    task->dependent->DependencyComplete(task);
  }
}

// This function is not being used;
bool ParallelHashExecutor::DExecute() {
  PL_ASSERT(false);
  return false;
}

} /* namespace executor */
} /* namespace peloton */
