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
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool ParallelHashExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);
  // Initialize executor state
  result_itr = 0;

  // Initialize the hash keys
  InitHashKeys();
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
    column_ids_.push_back(tuple_value->GetColumnId());
  }
}

void ParallelHashExecutor::ExecuteTask(std::shared_ptr<HashTask> hash_task) {
  // Construct the hash table by going over each child logical tile and hashing
  auto task_id = hash_task->task_id;
  auto child_tiles = hash_task->result_tile_lists;
  auto &hash_table = hash_task->hash_executor->GetHashTable();
  auto &column_ids = hash_task->hash_executor->GetHashKeyIds();

  for (size_t tile_itr = 0; tile_itr < (*child_tiles)[task_id].size();
       tile_itr++) {

    LOG_DEBUG("Advance to next tile");
    auto tile = (*child_tiles)[task_id][tile_itr].get();

    // Go over all tuples in the logical tile
    for (oid_t tuple_id : *tile) {
      // Key : container tuple with a subset of tuple attributes
      // Value : < child_tile offset, tuple offset >

      // FIXME This is not thread safe at all since multiple threads may insert
      // to the same std::set
      ParallelHashMapType::key_type key(tile, tuple_id, &column_ids);
      std::shared_ptr<HashSet> value;
      auto status = hash_table.find(key, value);
      // Not found
      if (status == false) {
        LOG_TRACE("key not found %d", (int)tuple_id);
        value.reset(new HashSet());
        value->insert(std::make_pair(tile_itr, tuple_id));
        auto success = hash_table.insert(key, value);
        PL_ASSERT(success);
        (void)success;
      } else {
        // Found
        LOG_TRACE("key found %d", (int)tuple_id);
        value->insert(std::make_pair(tile_itr, tuple_id));
      }
      PL_ASSERT(hash_table.contains(key));
    }
  }
}

bool ParallelHashExecutor::DExecute() {
  LOG_TRACE("Hash Executor");

  // Return logical tiles one at a time
  while (result_itr < (*child_tiles_)[0].size()) {
    if ((*child_tiles_)[0][result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput((*child_tiles_)[0][result_itr++].release());
      LOG_DEBUG("Hash Executor : true -- return tile one at a time ");
      return true;
    }
  }

  LOG_DEBUG("Hash Executor : false -- done ");
  return false;
}

} /* namespace executor */
} /* namespace peloton */
