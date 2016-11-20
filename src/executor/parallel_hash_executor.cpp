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
#include "planner/hash_plan.h"
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
  done_ = false;
  result_itr = 0;

  return true;
}

bool ParallelHashExecutor::DExecute() {
  LOG_TRACE("Hash Executor");

  if (done_ == false) {
    const planner::HashPlan &node = GetPlanNode<planner::HashPlan>();

    // First, get all the input logical tiles
    while (children_[0]->Execute()) {
      child_tiles_.emplace_back(children_[0]->GetOutput());
    }

    if (child_tiles_.size() == 0) {
      LOG_TRACE("Hash Executor : false -- no child tiles ");
      return false;
    }

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

    // Construct the hash table by going over each child logical tile and
    // hashing
    for (size_t child_tile_itr = 0; child_tile_itr < child_tiles_.size();
         child_tile_itr++) {
      LOG_DEBUG("Advance to next tile");
      auto tile = child_tiles_[child_tile_itr].get();

      // Go over all tuples in the logical tile
      for (oid_t tuple_id : *tile) {
        // Key : container tuple with a subset of tuple attributes
        // Value : < child_tile offset, tuple offset >

        // FIXME This is not thread safe at all
        ParallelHashMapType::key_type key(tile, tuple_id, &column_ids_);
        std::shared_ptr<HashSet> value;
        auto status = hash_table_.find(key, value);
        // Not found
        if (status == false) {
          LOG_TRACE("key not found %d", (int)tuple_id);
          value.reset(new HashSet());
          value->insert(std::make_pair(child_tile_itr, tuple_id));
          auto success = hash_table_.insert(key, value);
          PL_ASSERT(success);
        } else {
          // Found
          LOG_TRACE("key found %d", (int)tuple_id);
          value->insert(std::make_pair(child_tile_itr, tuple_id));
        }
        PL_ASSERT(hash_table_.contains(key));
      }
    }

    done_ = true;
  }

  // Return logical tiles one at a time
  while (result_itr < child_tiles_.size()) {
    if (child_tiles_[result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput(child_tiles_[result_itr++].release());
      LOG_DEBUG("Hash Executor : true -- return tile one at a time ");
      return true;
    }
  }

  LOG_DEBUG("Hash Executor : false -- done ");
  return false;
}

} /* namespace executor */
} /* namespace peloton */
