//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_executor.cpp
//
// Identification: src/executor/hash_join_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "common/types.h"
#include "common/logger.h"
#include "executor/logical_tile_factory.h"
#include "executor/parallel_hash_join_executor.h"
#include "executor/parallel_seq_scan_executor.h"
#include "expression/abstract_expression.h"
#include "common/container_tuple.h"
#include "executor/executor_context.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
ParallelHashJoinExecutor::ParallelHashJoinExecutor(
    const planner::AbstractPlan *node, ExecutorContext *executor_context)
    : AbstractParallelJoinExecutor(node, executor_context) {}

bool ParallelHashJoinExecutor::DInit() {
  PL_ASSERT(children_.size() == 2);

  auto status = AbstractParallelJoinExecutor::DInit();
  if (status == false) return status;

  PL_ASSERT(children_[1]->GetRawNode()->GetPlanNodeType() ==
            PLAN_NODE_TYPE_PARALLEL_HASH);

  hash_executor_ = reinterpret_cast<ParallelHashExecutor *>(children_[1]);

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool ParallelHashJoinExecutor::DExecute() {
  LOG_DEBUG("********** Hash Join executor :: 2 children \n");

  // Loop until we have non-empty result tile or exit
  for (;;) {
    // Check if we have any buffered output tiles
    if (buffered_output_tiles.empty() == false) {
      auto output_tile = buffered_output_tiles.front();
      SetOutput(output_tile);
      buffered_output_tiles.pop_front();
      return true;
    }

    // Build outer join output when done
    if (left_child_done_ == true) {
      return BuildOuterJoinOutput();
    }

    //===------------------------------------------------------------------===//
    // Pick right and left tiles
    //===------------------------------------------------------------------===//

    // Get all the tiles from RIGHT child
    if (right_child_done_ == false) {
      auto child_tiles = hash_executor_->child_tiles;
      auto num_tasks = child_tiles->size();
      size_t num_tiles = 0;
      for (size_t task_itr = 0; task_itr < num_tasks; task_itr++) {
        tile_offsets_.push_back(num_tiles);
        auto &target_tile_list = (*child_tiles)[task_itr];
        for (size_t tile_itr = 0; tile_itr < target_tile_list.size();
             tile_itr++) {
          if (target_tile_list[tile_itr]->GetTupleCount() > 0) {
            // TODO Eliminate buffer right tile function
            BufferRightTile(target_tile_list[tile_itr].get());
            num_tiles++;
          }
        }
      }
      LOG_DEBUG("Number of right tiles buffered: %d", (int)num_tiles);
      right_child_done_ = true;
    }

    // Get next tile from LEFT child
    if (children_[0]->Execute() == false) {
      LOG_DEBUG("Did not get left tile \n");
      left_child_done_ = true;
      continue;
    }

    BufferLeftTile(children_[0]->GetOutput());
    LOG_DEBUG("Got left tile \n");

    if (right_result_tiles_.size() == 0) {
      LOG_DEBUG("Did not get any right tiles \n");
      return BuildOuterJoinOutput();
    }

    LogicalTile *left_tile = left_result_tiles_.back().get();

    //===------------------------------------------------------------------===//
    // Build Join Tile
    //===------------------------------------------------------------------===//

    // Get the hash table from the hash executor
    bool use_custom = hash_executor_->use_custom_hash_table;
    auto &custom_ht = hash_executor_->GetCustomHashTable();
    auto &cuckoo_ht = hash_executor_->GetCuckooHashTable();

    auto &hashed_col_ids = hash_executor_->GetHashKeyIds();

    oid_t prev_tile = INVALID_OID;
    std::unique_ptr<LogicalTile> output_tile;
    LogicalTile::PositionListsBuilder pos_lists_builder;

    // Go over the left tile
    for (auto left_tile_itr : *left_tile) {
      const expression::ContainerTuple<executor::LogicalTile> left_tuple(
          left_tile, left_tile_itr, &hashed_col_ids);

      // Find matching tuples in the hash table built on top of the right table
      std::shared_ptr<ParallelHashExecutor::ConcurrentVector> right_tuple_set;
      auto success = use_custom ? custom_ht.Get(left_tuple, right_tuple_set)
                                : cuckoo_ht.find(left_tuple, right_tuple_set);
      auto &right_tuples = right_tuple_set->GetVector();

      if (success) {
        RecordMatchedLeftRow(left_result_tiles_.size() - 1, left_tile_itr);

        // Go over the matching right tuples
        for (auto &location : right_tuples) {

          auto task_id = std::get<2>(location);
          PL_ASSERT(task_id < tile_offsets_.size());
          size_t right_tile_itr =
              tile_offsets_[task_id] + std::get<0>(location);
          size_t right_tuple_itr = std::get<1>(location);

          // Check if we got a new right tile itr
          if (prev_tile != right_tile_itr) {
            // Check if we have any join tuples
            if (pos_lists_builder.Size() > 0) {
              LOG_DEBUG("Join tile size : %lu \n", pos_lists_builder.Size());
              output_tile->SetPositionListsAndVisibility(
                  pos_lists_builder.Release());
              buffered_output_tiles.push_back(output_tile.release());
            }

            // Get the logical tile from right child
            PL_ASSERT(right_tile_itr < right_result_tiles_.size());
            LogicalTile *right_tile = right_result_tiles_[right_tile_itr];
            PL_ASSERT(right_tile != nullptr);

            // Build output logical tile
            output_tile = BuildOutputLogicalTile(left_tile, right_tile);

            // Build position lists
            pos_lists_builder =
                LogicalTile::PositionListsBuilder(left_tile, right_tile);

            pos_lists_builder.SetRightSource(
                &right_result_tiles_[right_tile_itr]->GetPositionLists());
          }

          // Add join tuple
          pos_lists_builder.AddRow(left_tile_itr, right_tuple_itr);

          RecordMatchedRightRow(right_tile_itr, right_tuple_itr);

          // Cache prev logical tile itr
          prev_tile = right_tile_itr;
        }
      }
    }

    // Check if we have any join tuples
    if (pos_lists_builder.Size() > 0) {
      LOG_DEBUG("Join tile size : %lu \n", pos_lists_builder.Size());
      output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
      buffered_output_tiles.push_back(output_tile.release());
    }

    // Check if we have any buffered output tiles
    if (buffered_output_tiles.empty() == false) {
      auto output_tile = buffered_output_tiles.front();
      SetOutput(output_tile);
      buffered_output_tiles.pop_front();

      return true;
    } else {
      // Try again
      continue;
    }
  }
}

void ParallelHashJoinExecutor::ExecuteTask(
    std::shared_ptr<AbstractTask> task_bottom,
    std::shared_ptr<AbstractTask> task_top) {

  PL_ASSERT(task_top->GetTaskType() == TASK_HASHJOIN);
  PL_ASSERT(task_top->initialized);
  PL_ASSERT(task_bottom->initialized);

  HashJoinTask *hash_join_task = static_cast<HashJoinTask *>(task_top.get());

  // Create execution context
  std::shared_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(hash_join_task->txn));
  context->SetTask(task_bottom);

  // Construct the hash join executor
  std::shared_ptr<executor::ParallelHashJoinExecutor> hash_join_executor(
      new executor::ParallelHashJoinExecutor(task_top->node, context.get()));

  std::shared_ptr<executor::ParallelSeqScanExecutor> seq_scan_executor(
      new executor::ParallelSeqScanExecutor(task_bottom->node, context.get()));

  hash_join_executor->AddChild(seq_scan_executor.get());
  hash_join_executor->AddChild(hash_join_task->hash_executor.get());

  bool status = hash_join_executor->Init();
  if (status == true) {
    while (status) {
      status = hash_join_executor->Execute();
      // Set the result
      auto result_tile = hash_join_executor->GetOutput();
      if (result_tile != nullptr) {
        hash_join_task->GetResultTileList().emplace_back(result_tile);
      }
    }
  } else {
    // TODO handle failure
    PL_ASSERT(false);
  }

  if (task_top->trackable->TaskComplete()) {
    (const_cast<planner::Dependent *>(
         reinterpret_cast<const planner::Dependent *>(task_top->node)))
        ->RecordTaskExecutionEnd();
    // XXX The start of result aggregation
    task_top->dependent->RecordTaskExecutionStart();
    LOG_INFO("All the parallel hash join tasks have completed");
    task_top->dependent->DependencyComplete(task_top);
  }
}

}  // namespace executor
}  // namespace peloton
