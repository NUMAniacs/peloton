//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_seq_scan_executor.cpp
//
// Identification: src/executor/parallel_seq_scan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/parallel_seq_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>
#include <numeric>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/executor_context.h"
#include "expression/abstract_expression.h"
#include "common/container_tuple.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/logger.h"
#include "index/index.h"
#include "planner/parallel_seq_scan_plan.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
ParallelSeqScanExecutor::ParallelSeqScanExecutor(
    const planner::AbstractPlan *node, ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool ParallelSeqScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::ParallelSeqScanPlan &node =
      GetPlanNode<planner::ParallelSeqScanPlan>();

  target_table_ = node.GetTable();

  seq_scan_task_ =
      std::dynamic_pointer_cast<SeqScanTask>(executor_context_->GetTask());
  // the task should be set in a previous step
  PL_ASSERT(seq_scan_task_.get() != nullptr);

  tile_group_itr_ = seq_scan_task_->tile_group_ptrs.begin();
  tile_group_end_itr_ = seq_scan_task_->tile_group_ptrs.end();

  curr_result_idx = 0;

  txn_partition_id_ = PL_GET_PARTITION_NODE();

  if (target_table_ != nullptr) {
    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

/**
 * @brief Creates logical tile from tile group and applies scan predicate.
 * @return true on success, false otherwise.
 */
bool ParallelSeqScanExecutor::DExecute() {
  // Scanning over a logical tile.
  if (children_.size() == 1) {
    // FIXME Check all requirements for children_.size() == 0 case.
    LOG_DEBUG("Seq Scan executor :: 1 child ");

    PL_ASSERT(target_table_ == nullptr);
    PL_ASSERT(column_ids_.size() == 0);

    while (children_[0]->Execute()) {
      std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

      if (predicate_ != nullptr) {
        // Invalidate tuples that don't satisfy the predicate.
        for (oid_t tuple_id : *tile) {
          expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
          if (eval.IsFalse()) {
            // if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
            //        .IsFalse()) {
            tile->RemoveVisibility(tuple_id);
          }
        }
      }

      if (0 == tile->GetTupleCount()) {  // Avoid returning empty tiles
        continue;
      }

      /* Hopefully we needn't do projections here */
      SetOutput(tile.release());
      return true;
    }
    return false;
  }
  // Scanning a table
  else if (children_.size() == 0) {
    LOG_TRACE("Seq Scan executor :: 0 child ");

    PL_ASSERT(target_table_ != nullptr);
    PL_ASSERT(column_ids_.size() > 0);

    // Force to use occ txn manager if dirty read is forbidden
    concurrency::TransactionManager &transaction_manager =
        concurrency::TransactionManagerFactory::GetInstance();

    bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();
    auto current_txn = executor_context_->GetTransaction();

    // Retrieve next tile group.
    while (tile_group_itr_ != tile_group_end_itr_) {
      auto tile_group = *tile_group_itr_;

      // move to next tile group
      tile_group_itr_++;

      auto tile_group_header = tile_group->GetHeader();

      oid_t active_tuple_count = tile_group->GetNextTupleSlot();

      // Construct position list by looping through tile group
      // and applying the predicate.
      LogicalTile::PositionList position_list;
      for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
        ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

        auto visibility = transaction_manager.IsVisible(
            current_txn, tile_group_header, tuple_id);

        // check transaction visibility
        if (visibility == VISIBILITY_OK) {
          // if the tuple is visible, then perform predicate evaluation.
          if (predicate_ == nullptr) {
            position_list.push_back(tuple_id);
            auto res = transaction_manager.PerformRead(
                current_txn, location, acquire_owner, txn_partition_id_);
            if (!res) {
              transaction_manager.SetTransactionResult(current_txn,
                                                       RESULT_FAILURE);
              return res;
            }
          } else {
            expression::ContainerTuple<storage::TileGroup> tuple(
                tile_group.get(), tuple_id);
            LOG_TRACE("Evaluate predicate for a tuple");
            auto eval =
                predicate_->Evaluate(&tuple, nullptr, executor_context_);
            LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
            if (eval.IsTrue()) {
              position_list.push_back(tuple_id);
              auto res = transaction_manager.PerformRead(
                  current_txn, location, acquire_owner, txn_partition_id_);
              if (!res) {
                transaction_manager.SetTransactionResult(current_txn,
                                                         RESULT_FAILURE);
                return res;
              } else {
                LOG_TRACE("Sequential Scan Predicate Satisfied");
              }
            }
          }
        }
      }

      // Don't return empty tiles
      if (position_list.size() == 0) {
        continue;
      }

      // Construct logical tile.
      // We should construct the logical tile in the current partition
      LogicalTile *logical_tile = LogicalTileFactory::GetTile(DEFAULT_NUMA_REGION);
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(position_list));

      LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
      result_tiles[num_result_tiles++] = logical_tile;

      return true;
    }
  }
  LOG_DEBUG("Total number of result tiles for task %d: %d",
            (int)seq_scan_task_->task_id,
            (int)seq_scan_task_->GetResultTileList().size());
  return false;
}

LogicalTile *ParallelSeqScanExecutor::GetOutput() {
  if (curr_result_idx >= num_result_tiles) {
    return nullptr;
  }
  auto result_tile = result_tiles[curr_result_idx++];
  return result_tile;
}

// TODO We should have a generic ExecuteTask static function
void ParallelSeqScanExecutor::ExecuteTask(std::shared_ptr<AbstractTask> task) {
  PL_ASSERT(task->GetTaskType() == TASK_SEQ_SCAN);
  PL_ASSERT(task->initialized);

  std::shared_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(task->txn));
  context->SetTask(task);

  // Generate seq scan executors
  ParallelSeqScanExecutor executor(task->node, context.get());

  bool status = executor.Init();
  if (status == true) {
    while (status) {
      status = executor.Execute();
    }
  } else {
    // TODO handle failure
    PL_ASSERT(false);
  }

  SeqScanTask *seq_scan_task = (SeqScanTask *)task.get();
  auto &list = seq_scan_task->GetResultTileList();
  for (size_t i = 0; i < executor.num_result_tiles; i++) {
    list.emplace_back(executor.result_tiles[i]);
  }

  if (task->trackable->TaskComplete()) {
    //    Record the time spent on seq scan
    (const_cast<planner::AbstractPlan *>(task->node))->RecordTaskExecutionEnd();
    LOG_INFO("All the parallel seq scan tasks have completed");
    task->dependent->DependencyComplete(task);
  }
}

}  // namespace executor
}  // namespace peloton
