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
#include <include/planner/parallel_seq_scan_plan.h>

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

namespace peloton {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
ParallelSeqScanExecutor::ParallelSeqScanExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool ParallelSeqScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::ParallelSeqScanPlan &node = GetPlanNode<planner::ParallelSeqScanPlan>();

  target_table_ = node.GetTable();

  seq_scan_task_ = std::dynamic_pointer_cast<SeqScanTask>(executor_context_->GetTask());
  // the task should be set in a previous step
  PL_ASSERT(seq_scan_task_.get() != nullptr);

  tile_group_itr_ = seq_scan_task_->tile_group_ptrs.begin();
  tile_group_end_itr_ = seq_scan_task_->tile_group_ptrs.end();

  result_tiles_itr_ = seq_scan_task_->GetResultTileList().begin();

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
    LOG_TRACE("Seq Scan executor :: 1 child ");

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
            //if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
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
      std::vector<oid_t> position_list;
      for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
        ItemPointer location(tile_group->GetTileGroupId(), tuple_id);


        auto visibility = transaction_manager.IsVisible(current_txn, tile_group_header, tuple_id);

        // check transaction visibility
        if (visibility == VISIBILITY_OK) {
          // if the tuple is visible, then perform predicate evaluation.
          if (predicate_ == nullptr) {
            position_list.push_back(tuple_id);
            auto res = transaction_manager.PerformRead(current_txn, location, acquire_owner,
                                                       txn_partition_id_);
            if (!res) {
              transaction_manager.SetTransactionResult(current_txn, RESULT_FAILURE);
              return res;
            }
          } else {
            expression::ContainerTuple<storage::TileGroup> tuple(
                tile_group.get(), tuple_id);
            LOG_TRACE("Evaluate predicate for a tuple");
            auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
            LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
            if (eval.IsTrue()) {
              position_list.push_back(tuple_id);
              auto res = transaction_manager.PerformRead(current_txn, location, acquire_owner,
                                                         txn_partition_id_);
              if (!res) {
                transaction_manager.SetTransactionResult(current_txn, RESULT_FAILURE);
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
      // TODO We should construct the logical tile in the current partition
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(position_list));

      LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
      seq_scan_task_->GetResultTileList().push_back(std::move(logical_tile));
      return true;
    }
  }
  return false;
}

LogicalTile* ParallelSeqScanExecutor::GetOutput() {
  if (result_tiles_itr_ == seq_scan_task_->GetResultTileList().end()) {
    return nullptr;
  }
  auto result_tile = (*result_tiles_itr_).release();
  result_tiles_itr_++;
  return result_tile;
}

}  // namespace executor
}  // namespace peloton
