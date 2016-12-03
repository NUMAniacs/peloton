//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.cpp
//
// Identification: src/executor/seq_scan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/seq_scan_executor.h"

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

namespace peloton {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();

  target_table_ = node.GetTable();

  // offset by partition_id when multiple threads run the query
  current_tile_group_offset_ = (START_OID + partition_id_);

  LOG_TRACE("Partition_ID:%d, Parallelism Count:%d Tile Group Offset:%d\n",
            partition_id_, num_tasks_, current_tile_group_offset_);

  txn_partition_id_ = PL_GET_PARTITION_NODE();

  task_ = std::dynamic_pointer_cast<PartitionUnawareTask>(executor_context_->GetTask());

  if (task_.get() != nullptr) {
    task_->cpu_id = PL_GET_PARTITION_NODE();
  }

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    // round up to the nearest value of parallelism count
    num_tile_groups_per_thread_ =
        (table_tile_group_count_ + num_tasks_ - 1)/num_tasks_;

    // offset by the number of tiles that each thread processes
    current_tile_group_offset_ *= num_tile_groups_per_thread_;

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
bool SeqScanExecutor::DExecute() {
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
    // support intra-query parallelism, be parallelism count aware

    LOG_TRACE("Seq Scan executor :: 0 child ");

    PL_ASSERT(target_table_ != nullptr);
    PL_ASSERT(column_ids_.size() > 0);

    // Force to use occ txn manager if dirty read is forbidden
    concurrency::TransactionManager &transaction_manager =
        concurrency::TransactionManagerFactory::GetInstance();

    bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();
    auto current_txn = executor_context_->GetTransaction();

    // Retrieve next tile group.
    while (current_tile_group_offset_ < table_tile_group_count_ &&
           num_tile_groups_processed_ < num_tile_groups_per_thread_) {
      size_t tile_group_partition_id;
      auto tile_group =
          target_table_->GetTileGroup(current_tile_group_offset_,
                                      &tile_group_partition_id);

      if (task_.get() != nullptr) {
        task_->total_access_count++;
        // non-local tile group access?
        if (tile_group_partition_id != (size_t)PL_GET_PARTITION_ID(txn_partition_id_)) {
          task_->non_local_access_count++;
        }
      }

      // move to next offset
      current_tile_group_offset_++;
      num_tile_groups_processed_++;

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
            auto res = transaction_manager.PerformRead(current_txn, location, acquire_owner);
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
              auto res = transaction_manager.PerformRead(current_txn, location, acquire_owner);
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
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile(UNDEFINED_NUMA_REGION));
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(position_list));

      LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
      SetOutput(logical_tile.release());
      return true;
    }
  }
  return false;
}

}  // namespace executor
}  // namespace peloton