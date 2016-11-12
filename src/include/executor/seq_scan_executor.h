//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.h
//
// Identification: src/include/executor/seq_scan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/seq_scan_plan.h"
#include "executor/abstract_scan_executor.h"

namespace peloton {
namespace executor {

class SeqScanExecutor : public AbstractScanExecutor {
 public:
  SeqScanExecutor(const SeqScanExecutor &) = delete;
  SeqScanExecutor &operator=(const SeqScanExecutor &) = delete;
  SeqScanExecutor(SeqScanExecutor &&) = delete;
  SeqScanExecutor &operator=(SeqScanExecutor &&) = delete;

  explicit SeqScanExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

  void ResetState() { current_tile_group_offset_ = START_OID; }

 protected:
  bool DInit();

  bool DExecute();

  private:
    //===--------------------------------------------------------------------===//
    // Executor State
    //===--------------------------------------------------------------------===//

  /** @brief Keeps track of current tile group id within the current
   * partition being scanned. */
  size_t current_tile_group_offset_ = INVALID_OID;

  /** @brief Keeps track of the current partition being accessed */
  size_t current_partition_offset_ = INVALID_OID;

  /** @brief Keeps track of the number of partitions to scan. */
  size_t table_partition_count_ = 0;

  /** @brief Keep track of the number of tile groups
   * in the current partition */
  size_t partition_tile_group_count_ = 0;

  // number of tile groups that this thread
  // of execution will work on
  int num_tile_groups_processed_ = 0;

  // number of sequential tile groups each
  // thread of execution will work on
  int num_tile_groups_per_thread_;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;
};


}  // namespace executor
}  // namespace peloton
