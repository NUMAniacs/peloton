//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_tests_util.h
//
// Identification: test/include/executor/executor_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>
#include "boost/thread/future.hpp"
#include "common/types.h"
#include "executor/abstract_task.h"
#include "planner/abstract_plan.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace common {
class VarlenPool;
}

namespace catalog {
class Column;
class Manager;
}

namespace concurrency {
class Transaction;
}

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace storage {
class Backend;
class TileGroup;
class DataTable;
class Tuple;
}

#define TESTS_TUPLES_PER_TILEGROUP 5
#define DEFAULT_TILEGROUP_COUNT 3

namespace test {

class ExecutorTestsUtil {
 public:
  /**
   * @brief Creates a basic tile group with allocated but not populated
   *        tuples.
   */
  static std::shared_ptr<storage::TileGroup> CreateTileGroup(
      int allocate_tuple_count = TESTS_TUPLES_PER_TILEGROUP);

  /** @brief Creates a basic table with allocated but not populated tuples */
  static storage::DataTable *CreateTable(
      int tuples_per_tilegroup_count = TESTS_TUPLES_PER_TILEGROUP,
      bool indexes = true, oid_t table_oid = INVALID_OID);

  /** @brief Creates a basic table with allocated and populated tuples */
  static storage::DataTable *CreateAndPopulateTable();

  static void PopulateTable(storage::DataTable *table, int num_rows,
                            bool mutate, bool random, bool group_by,
                            concurrency::Transaction *current_txn);

  static void PopulateTiles(std::shared_ptr<storage::TileGroup> tile_group,
                            int num_rows);

  static catalog::Column GetColumnInfo(int index);

  static executor::LogicalTile *ExecuteTile(
      executor::AbstractExecutor *executor,
      executor::LogicalTile *source_logical_tile);

  /**
   * @brief Returns the value populated at the specified field.
   * @param tuple_id Row of the specified field.
   * @param column_id Column of the specified field.
   *
   * This method defines the values that are populated by PopulateTiles().
   *
   * @return Populated value.
   */
  inline static int PopulatedValue(const oid_t tuple_id,
                                   const oid_t column_id) {
    return 10 * tuple_id + column_id;
  }

  static std::unique_ptr<storage::Tuple> GetTuple(storage::DataTable *table,
                                                  oid_t tuple_id,
                                                  common::VarlenPool *pool);
  static std::unique_ptr<storage::Tuple> GetNullTuple(storage::DataTable *table,
                                                      common::VarlenPool *pool);

  /** Print the tuples from a vector of logical tiles */
  static void PrintTileVector(
      std::vector<std::unique_ptr<executor::LogicalTile>> &tile_vec);

  static void Execute(executor::AbstractExecutor *executor,
                      boost::promise<bool> *p);
};

struct ParallelScanArgs {
  boost::promise<bool> p;
  boost::unique_future<bool> f;
  concurrency::Transaction *txn;
  planner::AbstractPlan *node;
  std::shared_ptr<executor::AbstractTask> task;
  bool select_for_update;
  std::vector<int> results;
  std::vector<std::shared_ptr<executor::LogicalTile>> result_tiles;
  ParallelScanArgs *self;

  ParallelScanArgs(concurrency::Transaction *txn, planner::AbstractPlan *node,
                   std::shared_ptr<executor::AbstractTask>& task,
                   bool select_for_update) :
      txn(txn), node(node), task(task),
      select_for_update(select_for_update) {
    f = p.get_future();
    self = this;
  }
};

}  // namespace test
}  // namespace peloton
