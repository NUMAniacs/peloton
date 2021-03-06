//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile_test.cpp
//
// Identification: test/executor/logical_tile_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include <utility>
#include <vector>

#include "common/harness.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/types.h"
#include "common/value_factory.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"
#include "storage/tile.h"

#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logical Tile Tests
//===--------------------------------------------------------------------===//

class LogicalTileTests : public PelotonTest {};

TEST_F(LogicalTileTests, TileMaterializationTest) {
  const int tuple_count = 4;
  std::shared_ptr<storage::TileGroup> tile_group(
      ExecutorTestsUtil::CreateTileGroup(tuple_count));

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchemaList(tile_schemas));

  // Create tuples and insert them into tile group.
  const bool allocate = true;
  storage::Tuple tuple1(schema.get(), allocate);
  storage::Tuple tuple2(schema.get(), allocate);
  auto pool = tile_group->GetTilePool(1);

  tuple1.SetValue(0, common::ValueFactory::GetIntegerValue(1), pool);
  tuple1.SetValue(1, common::ValueFactory::GetIntegerValue(1), pool);
  tuple1.SetValue(2, common::ValueFactory::GetTinyIntValue(1), pool);
  tuple1.SetValue(3, common::ValueFactory::GetVarcharValue("tuple 1"), pool);

  tuple2.SetValue(0, common::ValueFactory::GetIntegerValue(2), pool);
  tuple2.SetValue(1, common::ValueFactory::GetIntegerValue(2), pool);
  tuple2.SetValue(2, common::ValueFactory::GetTinyIntValue(2), pool);
  tuple2.SetValue(3, common::ValueFactory::GetVarcharValue("tuple 2"), pool);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // txn_id_t txn_id = txn->GetTransactionId();

  auto tuple_id1 = tile_group->InsertTuple(&tuple1);
  auto tuple_id2 = tile_group->InsertTuple(&tuple2);
  auto tuple_id3 = tile_group->InsertTuple(&tuple1);

  ItemPointer *index_entry_ptr = nullptr;
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_id1), index_entry_ptr);
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_id2), index_entry_ptr);
  txn_manager.PerformInsert(txn,
      ItemPointer(tile_group->GetTileGroupId(), tuple_id3), index_entry_ptr);

  txn_manager.CommitTransaction(txn);

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (1 BASE TILE)
  ////////////////////////////////////////////////////////////////

  // Don't transfer ownership of any base tile to logical tile.
  auto base_tile_ref = tile_group->GetTileReference(1);

  std::vector<oid_t> position_list1 = {0, 1};
  std::vector<oid_t> position_list2 = {0, 1};

  std::unique_ptr<executor::LogicalTile> logical_tile(
      executor::LogicalTileFactory::GetTile(UNDEFINED_NUMA_REGION));

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));

  PL_ASSERT(tile_schemas.size() == 2);
  catalog::Schema *schema1 = &tile_schemas[0];
  catalog::Schema *schema2 = &tile_schemas[1];
  oid_t column_count = schema2->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    logical_tile->AddColumn(base_tile_ref, column_itr, column_itr);
  }

  LOG_INFO("%s", logical_tile->GetInfo().c_str());

  ////////////////////////////////////////////////////////////////
  // LOGICAL TILE (2 BASE TILE)
  ////////////////////////////////////////////////////////////////

  logical_tile.reset(
      executor::LogicalTileFactory::GetTile(UNDEFINED_NUMA_REGION));

  auto base_tile_ref1 = tile_group->GetTileReference(0);
  auto base_tile_ref2 = tile_group->GetTileReference(1);

  position_list1 = {0, 1};
  position_list2 = {0, 1};
  std::vector<oid_t> position_list3 = {0, 1};
  std::vector<oid_t> position_list4 = {0, 1};

  logical_tile->AddPositionList(std::move(position_list1));
  logical_tile->AddPositionList(std::move(position_list2));
  logical_tile->AddPositionList(std::move(position_list3));
  logical_tile->AddPositionList(std::move(position_list4));

  oid_t column_count1 = schema1->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count1; column_itr++) {
    logical_tile->AddColumn(base_tile_ref1, column_itr, column_itr);
  }

  oid_t column_count2 = schema2->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count2; column_itr++) {
    logical_tile->AddColumn(base_tile_ref2, column_itr,
                            column_count1 + column_itr);
  }

  LOG_INFO("%s", logical_tile->GetInfo().c_str());
}

}  // End test namespace
}  // End peloton namespace
