
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_test.cpp
//
// Identification: /peloton/test/executor/insert_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "gtest/gtest.h"

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/partition_macros.h"
#include "common/logger.h"
#include "executor/insert_executor.h"
#include "expression/tuple_value_expression.h"
#include "parser/statement_insert.h"
#include "parser/statement_select.h"
#include "planner/insert_plan.h"
#include "executor/plan_executor.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class InsertTests : public PelotonTest {};

TEST_F(InsertTests, InsertRecord) {
  catalog::Catalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, "TEST_TABLE",
                                               std::move(table_schema), txn,
                                               NO_PARTITION_COLUMN);
  txn_manager.CommitTransaction(txn);

  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "TEST_TABLE");

  txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::unique_ptr<parser::InsertStatement> insert_node(
      new parser::InsertStatement(INSERT_TYPE_VALUES));

  char *name = new char[11]();
  strcpy(name, "TEST_TABLE");
  parser::TableInfo *table_info = new parser::TableInfo();
  table_info->table_name = name;

  char *col_1 = new char[8]();
  strcpy(col_1, "dept_id");

  char *col_2 = new char[10]();
  strcpy(col_2, "dept_name");

  insert_node->table_info_ = table_info;

  insert_node->columns = new std::vector<char *>;
  insert_node->columns->push_back(const_cast<char *>(col_1));
  insert_node->columns->push_back(const_cast<char *>(col_2));

  insert_node->insert_values =
      new std::vector<std::vector<expression::AbstractExpression *> *>;
  auto values_ptr = new std::vector<expression::AbstractExpression *>;
  insert_node->insert_values->push_back(values_ptr);

  values_ptr->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(70)));

  values_ptr->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetVarcharValue("Hello")));
  // why there is a select statement
  insert_node->select = new parser::SelectStatement();

  planner::InsertPlan node(insert_node.get());
  std::shared_ptr<executor::AbstractTask> task(
      new executor::InsertTask(&node, node.GetBulkInsertCount()));
  context->SetTask(task);
  executor::InsertExecutor executor(context.get());

  EXPECT_TRUE(executor.Init());
  EXPECT_TRUE(executor.Execute());
  EXPECT_EQ(1, table->GetTupleCount());

  delete values_ptr->at(0);
  values_ptr->at(0) = new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(80));
  planner::InsertPlan node2(insert_node.get());
  std::shared_ptr<executor::AbstractTask> task2(
      new executor::InsertTask(&node2, node2.GetBulkInsertCount()));
  context->SetTask(task2);
  executor::InsertExecutor executor2(context.get());

  EXPECT_TRUE(executor2.Init());
  EXPECT_TRUE(executor2.Execute());
  EXPECT_EQ(2, table->GetTupleCount());

  auto values_ptr2 = new std::vector<expression::AbstractExpression *>;
  insert_node->insert_values->push_back(values_ptr2);

  values_ptr2->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(100)));

  values_ptr2->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetVarcharValue("Hello")));

  delete values_ptr->at(0);
  values_ptr->at(0) = new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(90));
  planner::InsertPlan node3(insert_node.get());
  std::shared_ptr<executor::AbstractTask> task3(
      new executor::InsertTask(&node3, node3.GetBulkInsertCount()));
  context->SetTask(task3);
  executor::InsertExecutor executor3(context.get());

  EXPECT_TRUE(executor3.Init());
  EXPECT_TRUE(executor3.Execute());
  EXPECT_EQ(4, table->GetTupleCount());

  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(InsertTests, InsertPartitionedRecord) {
  catalog::Catalog::GetInstance();

  // start executor pool
  ExecutorPoolHarness::GetInstance();

  // Set parallelism for data table
  int parallelism = (std::thread::hardware_concurrency() + 1) / 2;
  storage::DataTable::SetActiveTileGroupCount(parallelism);
  storage::DataTable::SetActiveIndirectionArrayCount(parallelism);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, "TEST_TABLE",
                                               std::move(table_schema), txn,
                                               NO_PARTITION_COLUMN);
  txn_manager.CommitTransaction(txn);

  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "TEST_TABLE");

  size_t num_partition = PL_NUM_PARTITIONS();
  LOG_DEBUG("Total number of partitions: %d", (int)num_partition);
  txn = txn_manager.BeginTransaction();

  // Expression for columns and table names
  char *name = new char[11]();
  strcpy(name, "TEST_TABLE");
  parser::TableInfo *table_info = new parser::TableInfo();
  table_info->table_name = name;

  char *col_1 = new char[8]();
  strcpy(col_1, "dept_id");
  char *col_2 = new char[10]();
  strcpy(col_2, "dept_name");

  // Build an insert stmt
  std::unique_ptr<parser::InsertStatement> insert_stmt(
      new parser::InsertStatement(INSERT_TYPE_VALUES));
  insert_stmt->table_info_ = table_info;
  insert_stmt->columns = new std::vector<char *>;
  insert_stmt->columns->push_back(const_cast<char *>(col_1));
  insert_stmt->columns->push_back(const_cast<char *>(col_2));
  insert_stmt->select = new parser::SelectStatement();
  insert_stmt->insert_values =
      new std::vector<std::vector<expression::AbstractExpression *> *>;

  // Initialize one tuple per partition
  for (size_t partition = 0; partition < num_partition; partition++) {
    auto values_ptr = new std::vector<expression::AbstractExpression *>;
    insert_stmt->insert_values->push_back(values_ptr);
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(70)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetVarcharValue("Hello")));
  }

  // Construct insert plan
  planner::InsertPlan node(insert_stmt.get());
  // Construct the executor context
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::shared_ptr<executor::Trackable> trackable(
      new executor::Trackable(num_partition));
  bridge::BlockingWait wait;

  // Construct the task. Each partition has 1 tuple to insert
  for (size_t partition = 0; partition < num_partition; partition++) {
    size_t task_id = partition;
    LOG_INFO("Execute insert task on partition %d", (int)partition);
    executor::InsertTask *insert_task = new executor::InsertTask(
        &node, node.GetBulkInsertCount(), task_id, partition);
    insert_task->tuple_bitmap.clear();
    insert_task->tuple_bitmap.resize(node.GetBulkInsertCount(), false);
    // Only insert the tuple in this partition
    insert_task->tuple_bitmap[partition] = true;
    std::shared_ptr<executor::AbstractTask> task(insert_task);
    task->Init(trackable, &wait, num_partition, txn);

    partitioned_executor_thread_pool.SubmitTask(
        partition, executor::InsertExecutor::ExecuteTask, std::move(task));
  }
  wait.WaitForCompletion();
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(num_partition, (int)table->GetTupleCount());

  size_t tile_group_per_region = parallelism / PL_NUM_PARTITIONS();
  // Check if the tuples go to the correct partition
  for (size_t partition = 0; partition < num_partition; partition++) {
    oid_t total_num_tuples = 0;
    // Accumulate the total number of tuple per region
    for (size_t tile_group_itr = 0; tile_group_itr < tile_group_per_region;
         tile_group_itr++) {
      auto tile_group =
          table->GetTileGroupFromPartition(partition, tile_group_itr);
      total_num_tuples += tile_group->GetActiveTupleCount();
    }
    EXPECT_EQ(total_num_tuples, 1);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
