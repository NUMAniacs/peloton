//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_loader.cpp
//
// Identification: src/main/ycsb/ycsb_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>

#include "benchmark/numabench/numabench_loader.h"
#include "benchmark/numabench/numabench_configuration.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "executor/executor_tests_util.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "index/index_factory.h"
#include "planner/insert_plan.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/database.h"

#include <cstdio>

#include "gtest/gtest.h"

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "executor/insert_executor.h"
#include "expression/abstract_expression.h"
#include "parser/statement_insert.h"
#include "parser/statement_select.h"
#include "planner/insert_plan.h"
#include "executor/executor_tests_util.h"
// Logging mode
extern LoggingType peloton_logging_mode;

namespace peloton {
namespace benchmark {
namespace numabench {

storage::Database *numabench_database = nullptr;

storage::DataTable *left_table = nullptr;
storage::DataTable *right_table = nullptr;

void CreateNUMABenchDatabase() {

  const bool is_inlined = false;

  /////////////////////////////////////////////////////////
  // Create database & tables
  /////////////////////////////////////////////////////////
  // Clean up
  delete numabench_database;
  numabench_database = nullptr;
  left_table = nullptr;
  right_table = nullptr;

  auto catalog = catalog::Catalog::GetInstance();
  int parallelism =  (std::thread::hardware_concurrency() + 1) / 2;
  storage::DataTable::SetActiveTileGroupCount(parallelism);
  storage::DataTable::SetActiveIndirectionArrayCount(parallelism);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // TODO: who is the primary key?????
  auto foo_column = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          "foo", true);
  auto bar_column = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          "bar", true);

  std::unique_ptr<catalog::Schema> table_schema(
          new catalog::Schema({foo_column, bar_column}));

  catalog::Catalog::GetInstance()->CreateDatabase(NUMABENCH_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // create left table
  txn = txn_manager.BeginTransaction();
  // TODO: can I use std move here? I use it to create two tables.
  catalog::Catalog::GetInstance()->CreateTable(NUMABENCH_DB_NAME, "LEFT_TABLE",
                                               std::move(table_schema), txn,
                                               NO_PARTITION_COLUMN);
  txn_manager.CommitTransaction(txn);
  // create right table
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, "RIGHT_TABLE",
                                               std::move(table_schema), txn,
                                               NO_PARTITION_COLUMN);
  txn_manager.CommitTransaction(txn);

//  // Primary index on user key
//  std::vector<oid_t> key_attrs;
//
//  auto tuple_schema = user_table->GetSchema();
//  catalog::Schema *key_schema;
//  index::IndexMetadata *index_metadata;
//  bool unique;
//
//  key_attrs = {0};
//  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
//  key_schema->SetIndexedColumns(key_attrs);
//
//  unique = true;
//
//  index_metadata = new index::IndexMetadata(
//    "primary_index", user_table_pkey_index_oid, user_table_oid,
//    numabench_database_oid, state.index, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
//    tuple_schema, key_schema, key_attrs, unique);
//
//  std::shared_ptr<index::Index> pkey_index(
//      index::IndexFactory::GetInstance(index_metadata));
//  user_table->AddIndex(pkey_index);
}

void LoadNUMABenchDatabase() {
  left_table = catalog::Catalog::GetInstance()->GetTableWithName(
          NUMABENCH_DB_NAME, "LEFT_TABLE"
  );

  right_table = catalog::Catalog::GetInstance()->GetTableWithName(
          NUMABENCH_DB_NAME, "RIGHT_TABLE"
  );

  size_t num_partition = PL_NUM_PARTITIONS();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  char *left_table_name_str = new char[11]();
  strcpy(left_table_name_str, "LEFT_TABLE");
  char *right_table_name_str = new char[12]();
  strcpy(right_table_name_str, "RIGHT_TABLE");
  expression::ParserExpression *left_table_name = new expression::ParserExpression(
          EXPRESSION_TYPE_TABLE_REF, left_table_name_str, nullptr);
  expression::ParserExpression *right_table_name = new expression::ParserExpression(
          EXPRESSION_TYPE_TABLE_REF, right_table_name_str, nullptr);
  char *col_1 = new char[4]();
  strcpy(col_1, "foo");
  char *col_2 = new char[4]();
  strcpy(col_2, "bar");

  {
    // insert to left table; build an insert statement
    // TODO: Why doesn't it work??? Do not have the constructor???
    std::unique_ptr<parser::InsertStatement> insert_stmt(
            new parser::InsertStatement(INSERT_TYPE_VALUES));
//  std::unique_ptr<parser::InsertStatement> insert_stmt;
    insert_stmt->table_name = left_table_name;
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
              common::ValueFactory::GetIntegerValue(90)));
    }

    // Construct insert plan
    planner::InsertPlan node(insert_stmt.get());
    // Construct the executor context
    std::unique_ptr<executor::ExecutorContext> context(
            new executor::ExecutorContext(txn));
    // Construct the task. Each partition has 1 tuple to insert
    for (size_t partition = 0; partition < num_partition; partition++) {
      size_t task_id = partition;
      LOG_INFO("Execute insert task on partition %d", (int) partition);
      executor::InsertTask *insert_task =
              new executor::InsertTask(&node, node.GetBulkInsertCount(), task_id, partition);
      insert_task->tuple_bitmap.clear();
      insert_task->tuple_bitmap.resize(node.GetBulkInsertCount(), false);
      // Only insert the tuple in this partition
      insert_task->tuple_bitmap[partition] = true;
      std::shared_ptr<executor::AbstractTask> task(insert_task);
      context->SetTask(task);
      executor::InsertExecutor executor(context.get());

      // Setup promise future for partitioned thread execution
      // XXX We should use callbacks instead
      boost::promise<bool> p;
      boost::unique_future<bool> execute_status = p.get_future();

//      EXPECT_TRUE(executor.Init());
      partitioned_executor_thread_pool.SubmitTask(
              partition, test::ExecutorTestsUtil::Execute, &executor, &p);

//      EXPECT_TRUE(execute_status.get());
//      EXPECT_EQ(partition + 1, (int) table->GetTupleCount());
    }

    txn_manager.CommitTransaction(txn);
  }

  {
    // insert to right table; build an insert statement
    // TODO: Why doesn't it work??? Do not have the constructor???
    std::unique_ptr<parser::InsertStatement> insert_stmt(
            new parser::InsertStatement(INSERT_TYPE_VALUES));
//  std::unique_ptr<parser::InsertStatement> insert_stmt;
    insert_stmt->table_name = right_table_name;
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
              common::ValueFactory::GetIntegerValue(90)));
    }

    // Construct insert plan
    planner::InsertPlan node(insert_stmt.get());
    // Construct the executor context
    std::unique_ptr<executor::ExecutorContext> context(
            new executor::ExecutorContext(txn));
    // Construct the task. Each partition has 1 tuple to insert
    for (size_t partition = 0; partition < num_partition; partition++) {
      size_t task_id = partition;
      LOG_INFO("Execute insert task on partition %d", (int) partition);
      executor::InsertTask *insert_task =
              new executor::InsertTask(&node, node.GetBulkInsertCount(), task_id, partition);
      insert_task->tuple_bitmap.clear();
      insert_task->tuple_bitmap.resize(node.GetBulkInsertCount(), false);
      // Only insert the tuple in this partition
      insert_task->tuple_bitmap[partition] = true;
      std::shared_ptr<executor::AbstractTask> task(insert_task);
      context->SetTask(task);
      executor::InsertExecutor executor(context.get());

      // Setup promise future for partitioned thread execution
      // XXX We should use callbacks instead
      boost::promise<bool> p;
      boost::unique_future<bool> execute_status = p.get_future();

      // TODO: Why does not macro EXPECT_TRUE work?
//      EXPECT_TRUE(executor.Init());
      partitioned_executor_thread_pool.SubmitTask(
              partition, test::ExecutorTestsUtil::Execute, &executor, &p);

//      EXPECT_TRUE(execute_status.get());
//      EXPECT_EQ(partition + 1, (int) table->GetTupleCount());
    }

    txn_manager.CommitTransaction(txn);
  }
}

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
