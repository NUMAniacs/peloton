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
#include "executor/executor_tests_util.h"

#include "parser/statement_insert.h"

#define LINEITEM_TABLE_SIZE 6000000 / 10
#define PART_TABLE_SIZE 200000 / 10

namespace peloton {
namespace benchmark {
namespace numabench {

storage::Database *numabench_database = nullptr;

storage::DataTable *left_table = nullptr;
storage::DataTable *right_table = nullptr;

void ExecuteTask(executor::AbstractExecutor *executor,
                                boost::promise<bool> *p) {
  auto status = executor->Execute();
  p->set_value(status);
}

void CreateNUMABenchDatabase() {

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

  auto l_id_col = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          "l_id", true);
  auto l_partkey_col = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          "l_partkey", true);
  auto l_shipdate_col = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          "l_shipdate", true);

  std::unique_ptr<catalog::Schema> right_table_schema(
            new catalog::Schema({l_id_col, l_partkey_col, l_shipdate_col}));

  auto p_id_col = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          "p_id", true);
  auto p_partkey_col = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          "p_partkey", true);
  std::unique_ptr<catalog::Schema> left_table_schema(
          new catalog::Schema({p_id_col, p_partkey_col}));




  catalog->CreateDatabase(NUMABENCH_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // create left table
  txn = txn_manager.BeginTransaction();
  // TODO: can I use std move here? I use it to create two tables.
  catalog->CreateTable(NUMABENCH_DB_NAME, "LEFT_TABLE",
                                               std::move(left_table_schema), txn,
                                               1);
  txn_manager.CommitTransaction(txn);
  // create right table
  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(NUMABENCH_DB_NAME, "RIGHT_TABLE",
                                               std::move(right_table_schema), txn,
                                               1);
  txn_manager.CommitTransaction(txn);


}

void LoadHelper(unsigned int num_partition, parser::InsertStatement* insert_stmt, int insert_size, std::vector<std::vector<bool>>& insert_tuple_bitmaps){
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction * txns[num_partition];
  executor::ExecutorContext * contexts[num_partition];
  boost::promise<bool> * promises[num_partition];
  executor::AbstractExecutor * executors[num_partition];
  planner::InsertPlan node(insert_stmt);

  // start the tasks
  for (int partition = 0; partition < (int)num_partition; partition++){
    auto txn = txn_manager.BeginTransaction();
    auto context = new executor::ExecutorContext(txn);
    auto p = new boost::promise<bool>;

    auto insert_task = new executor::InsertTask(&node, insert_size, partition, partition);
    insert_task->tuple_bitmap = insert_tuple_bitmaps[partition];
    std::shared_ptr<executor::AbstractTask> task(insert_task);


    context->SetTask(task);

    executor::AbstractExecutor* executor = new executor::InsertExecutor(context);

    txns[partition] = txn;
    contexts[partition] = context;
    promises[partition] = p;
    executors[partition] = executor;

    partitioned_executor_thread_pool.SubmitTask(
                  partition, ExecuteTask, std::move(executor), std::move(p));
  }

  // wait for the tasks to finish and commit their transactions
  for (int partition = 0; partition < (int)num_partition; partition++){
    promises[partition]->get_future().get();
    txn_manager.CommitTransaction(txns[partition]);
  }

  //clean up and reset tasks
  // wait for the tasks to finish and commit their transactions
  for (int partition = 0; partition < (int)num_partition; partition++){
    // txns are cleaned up on commit
    // tasks are cleaned up using shared ptrs
    delete promises[partition];
    delete executors[partition];
    delete contexts[partition];
    insert_tuple_bitmaps[partition].clear();
    insert_tuple_bitmaps[partition].resize(insert_size, false);
  }
  for (auto val_list : *insert_stmt->insert_values){
    for (auto val : *val_list){
      delete val;
    }
    delete val_list;
  }
  insert_stmt->insert_values->clear();
}

void LoadNUMABenchDatabase() {
  left_table = catalog::Catalog::GetInstance()->GetTableWithName(
          NUMABENCH_DB_NAME, "LEFT_TABLE"
  );

  right_table = catalog::Catalog::GetInstance()->GetTableWithName(
          NUMABENCH_DB_NAME, "RIGHT_TABLE"
  );

  size_t num_partition = PL_NUM_PARTITIONS();


  char *left_table_name_str = new char[11]();
  strcpy(left_table_name_str, "LEFT_TABLE");
  char *left_db_name_str = new char[strlen(NUMABENCH_DB_NAME)+1]();
  strcpy(left_db_name_str, NUMABENCH_DB_NAME);
  char *right_table_name_str = new char[12]();
  strcpy(right_table_name_str, "RIGHT_TABLE");
  char *right_db_name_str = new char[strlen(NUMABENCH_DB_NAME)+1]();
  strcpy(right_db_name_str, NUMABENCH_DB_NAME);
  auto *left_table_name = new parser::TableInfo();
  left_table_name->table_name = left_table_name_str;
  left_table_name->database_name = left_db_name_str;
  auto *right_table_name = new parser::TableInfo();
  right_table_name->table_name = right_table_name_str;
  right_table_name->database_name = right_db_name_str;

  std::vector<std::vector<bool>> insert_tuple_bitmaps;
  insert_tuple_bitmaps.resize(num_partition);
  // size to insert before firing off tasks and waiting
  int insert_size = 10000;
  for (int partition = 0; partition < (int) num_partition; partition++){
    insert_tuple_bitmaps[partition].clear();
    insert_tuple_bitmaps[partition].resize(insert_size, false);
  }



  {

    char *r_col_1 = new char[5]();
    strcpy(r_col_1, "l_id");
    char *r_col_2 = new char[10]();
    strcpy(r_col_2, "l_partkey");
    char *r_col_3 = new char[11]();
    strcpy(r_col_3, "l_shipdate");
    // insert to left table; build an insert statement
    std::unique_ptr<parser::InsertStatement> insert_stmt(
            new parser::InsertStatement(INSERT_TYPE_VALUES));
//  std::unique_ptr<parser::InsertStatement> insert_stmt;
    insert_stmt->table_info_ = right_table_name;
    insert_stmt->columns = new std::vector<char *>;
    insert_stmt->columns->push_back(const_cast<char *>(r_col_1));
    insert_stmt->columns->push_back(const_cast<char *>(r_col_2));
    insert_stmt->columns->push_back(const_cast<char *>(r_col_3));
    insert_stmt->select = new parser::SelectStatement();
    insert_stmt->insert_values =
            new std::vector<std::vector<expression::AbstractExpression *> *>;


    for (int tuple_id = 0; tuple_id < LINEITEM_TABLE_SIZE * state.scale_factor; tuple_id++){
      auto values_ptr = new std::vector<expression::AbstractExpression *>;
      insert_stmt->insert_values->push_back(values_ptr);
      int shipdate = rand()%60;
      int partkey = rand()%(PART_TABLE_SIZE*state.scale_factor);

      values_ptr->push_back(new expression::ConstantValueExpression(
              common::ValueFactory::GetIntegerValue(tuple_id)));
      values_ptr->push_back(new expression::ConstantValueExpression(
              common::ValueFactory::GetIntegerValue(partkey)));
      values_ptr->push_back(new expression::ConstantValueExpression(
              common::ValueFactory::GetIntegerValue(shipdate)));


      int partition_key = state.partition_right ? partkey : tuple_id;
      int partition = common::ValueFactory::GetIntegerValue(partition_key).Hash() % num_partition;
      insert_tuple_bitmaps[partition][tuple_id % insert_size] = true;

      if ((tuple_id + 1) % insert_size == 0){
        LOG_INFO("finished writing tuple in part table: %d", tuple_id+1);
        LoadHelper(num_partition, insert_stmt.get(), insert_size, insert_tuple_bitmaps);
      }

    }
  }
    {

      char *l_col_1 = new char[5]();
      strcpy(l_col_1, "p_id");
      char *l_col_2 = new char[11]();
      strcpy(l_col_2, "p_partkey");
      // insert to left table; build an insert statement
      std::unique_ptr<parser::InsertStatement> insert_stmt(
              new parser::InsertStatement(INSERT_TYPE_VALUES));

      insert_stmt->table_info_ = left_table_name;
      insert_stmt->columns = new std::vector<char *>;
      insert_stmt->columns->push_back(const_cast<char *>(l_col_1));
      insert_stmt->columns->push_back(const_cast<char *>(l_col_2));
      insert_stmt->select = new parser::SelectStatement();
      insert_stmt->insert_values =
              new std::vector<std::vector<expression::AbstractExpression *> *>;

      for (int partkey = 0; partkey < PART_TABLE_SIZE * state.scale_factor; partkey++){
        auto values_ptr = new std::vector<expression::AbstractExpression *>;
        insert_stmt->insert_values->push_back(values_ptr);

        int tuple_id = partkey+PART_TABLE_SIZE*state.scale_factor;
        // this is so when we partition, we do not put in the same place as if
        // we partition on part_Key
        values_ptr->push_back(new expression::ConstantValueExpression(
                common::ValueFactory::GetIntegerValue(tuple_id)));
        values_ptr->push_back(new expression::ConstantValueExpression(
                common::ValueFactory::GetIntegerValue(partkey)));
        //TODO if not partitioning send to random partition
        int partition_key = state.partition_left ? partkey : tuple_id;
        int partition = common::ValueFactory::GetIntegerValue(partition_key).Hash() % num_partition;
        insert_tuple_bitmaps[partition][partkey % insert_size] = true;
        if ((partkey + 1) % insert_size == 0){
          LOG_INFO("finished writing tuple in part table: %d", partkey+1);
          LoadHelper(num_partition, insert_stmt.get(), insert_size, insert_tuple_bitmaps);
        }
      }


  }
}

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
