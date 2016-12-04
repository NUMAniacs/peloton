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

#include "benchmark/scanbench/scanbench_loader.h"
#include "benchmark/scanbench/scanbench_config.h"
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

namespace peloton {
namespace benchmark {
namespace scanbench {

storage::Database *scanbench_database = nullptr;

storage::DataTable *scan_table = nullptr;

std::string id_col_name = "id";

std::string extra_id_col_name = "extra_id";

std::string single_col_name = "single";

std::string scan_table_name = "SCAN_TABLE";

std::string scanbench_db_name = "SCANBENCH_DB";


void ExecuteTask(executor::AbstractExecutor *executor,
                                boost::promise<bool> *p) {
  auto status = executor->Execute();
  p->set_value(status);
}

void CreateScanBenchDatabase() {

  /////////////////////////////////////////////////////////
  // Create database & tables
  /////////////////////////////////////////////////////////
  // Clean up
  delete scanbench_database;
  scanbench_database = nullptr;
  scan_table = nullptr;

  std::string not_null_constraint_name = "not_null";
  std::string pkey_constraint_name = "primary key";

  auto catalog = catalog::Catalog::GetInstance();
  int parallelism =  (std::thread::hardware_concurrency() + 1) / 2;
  storage::DataTable::SetActiveTileGroupCount(parallelism);
  storage::DataTable::SetActiveIndirectionArrayCount(parallelism);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // create the columns
  auto s_id = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          id_col_name, true);
  s_id.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                           not_null_constraint_name));
  s_id.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_PRIMARY,
                                         pkey_constraint_name));
  auto s_extra_id = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          extra_id_col_name, true);
  auto s_single = catalog::Column(
          common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
          single_col_name, true);

  // create the schema
  std::unique_ptr<catalog::Schema> scan_table_schema(
            new catalog::Schema({s_id, s_extra_id, s_single}));

  // create the database
  catalog->CreateDatabase(scanbench_db_name, txn);

  int partition_column;

  if (state.numa_aware) {
    partition_column = 1;
  } else {
    partition_column = NO_PARTITION_COLUMN;
  }

  // create scan table
  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(scanbench_db_name, scan_table_name,
                       std::move(scan_table_schema), txn, partition_column);
  txn_manager.CommitTransaction(txn);
}

void ParallelLoader(unsigned int num_partition, parser::InsertStatement* insert_stmt,
                int insert_size, std::vector<std::vector<bool>>& insert_tuple_bitmaps){
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();


  if (state.numa_aware == false) {
    auto txn = txn_manager.BeginTransaction();
    auto context = new executor::ExecutorContext(txn);
    auto p = new boost::promise<bool>;

    // populate random partition
    size_t partition_id = rand() % num_partitions;
    std::shared_ptr<executor::AbstractTask> insert_task(
        new executor::InsertTask(plan_tree,
                                 insert_plan->GetBulkInsertCount(),
                                 INVALID_TASK_ID, partition_id));

    std::shared_ptr<executor::AbstractTask> task(insert_task);
    context->SetTask(task);

    executor::AbstractExecutor* executor = new executor::InsertExecutor(context);

    partitioned_executor_thread_pool.SubmitTask(
        partition_id, ExecuteTask, std::move(executor), std::move(p));

    // wait for the tasks to finish and commit their transaction
    p->get_future().get();
    txn_manager.CommitTransaction(txn);

  } else {
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
  }

  // clear out all the insert values
  for (auto val_list : *insert_stmt->insert_values){
    for (auto val : *val_list){
      delete val;
    }
    delete val_list;
  }
  insert_stmt->insert_values->clear();
}

void LoadScanBenchDatabase() {
  scan_table = catalog::Catalog::GetInstance()->GetTableWithName(
          scanbench_db_name, scan_table_name
  );

  size_t num_partition = PL_NUM_PARTITIONS();

  auto *scan_table_info = new parser::TableInfo();
  char *table_name = new char[scan_table_name.size() + 1];
  std::copy(scan_table_name.begin(), scan_table_name.end(), table_name);
  table_name[scan_table_name.size()] = '\0';
  scan_table_info->table_name = table_name;

  char *db_name = new char[scanbench_db_name.size() + 1];
  std::copy(scanbench_db_name.begin(), scanbench_db_name.end(), db_name);
  db_name[scanbench_db_name.size()] = '\0';
  scan_table_info->database_name = db_name;

  std::vector<std::vector<bool>> insert_tuple_bitmaps;
  insert_tuple_bitmaps.resize(num_partition);

  // size to insert before firing off tasks and waiting
  int insert_size = 100000;
  for (int partition = 0; partition < (int) num_partition; partition++){
    insert_tuple_bitmaps[partition].clear();
    insert_tuple_bitmaps[partition].resize(insert_size, false);
  }

  // insert to scan table; build an insert statement
  std::unique_ptr<parser::InsertStatement> insert_stmt(
          new parser::InsertStatement(INSERT_TYPE_VALUES));

  insert_stmt->table_info_ = scan_table_info;
  insert_stmt->columns = new std::vector<char *>;

  char *col1 = new char[id_col_name.length()+1];
  std::copy(id_col_name.begin(), id_col_name.end(), col1);
  col1[id_col_name.size()] = '\0';
  insert_stmt->columns->push_back(col1);

  char *col2 = new char[extra_id_col_name.length()+1];
  std::copy(extra_id_col_name.begin(), extra_id_col_name.end(), col2);
  col2[extra_id_col_name.size()] = '\0';
  insert_stmt->columns->push_back(col2);

  char *col3 = new char[single_col_name.length()+1];
  std::copy(single_col_name.begin(), single_col_name.end(), col3);
  col3[single_col_name.size()] = '\0';
  insert_stmt->columns->push_back(col3);

  insert_stmt->select = new parser::SelectStatement();
  insert_stmt->insert_values =
          new std::vector<std::vector<expression::AbstractExpression *> *>;


  int total_tuples = SCAN_TABLE_SIZE * state.scale_factor;

  for (int tuple_id = 0; tuple_id < total_tuples; tuple_id++) {
    auto values_ptr = new std::vector<expression::AbstractExpression *>;
    insert_stmt->insert_values->push_back(values_ptr);

    values_ptr->push_back(new expression::ConstantValueExpression(
            common::ValueFactory::GetIntegerValue(tuple_id)));
    values_ptr->push_back(new expression::ConstantValueExpression(
            common::ValueFactory::GetIntegerValue(tuple_id%1000)));
    values_ptr->push_back(new expression::ConstantValueExpression(
            common::ValueFactory::GetIntegerValue(tuple_id)));

    // partition if we are numa-aware
    if (state.numa_aware == true) {
      int partition_key = tuple_id;
      int partition = common::ValueFactory::GetIntegerValue(partition_key).Hash() % num_partition;
      insert_tuple_bitmaps[partition][tuple_id % insert_size] = true;
    }

    if ((tuple_id + 1) % insert_size == 0){
      ParallelLoader(num_partition, insert_stmt.get(), insert_size, insert_tuple_bitmaps);
      LOG_INFO("Inserted %d rows out of %d rows", tuple_id+1, total_tuples);
    }
  }
}

}  // namespace scanbench
}  // namespace benchmark
}  // namespace peloton