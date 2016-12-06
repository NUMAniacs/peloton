//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numabench_loader.cpp
//
// Identification: src/main/ycsb/numabench_loader.cpp
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
#include <pthread.h>

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
#include "executor/plan_executor.h"

#include "parser/statement_insert.h"

#define LINEITEM_TABLE_SIZE 6000000
#define PART_TABLE_SIZE 200000

namespace peloton {
namespace benchmark {
namespace numabench {

storage::Database *numabench_database = nullptr;

storage::DataTable *left_table = nullptr;
storage::DataTable *right_table = nullptr;

// source:https://en.wikipedia.org/wiki/Xorshift
class PseudoRand {
public:
  PseudoRand() :
      x(rand()), y(rand()), z(rand()), w(rand()) {
  }

  uint32_t GetRand(void) {
    uint32_t t = x;
    t ^= t << 11;
    t ^= t >> 8;
    x = y;
    y = z;
    z = w;
    w ^= w >> 19;
    w ^= t;
    return w;
  }
private:
  /* These state variables must be initialized so that they are not all zero. */
  uint32_t x, y, z, w;
};


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
  int parallelism = (std::thread::hardware_concurrency() + 1) / 2;
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
                       std::move(left_table_schema), txn, 1);
  txn_manager.CommitTransaction(txn);
  // create right table
  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(NUMABENCH_DB_NAME, "RIGHT_TABLE",
                       std::move(right_table_schema), txn, 1);
  txn_manager.CommitTransaction(txn);
}

/*
 * This class can be notified when a task completes
 */
class NumabenchBlockingWait {
 public:
  NumabenchBlockingWait(int tasks) : all_done(false) {
    tasks_left.store(tasks);
  }

  // when a task completes it will call this
  void DependencyComplete() {
    if (tasks_left.fetch_sub(1) == 1) {
      std::unique_lock<std::mutex> lk(done_lock);
      all_done = true;
      cv.notify_all();
    }
  }

  // wait for all tasks to be complete
  void WaitForCompletion() {
    std::unique_lock<std::mutex> lk(done_lock);
    while (!all_done) cv.wait(lk);
  }

 private:
  std::atomic<int> tasks_left;
  bool all_done;

  // dirty mutex is okay for now since this class will be removed
  std::mutex done_lock;
  std::condition_variable cv;
};

void LoadHelper(unsigned int num_partition,
                parser::InsertStatement *insert_stmt, int insert_size,
                std::vector<std::vector<bool>> &insert_tuple_bitmaps) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txns[num_partition];
  executor::ExecutorContext *contexts[num_partition];
  boost::promise<bool> *promises[num_partition];
  executor::AbstractExecutor *executors[num_partition];
  planner::InsertPlan node(insert_stmt);

  // start the tasks
  for (int partition = 0; partition < (int)num_partition; partition++) {
    auto txn = txn_manager.BeginTransaction();
    auto context = new executor::ExecutorContext(txn);
    auto p = new boost::promise<bool>;

    auto insert_task =
        new executor::InsertTask(&node, insert_size, partition, partition);
    insert_task->tuple_bitmap = insert_tuple_bitmaps[partition];
    std::shared_ptr<executor::AbstractTask> task(insert_task);

    context->SetTask(task);

    executor::AbstractExecutor *executor =
        new executor::InsertExecutor(context);

    txns[partition] = txn;
    contexts[partition] = context;
    promises[partition] = p;
    executors[partition] = executor;

    partitioned_executor_thread_pool.SubmitTask(
        partition, ExecuteTask, std::move(executor), std::move(p));
  }

  // wait for the tasks to finish and commit their transactions
  for (int partition = 0; partition < (int)num_partition; partition++) {
    promises[partition]->get_future().get();
    txn_manager.CommitTransaction(txns[partition]);
  }

  // clean up and reset tasks
  // wait for the tasks to finish and commit their transactions
  for (int partition = 0; partition < (int)num_partition; partition++) {
    // txns are cleaned up on commit
    // tasks are cleaned up using shared ptrs
    delete promises[partition];
    delete executors[partition];
    delete contexts[partition];
    insert_tuple_bitmaps[partition].clear();
    insert_tuple_bitmaps[partition].resize(insert_size, false);
  }
  for (auto val_list : *insert_stmt->insert_values) {
    for (auto val : *val_list) {
      delete val;
    }
    delete val_list;
  }
  insert_stmt->insert_values->clear();
}

struct loaderargs {
  int start;
  int batch_size;
  int total_size;
  NumabenchBlockingWait *block;
};

void *LineItemTableLoader(void *arg) {

  PseudoRand random;
  loaderargs *args = (loaderargs *)arg;
  int start = args->start;
  int batch_size = args->batch_size;
  int total_size = args->total_size;
  NumabenchBlockingWait *block = args->block;
  delete args;

  right_table = catalog::Catalog::GetInstance()->GetTableWithName(
      NUMABENCH_DB_NAME, "RIGHT_TABLE");

  size_t num_partition = PL_NUM_PARTITIONS();

  char *right_table_name_str = new char[12]();
  strcpy(right_table_name_str, "RIGHT_TABLE");
  char *right_db_name_str = new char[strlen(NUMABENCH_DB_NAME) + 1]();
  strcpy(right_db_name_str, NUMABENCH_DB_NAME);

  auto *right_table_name = new parser::TableInfo();
  right_table_name->table_name = right_table_name_str;
  right_table_name->database_name = right_db_name_str;

  std::vector<std::vector<bool>> insert_tuple_bitmaps;
  insert_tuple_bitmaps.resize(num_partition);
  // size to insert before firing off tasks and waiting

  for (int partition = 0; partition < (int)num_partition; partition++) {
    insert_tuple_bitmaps[partition].clear();
    insert_tuple_bitmaps[partition].resize(batch_size, false);
  }

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

  for (int tuple_id = start; tuple_id < start + total_size; tuple_id++) {
    auto values_ptr = new std::vector<expression::AbstractExpression *>;
    insert_stmt->insert_values->push_back(values_ptr);
    int shipdate = random.GetRand() % 60;
    int partkey = random.GetRand() % (PART_TABLE_SIZE * state.scale_factor);

    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(tuple_id)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(partkey)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(shipdate)));

    int partition_key = state.partition_right ? partkey : tuple_id;
    int partition =
        common::ValueFactory::GetIntegerValue(partition_key).Hash() %
        num_partition;
    insert_tuple_bitmaps[partition][tuple_id % batch_size] = true;
    if ((tuple_id + 1) % batch_size == 0) {
      LoadHelper(num_partition, insert_stmt.get(), batch_size,
                 insert_tuple_bitmaps);
      LOG_INFO("finished writing tuple in lineitem table: %d",
               start + (tuple_id + 1));
    }
  }

  block->DependencyComplete();
  return nullptr;
}

void *PartTableLoader(void *arg) {
  PseudoRand random;
  loaderargs *args = (loaderargs *)arg;
  int start = args->start;
  int batch_size = args->batch_size;
  int total_size = args->total_size;
  NumabenchBlockingWait *block = args->block;
  delete args;

  left_table = catalog::Catalog::GetInstance()->GetTableWithName(
      NUMABENCH_DB_NAME, "LEFT_TABLE");

  char *left_table_name_str = new char[11]();
  strcpy(left_table_name_str, "LEFT_TABLE");
  char *left_db_name_str = new char[strlen(NUMABENCH_DB_NAME) + 1]();
  strcpy(left_db_name_str, NUMABENCH_DB_NAME);
  auto *left_table_name = new parser::TableInfo();
  left_table_name->table_name = left_table_name_str;
  left_table_name->database_name = left_db_name_str;

  size_t num_partition = PL_NUM_PARTITIONS();

  std::vector<std::vector<bool>> insert_tuple_bitmaps;
  insert_tuple_bitmaps.resize(num_partition);
  // size to insert before firing off tasks and waiting

  for (int partition = 0; partition < (int)num_partition; partition++) {
    insert_tuple_bitmaps[partition].clear();
    insert_tuple_bitmaps[partition].resize(batch_size, false);
  }

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

  for (int partkey = start; partkey < start + total_size; partkey++) {
    auto values_ptr = new std::vector<expression::AbstractExpression *>;
    insert_stmt->insert_values->push_back(values_ptr);

    int tuple_id = partkey + PART_TABLE_SIZE * state.scale_factor;
    // this is so when we partition, we do not put in the same place as if
    // we partition on part_Key
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(tuple_id)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(partkey)));
    // TODO if not partitioning send to random partition
    int partition_key = state.partition_left ? partkey : tuple_id;
    int partition =
        common::ValueFactory::GetIntegerValue(partition_key).Hash() %
        PL_NUM_PARTITIONS();
    ;
    insert_tuple_bitmaps[partition][partkey % batch_size] = true;
    if ((partkey + 1) % batch_size == 0) {
      LOG_INFO("finished writing tuple in part table: %d", partkey + 1);
      LoadHelper(num_partition, insert_stmt.get(), batch_size,
                 insert_tuple_bitmaps);
    }
  }

  block->DependencyComplete();
  return nullptr;
}

void LoadNUMABenchDatabase() {

  int insert_size = 100000;
  int num_threads = 8;
  {

    NumabenchBlockingWait block(num_threads);
    for (int tuple_id = 0; tuple_id < LINEITEM_TABLE_SIZE * state.scale_factor;
         tuple_id += LINEITEM_TABLE_SIZE * state.scale_factor / num_threads) {
      pthread_t thread;
      pthread_create(&thread, nullptr, LineItemTableLoader,
                     new loaderargs{tuple_id, insert_size,
                                    LINEITEM_TABLE_SIZE * state.scale_factor /
                                        num_threads,
                                    &block});
      pthread_detach(thread);
    }
    block.WaitForCompletion();
  }
  {
    NumabenchBlockingWait block(num_threads);
    for (int partkey = 0; partkey < PART_TABLE_SIZE * state.scale_factor;
         partkey += PART_TABLE_SIZE * state.scale_factor / num_threads) {
      pthread_t thread;
      pthread_create(
          &thread, nullptr, PartTableLoader,
          new loaderargs{
              partkey,                                            insert_size,
              PART_TABLE_SIZE * state.scale_factor / num_threads, &block});
      pthread_detach(thread);
    }
    block.WaitForCompletion();
  }
}

}  // namespace numabench
}  // namespace benchmark
}  // namespace peloton
