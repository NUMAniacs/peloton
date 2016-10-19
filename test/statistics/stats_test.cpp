//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_test.cpp
//
// Identification: test/statistics/stats_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "common/config.h"
#include "common/harness.h"

#include <sys/resource.h>
#include <time.h>
#include <include/tcop/tcop.h>

#include "executor/executor_context.h"
#include "executor/executor_tests_util.h"
#include "executor/insert_executor.h"
#include "statistics/backend_stats_context.h"
#include "statistics/stats_aggregator.h"
#include "statistics/stats_tests_util.h"
#include "tcop/tcop.h"

#define NUM_ITERATION 50
#define NUM_TABLE_INSERT 1
#define NUM_TABLE_DELETE 2
#define NUM_TABLE_UPDATE 3
#define NUM_TABLE_READ 4
#define NUM_INDEX_INSERT 1
#define NUM_INDEX_DELETE 2
#define NUM_INDEX_READ 4
#define NUM_DB_COMMIT 1
#define NUM_DB_ABORT 2

namespace peloton {
namespace test {

class StatsTest : public PelotonTest {};

// Launch the aggregator thread manually
void LaunchAggregator(int64_t stat_interval) {
  FLAGS_stats_mode = STATS_TYPE_ENABLE;
  auto &aggregator =
      peloton::stats::StatsAggregator::GetInstance(stat_interval);
  aggregator.GetAggregatedStats().ResetQueryCount();
  aggregator.ShutdownAggregator();
  aggregator.LaunchAggregator();
}

// Force a final aggregation
void ForceFinalAggregation(int64_t stat_interval) {
  auto &aggregator =
      peloton::stats::StatsAggregator::GetInstance(stat_interval);
  int64_t interval_cnt = 0;
  double alpha = 0;
  double weighted_avg_throughput = 0;
  aggregator.Aggregate(interval_cnt, alpha, weighted_avg_throughput);
}

void TransactionTest(storage::Database *database, storage::DataTable *table,
                     UNUSED_ATTRIBUTE uint64_t thread_itr) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();
  auto tile_group_id = table->GetTileGroup(0)->GetTileGroupId();
  auto index_metadata = table->GetIndex(0)->GetMetadata();
  auto db_oid = database->GetOid();
  auto context = stats::BackendStatsContext::GetInstance();

  for (oid_t txn_itr = 1; txn_itr <= NUM_ITERATION; txn_itr++) {
    context->InitQueryMetric("query_string", db_oid);

    if (thread_id % 2 == 0) {
      std::chrono::microseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    }

    // Record table stat
    for (int i = 0; i < NUM_TABLE_READ; i++) {
      context->IncrementTableReads(tile_group_id);
    }
    for (int i = 0; i < NUM_TABLE_UPDATE; i++) {
      context->IncrementTableUpdates(tile_group_id);
    }
    for (int i = 0; i < NUM_TABLE_INSERT; i++) {
      context->IncrementTableInserts(tile_group_id);
    }
    for (int i = 0; i < NUM_TABLE_DELETE; i++) {
      context->IncrementTableDeletes(tile_group_id);
    }

    // Record index stat
    context->IncrementIndexReads(NUM_INDEX_READ, index_metadata);
    context->IncrementIndexDeletes(NUM_INDEX_DELETE, index_metadata);
    for (int i = 0; i < NUM_INDEX_INSERT; i++) {
      context->IncrementIndexInserts(index_metadata);
    }

    // Record database stat
    for (int i = 0; i < NUM_DB_COMMIT; i++) {
      context->GetOnGoingQueryMetric()->GetQueryLatency().RecordLatency();
      context->IncrementTxnCommitted(db_oid);
    }
    for (int i = 0; i < NUM_DB_ABORT; i++) {
      context->IncrementTxnAborted(db_oid);
    }
  }
}

TEST_F(StatsTest, MultiThreadStatsTest) {
  auto catalog = catalog::Catalog::GetInstance();

  // Launch aggregator thread
  int64_t aggregate_interval = 100;
  LaunchAggregator(aggregate_interval);
  auto &aggregator = stats::StatsAggregator::GetInstance();

  // Create database, table and index
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "id", true);
  catalog::Constraint constraint(CONSTRAINT_TYPE_PRIMARY, "con_primary");
  id_column.AddConstraint(constraint);
  auto name_column = catalog::Column(common::Type::VARCHAR, 32, "name", true);
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  catalog->CreateDatabase("EMP_DB", txn);
  catalog::Catalog::GetInstance()->CreateTable("EMP_DB", "emp_table",
                                               std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // Create multiple stat worker threads
  int num_threads = 8;
  storage::Database *database = catalog->GetDatabaseWithName("EMP_DB");
  storage::DataTable *table = database->GetTableWithName("emp_table");
  LaunchParallelTest(num_threads, TransactionTest, database, table);

  // Wait for aggregation to finish
  std::chrono::microseconds sleep_time(aggregate_interval * 2 * 1000);
  std::this_thread::sleep_for(sleep_time);
  aggregator.ShutdownAggregator();
  // Force a final aggregation
  ForceFinalAggregation(aggregate_interval);

  // Check query metrics
  auto &aggregated_stats = aggregator.GetAggregatedStats();
  ASSERT_EQ(aggregated_stats.GetQueryCount(), num_threads * NUM_ITERATION);

  // Check database metrics
  auto db_oid = database->GetOid();
  auto db_metric = aggregated_stats.GetDatabaseMetric(db_oid);
  ASSERT_EQ(db_metric->GetTxnCommitted().GetCounter(),
            num_threads * NUM_ITERATION * NUM_DB_COMMIT);
  ASSERT_EQ(db_metric->GetTxnAborted().GetCounter(),
            num_threads * NUM_ITERATION * NUM_DB_ABORT);

  // Check table metrics
  auto table_oid = table->GetOid();
  auto table_metric = aggregated_stats.GetTableMetric(db_oid, table_oid);
  auto table_access = table_metric->GetTableAccess();
  ASSERT_EQ(table_access.GetReads(),
            num_threads * NUM_ITERATION * NUM_TABLE_READ);
  ASSERT_EQ(table_access.GetUpdates(),
            num_threads * NUM_ITERATION * NUM_TABLE_UPDATE);
  ASSERT_EQ(table_access.GetDeletes(),
            num_threads * NUM_ITERATION * NUM_TABLE_DELETE);
  ASSERT_EQ(table_access.GetInserts(),
            num_threads * NUM_ITERATION * NUM_TABLE_INSERT);

  // Check index metrics
  auto index_oid = table->GetIndex(0)->GetOid();
  auto index_metric =
      aggregated_stats.GetIndexMetric(db_oid, table_oid, index_oid);
  auto index_access = index_metric->GetIndexAccess();
  ASSERT_EQ(index_access.GetReads(),
            num_threads * NUM_ITERATION * NUM_INDEX_READ);
  ASSERT_EQ(index_access.GetDeletes(),
            num_threads * NUM_ITERATION * NUM_INDEX_DELETE);
  ASSERT_EQ(index_access.GetInserts(),
            num_threads * NUM_ITERATION * NUM_INDEX_INSERT);

  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName("EMP_DB", txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(StatsTest, PerThreadStatsTest) {
  FLAGS_stats_mode = STATS_TYPE_ENABLE;

  // Register to StatsAggregator
  auto &aggregator = peloton::stats::StatsAggregator::GetInstance(1000000);

  // int tuple_count = 10;
  int tups_per_tile_group = 100;
  int num_rows = 10;

  // Create a table and wrap it in logical tiles
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tups_per_tile_group, true));

  // Ensure that the tile group is as expected.
  const catalog::Schema *schema = data_table->GetSchema();
  PL_ASSERT(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  std::vector<ItemPointer> tuple_slot_ids;

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;

    storage::Tuple tuple = StatsTestsUtil::PopulateTuple(
        schema, ExecutorTestsUtil::PopulatedValue(populate_value, 0),
        ExecutorTestsUtil::PopulatedValue(populate_value, 1),
        ExecutorTestsUtil::PopulatedValue(populate_value, 2),
        ExecutorTestsUtil::PopulatedValue(populate_value, 3));

    std::unique_ptr<const planner::ProjectInfo> project_info{
        TransactionTestsUtil::MakeProjectInfoFromTuple(&tuple)};

    // Insert
    planner::InsertPlan node(data_table.get(), std::move(project_info));
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }
  txn_manager.CommitTransaction(txn);
  oid_t database_id = data_table->GetDatabaseOid();
  oid_t table_id = data_table->GetOid();

  // Check: # transactions committed = 1, # table inserts = 10
  int64_t txn_commited = stats::BackendStatsContext::GetInstance()
                             ->GetDatabaseMetric(database_id)
                             ->GetTxnCommitted()
                             .GetCounter();
  int64_t inserts = stats::BackendStatsContext::GetInstance()
                        ->GetTableMetric(database_id, table_id)
                        ->GetTableAccess()
                        .GetInserts();
  EXPECT_EQ(1, txn_commited);
  EXPECT_EQ(num_rows, inserts);

  // Read every other tuple
  txn = txn_manager.BeginTransaction();
  for (int i = 0; i < num_rows; i += 2) {
    int result;
    TransactionTestsUtil::ExecuteRead(
        txn, data_table.get(), ExecutorTestsUtil::PopulatedValue(i, 0), result);
  }
  txn_manager.CommitTransaction(txn);

  // Check: # transactions committed = 2, # inserts = 10, # reads = 5
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  inserts = stats::BackendStatsContext::GetInstance()
                ->GetTableMetric(database_id, table_id)
                ->GetTableAccess()
                .GetInserts();
  int64_t reads = stats::BackendStatsContext::GetInstance()
                      ->GetTableMetric(database_id, table_id)
                      ->GetTableAccess()
                      .GetReads();
  EXPECT_EQ(2, txn_commited);
  EXPECT_EQ(num_rows, inserts);
  EXPECT_EQ(5, reads);

  // Do a single read and abort
  txn = txn_manager.BeginTransaction();
  int result;
  TransactionTestsUtil::ExecuteRead(
      txn, data_table.get(), ExecutorTestsUtil::PopulatedValue(0, 0), result);
  txn_manager.AbortTransaction(txn);

  // Check: # txns committed = 2, # txns aborted = 1, # reads = 6
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  int64_t txn_aborted = stats::BackendStatsContext::GetInstance()
                            ->GetDatabaseMetric(database_id)
                            ->GetTxnAborted()
                            .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              ->GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  EXPECT_EQ(2, txn_commited);
  EXPECT_EQ(1, txn_aborted);
  EXPECT_EQ(6, reads);

  // Read and update the first tuple
  txn = txn_manager.BeginTransaction();
  TransactionTestsUtil::ExecuteUpdate(txn, data_table.get(), 0, 2);
  txn_manager.CommitTransaction(txn);

  // Check: # txns committed = 3, # updates = 1, # reads = 7
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              ->GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  int64_t updates = stats::BackendStatsContext::GetInstance()
                        ->GetTableMetric(database_id, table_id)
                        ->GetTableAccess()
                        .GetUpdates();
  EXPECT_EQ(3, txn_commited);
  EXPECT_EQ(7, reads);
  EXPECT_EQ(1, updates);

  // Delete the 6th tuple and read the 1st tuple
  txn = txn_manager.BeginTransaction();
  TransactionTestsUtil::ExecuteDelete(txn, data_table.get(),
                                      ExecutorTestsUtil::PopulatedValue(5, 0));
  LOG_INFO("before read");
  TransactionTestsUtil::ExecuteRead(
      txn, data_table.get(), ExecutorTestsUtil::PopulatedValue(1, 0), result);
  txn_manager.CommitTransaction(txn);

  // Check: # txns committed = 4, # deletes = 1, # reads = 8
  txn_commited = stats::BackendStatsContext::GetInstance()
                     ->GetDatabaseMetric(database_id)
                     ->GetTxnCommitted()
                     .GetCounter();
  reads = stats::BackendStatsContext::GetInstance()
              ->GetTableMetric(database_id, table_id)
              ->GetTableAccess()
              .GetReads();
  int64_t deletes = stats::BackendStatsContext::GetInstance()
                        ->GetTableMetric(database_id, table_id)
                        ->GetTableAccess()
                        .GetDeletes();
  EXPECT_EQ(4, txn_commited);
  EXPECT_EQ(9, reads);
  EXPECT_EQ(1, deletes);

  aggregator.ShutdownAggregator();
}

TEST_F(StatsTest, PerQueryStatsTest) {
  int64_t aggregate_interval = 1000;
  // start executor pool
  ExecutorPoolHarness::GetInstance();
  LaunchAggregator(aggregate_interval);
  auto &aggregator = stats::StatsAggregator::GetInstance();

  // Create a table first
  auto catalog = catalog::Catalog::GetInstance();
  catalog->CreateDatabase("emp_db", nullptr);
  StatsTestsUtil::CreateTable();

  // Default database should include 4 metrics tables and the test table
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(CATALOG_DATABASE_NAME)
                ->GetTableCount(),
            6);
  LOG_INFO("Table created!");

  auto backend_context = stats::BackendStatsContext::GetInstance();

  // Inserting a tuple end-to-end
  auto statement = StatsTestsUtil::GetInsertStmt();
  // Initialize the query metric
  backend_context->InitQueryMetric(statement->GetQueryString(), DEFAULT_DB_ID);

  // Execute insert
  std::vector<common::Value *> params;
  std::vector<ResultType> result;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  bridge::peloton_status status = tcop::TrafficCop::ExchangeOperator(
      statement->GetPlanTree().get(), params, result, result_format);
  LOG_DEBUG("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple inserted!");

  // Now Updating end-to-end
  statement = std::move(StatsTestsUtil::GetUpdateStmt());
  // Initialize the query metric
  backend_context->InitQueryMetric(statement->GetQueryString(), DEFAULT_DB_ID);

  // Execute update
  params.clear();
  result.clear();
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = tcop::TrafficCop::ExchangeOperator(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_DEBUG("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple updated!");

  // Deleting end-to-end
  statement = std::move(StatsTestsUtil::GetDeleteStmt());
  // Initialize the query metric
  backend_context->InitQueryMetric(statement->GetQueryString(), DEFAULT_DB_ID);

  // Execute delete
  params.clear();
  result.clear();
  result_format =
      std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  status = tcop::TrafficCop::ExchangeOperator(statement->GetPlanTree().get(),
                                             params, result, result_format);
  LOG_DEBUG("Statement executed. Result: %d", status.m_result);
  LOG_INFO("Tuple deleted!");

  // Wait for aggregation to finish
  std::chrono::microseconds sleep_time(aggregate_interval * 2 * 1000);
  std::this_thread::sleep_for(sleep_time);
  aggregator.ShutdownAggregator();
  ForceFinalAggregation(aggregate_interval);

  EXPECT_EQ(aggregator.GetAggregatedStats().GetQueryCount(), 3);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName("emp_db", txn);
  txn_manager.CommitTransaction(txn);
}
}  // namespace stats
}  // namespace peloton
