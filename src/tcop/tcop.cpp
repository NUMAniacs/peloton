//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.cpp
//
// Identification: src/tcop/tcop.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "tcop/tcop.h"

#include "catalog/catalog.h"
#include "common/abstract_tuple.h"
#include "common/config.h"
#include "common/init.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/partition_macros.h"
#include "common/portal.h"
#include "common/type.h"
#include "common/types.h"
#include "common/partition_macros.h"
#include "common/thread_pool.h"

#include "executor/plan_executor.h"
#include "executor/abstract_task.h"

#include "expression/aggregate_expression.h"
#include "expression/expression_util.h"
#include "parser/parser.h"
#include "parser/statement_select.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "planner/parallel_seq_scan_plan.h"

#include <tuple>
#include <numa.h>

namespace peloton {
namespace tcop {

// global singleton
TrafficCop &TrafficCop::GetInstance(void) {
  static TrafficCop traffic_cop;
  return traffic_cop;
}

TrafficCop::TrafficCop() {
  // Nothing to do here !
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
}

TrafficCop::~TrafficCop() {
  // Nothing to do here !
}

Result TrafficCop::ExecuteStatement(
    const std::string &query, std::vector<ResultType> &result,
    std::vector<FieldInfoType> &tuple_descriptor, int &rows_changed,
    std::string &error_message) {
  LOG_TRACE("Received %s", query.c_str());

  // Prepare the statement
  std::string unnamed_statement = "unnamed";
  auto statement = PrepareStatement(unnamed_statement, query, error_message);

  if (statement.get() == nullptr) {
    return Result::RESULT_FAILURE;
  }

  // Then, execute the statement
  bool unnamed = true;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<common::Value> params;
  auto status =
      ExecuteStatement(statement, params, unnamed, nullptr, result_format,
                       result, rows_changed, error_message);

  if (status == Result::RESULT_SUCCESS) {
    LOG_TRACE("Execution succeeded!");
    tuple_descriptor = std::move(statement->GetTupleDescriptor());
  } else {
    LOG_TRACE("Execution failed!");
  }

  return status;
}

Result TrafficCop::ExecuteStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<common::Value> &params,
    UNUSED_ATTRIBUTE const bool unnamed,
    std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
    const std::vector<int> &result_format, std::vector<ResultType> &result,
    int &rows_changed, UNUSED_ATTRIBUTE std::string &error_message) {
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(statement,
                                                               param_stats);
  }

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetStatementName().c_str());
  try {
    bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
    auto status = ExchangeOperator(statement, params, result, result_format);
    LOG_TRACE("Statement executed. Result: %d", status.m_result);
    rows_changed = status.m_processed;
    return status.m_result;
  }
  catch (Exception &e) {
    error_message = e.what();
    return Result::RESULT_FAILURE;
  }
}

bridge::peloton_status TrafficCop::ExchangeOperator(
    const std::shared_ptr<Statement> &statement,
    const std::vector<common::Value> &params, std::vector<ResultType> &result,
    const std::vector<int> &result_format) {

  int num_tasks = 1;
  bridge::peloton_status final_status;

  bool print_time = false;

  if (statement->GetPlanTree().get() == nullptr) {
    return final_status;
  }

  auto start = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());

  std::vector<std::shared_ptr<executor::AbstractTask>> tasks;
  size_t num_partitions = PL_NUM_PARTITIONS();
  // The result of the logical tiles for all tasks
  std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
      new executor::LogicalTileLists());

  std::unordered_map<int, double> exec_histograms;

  auto plan_tree = statement->GetPlanTree().get();
  switch (plan_tree->GetPlanNodeType()) {
    // For insert queries, determine the tuple's partition before execute it
    case PLAN_NODE_TYPE_INSERT: {

      planner::InsertPlan *insert_plan =
          static_cast<planner::InsertPlan *>(plan_tree);

      auto target_table = insert_plan->GetTable();
      auto partition_cols = target_table->GetPartitionColumns();
      auto bulk_insert_count = insert_plan->GetBulkInsertCount();
      LOG_TRACE("bulk_insert_count = %d", (int)bulk_insert_count);

      // If we have partition key, then perform partition
      if (partition_cols != NO_PARTITION_COLUMN) {

        // One task is created for each partition now.
        // We should also avoid creating a task if no tuple is assigned to that
        // partition

        // Set default value for bitmap to false
        std::vector<std::vector<bool>> tuple_bitmaps(
            num_partitions, std::vector<bool>(bulk_insert_count, false));

        // Iterate through that tuples and compute the partition
        for (oid_t tuple_itr = 0; tuple_itr < bulk_insert_count; tuple_itr++) {
          auto tuple = insert_plan->GetTuple(tuple_itr);
          auto partition_val = tuple->GetValue(partition_cols);
          size_t partition_id = partition_val.Hash() % num_partitions;
          tuple_bitmaps[partition_id][tuple_itr] = true;
        }

        // Populate the tasks, one for each partition
        // TODO We could create more fine-grained tasks and exploit more
        // parallelism
        for (size_t task_id = 0; task_id < num_partitions; task_id++) {
          size_t partition = task_id;
          executor::InsertTask *insert_task = new executor::InsertTask(
              plan_tree, insert_plan->GetBulkInsertCount(), task_id, partition);
          insert_task->tuple_bitmap = tuple_bitmaps[partition];
          tasks.push_back(std::shared_ptr<executor::AbstractTask>(insert_task));
        }
      } else {
        // No partition specified, populate a task with random partition id
        size_t partition_id = rand() % num_partitions;
        std::shared_ptr<executor::AbstractTask> insert_task(
            new executor::InsertTask(plan_tree,
                                     insert_plan->GetBulkInsertCount(),
                                     INVALID_TASK_ID, partition_id));
        tasks.push_back(insert_task);
        LOG_DEBUG("Created task with random partition for insert stmt");
      }
      break;
    }

    case PlanNodeType::PLAN_NODE_TYPE_PARALLEL_SEQSCAN: {
      print_time = true;
      planner::ParallelSeqScanPlan *parallel_seq_scan_plan =
          static_cast<planner::ParallelSeqScanPlan *>(plan_tree);
      parallel_seq_scan_plan->GenerateTasks(tasks, result_tile_lists);
      break;
    }
    case PlanNodeType::PLAN_NODE_TYPE_SEQSCAN: {
      size_t num_tasks = 8;
      print_time = true;
      LOG_DEBUG("Creating statically partitioned tasks for SEQSCAN");
      for (size_t i=0; i<num_tasks; i++) {
        tasks.push_back(std::shared_ptr<executor::AbstractTask>(
            new executor::PartitionUnawareTask(plan_tree, result_tile_lists)));
      }
      break;
    }
    default: {
      // Populate default task for other queries
      LOG_DEBUG("Created partition unaware task for other stmt");
      tasks.push_back(std::shared_ptr<executor::AbstractTask>(
          new executor::PartitionUnawareTask(plan_tree, result_tile_lists)));
      break;
    }
  }

  LOG_DEBUG("Generated %ld tasks", tasks.size());
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // This happens for single statement queries in PG
  bool single_statement_txn = true;
  bool init_failure = false;

  auto txn = txn_manager.BeginTransaction();

  PL_ASSERT(txn);

  std::vector<std::shared_ptr<bridge::ExchangeParams>> exchg_params_list;
  final_status.m_processed = 0;

  bridge::BlockingWait wait;
  std::shared_ptr<executor::Trackable> trackable(
      new executor::Trackable(tasks.size()));

  for (size_t i=0; i<tasks.size(); i++) {
    // We create the callbacks only after we know the total number of tasks
    tasks[i]->Init(&wait, tasks.size());

    // in first pass make the exch params list
    std::shared_ptr<bridge::ExchangeParams> exchg_params(
        new bridge::ExchangeParams(txn, statement, params, tasks[i], result_format,
                                   init_failure, tasks.size(), i));
    exchg_params_list.push_back(exchg_params);

    switch (plan_tree->GetPlanNodeType()) {
      case PLAN_NODE_TYPE_PARALLEL_SEQSCAN:
      case PLAN_NODE_TYPE_INSERT: {
        // Use the partitioned_executor_pool for partition aware queries
        auto partition_aware_task =
            static_cast<executor::PartitionAwareTask *>(tasks[i].get());
        partitioned_executor_thread_pool.SubmitTask(
            partition_aware_task->partition_id,
            bridge::PlanExecutor::ExecutePlanLocal, &exchg_params->self);
        break;
      }
      default: {
        // Use normal executor pool for other queries
        partitioned_executor_thread_pool.SubmitTaskRandom(
            bridge::PlanExecutor::ExecutePlanLocal, &exchg_params->self);
        break;
      }
    }
  }

  std::map<int, std::pair<double, double>> access_histograms;

  // wait for tasks to complete
  wait.WaitForCompletion();
  for (size_t i=0; i<exchg_params_list.size(); i++) {
    // wait for executor thread to return result
    auto temp_status = exchg_params_list[i]->p_status;
    init_failure &= exchg_params_list[i]->init_failure;
    if (init_failure == false) {
      // proceed only if none of the threads so far have failed
      final_status.m_processed += temp_status.m_processed;

      // persist failure states across iterations
      if (final_status.m_result == peloton::Result::RESULT_SUCCESS)
        final_status.m_result = temp_status.m_result;
      final_status.m_result_slots = nullptr;

      result.insert(result.end(), exchg_params_list[i]->result.begin(),
                    exchg_params_list[i]->result.end());
    }

    if (plan_tree->GetPlanNodeType() == PLAN_NODE_TYPE_PARALLEL_SEQSCAN) {
      exec_histograms[exchg_params_list[i]->cpu_id] += exchg_params_list[i]->exec_time;
    }

    if (plan_tree->GetPlanNodeType() == PLAN_NODE_TYPE_SEQSCAN) {
      auto task = static_cast<executor::PartitionUnawareTask*>(tasks[i].get());
      auto current = access_histograms[task->cpu_id];
      access_histograms[task->cpu_id].first = current.first + task->non_local_access_count;
      access_histograms[task->cpu_id].second = current.second + task->total_access_count;
    }
  }

  LOG_TRACE("About to commit: single stmt: %d, init_failure: %d, status: %d",
            single_statement_txn, init_failure, txn->GetResult());

  // should we commit or abort ?
  if (single_statement_txn == true || init_failure == true) {
    auto status = txn->GetResult();
    switch (status) {
      case Result::RESULT_SUCCESS:
        // Commit
        LOG_TRACE("Commit Transaction");
        final_status.m_result = txn_manager.CommitTransaction(txn);
        break;

      case Result::RESULT_FAILURE:
      default:
        // Abort
        LOG_TRACE("Abort Transaction");
        final_status.m_result = txn_manager.AbortTransaction(txn);
    }
  }

  auto end = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());

  if (print_time == true) {
    std::stringstream histogram;
    for (auto itr=exec_histograms.begin(); itr!=exec_histograms.end(); itr++) {
      histogram << itr->first << " " << itr->second << std::endl;
    }
    LOG_ERROR("%s%f", histogram.str().c_str(), (end-start)/1000);
  }

  if (plan_tree->GetPlanNodeType() == PLAN_NODE_TYPE_SEQSCAN) {
    std::stringstream histogram;
    for (size_t i=0; i<std::thread::hardware_concurrency(); i++) {
      double frac = 0;
      if (access_histograms[i].second != 0) {
        frac = access_histograms[i].first/access_histograms[i].second;
      }
      histogram << frac << std::endl;
    }
    LOG_ERROR("Access Histogram:\n%s", histogram.str().c_str());
  }

  return final_status;
}

std::shared_ptr<Statement> TrafficCop::PrepareStatement(
    const std::string &statement_name, const std::string &query_string,
    UNUSED_ATTRIBUTE std::string &error_message) {
  std::shared_ptr<Statement> statement;

  LOG_DEBUG("Prepare Statement name: %s", statement_name.c_str());
  LOG_DEBUG("Prepare Statement query: %s", query_string.c_str());

  statement.reset(new Statement(statement_name, query_string));
  try {
    auto &peloton_parser = parser::Parser::GetInstance();
    auto sql_stmt = peloton_parser.BuildParseTree(query_string);
    if (sql_stmt->is_valid == false) {
      throw ParserException("Error parsing SQL statement");
    }
    statement->SetPlanTree(
        optimizer::SimpleOptimizer::BuildPelotonPlanTree(sql_stmt));

    for (auto stmt : sql_stmt->GetStatements()) {
      if (stmt->GetType() == STATEMENT_TYPE_SELECT) {
        auto tuple_descriptor = GenerateTupleDescriptor(stmt);
        statement->SetTupleDescriptor(tuple_descriptor);
      }
      break;
    }

    bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
    LOG_DEBUG("Statement Prepared!");
    return std::move(statement);
  }
  catch (Exception &e) {
    error_message = e.what();
    return nullptr;
  }
}

std::vector<FieldInfoType> TrafficCop::GenerateTupleDescriptor(
    parser::SQLStatement *stmt) {
  std::vector<FieldInfoType> tuple_descriptor;
  if (stmt->GetType() != STATEMENT_TYPE_SELECT) return tuple_descriptor;
  auto select_stmt = (parser::SelectStatement *)stmt;

  // TODO: this is a hack which I don't have time to fix now
  // but it replaces a worse hack that was here before
  // What should happen here is that plan nodes should store
  // the schema of their expected results and here we should just read
  // it and put it in the tuple descriptor

  // Get the columns information and set up
  // the columns description for the returned results
  // Set up the table
  storage::DataTable *target_table = nullptr;

  // Check if query only has one Table
  // Example : SELECT * FROM A;
  if (select_stmt->from_table->list == NULL) {
    target_table = static_cast<storage::DataTable *>(
        catalog::Catalog::GetInstance()->GetTableWithName(
            select_stmt->from_table->GetDatabaseName(),
            select_stmt->from_table->GetTableName()));
  }

  // Query has multiple tables
  // Example: SELECT COUNT(ID) FROM A,B <Condition>
  // For now we only pick the first table in the list
  // FIX: Better handle for queries with multiple tables
  else {
    for (auto table : *select_stmt->from_table->list) {
      target_table = static_cast<storage::DataTable *>(
          catalog::Catalog::GetInstance()->GetTableWithName(
              table->GetDatabaseName(), table->GetTableName()));
      break;
    }
  }

  // Get the columns of the table
  auto &table_columns = target_table->GetSchema()->GetColumns();

  int count = 0;
  for (auto expr : *select_stmt->select_list) {
    count++;
    if (expr->GetExpressionType() == EXPRESSION_TYPE_STAR) {
      for (auto column : table_columns) {
        tuple_descriptor.push_back(
            GetColumnFieldForValueType(column.column_name, column.column_type));
      }
    } else {
      std::string col_name;
      if (expr->alias.empty()) {
        col_name = expr->expr_name_.empty()
                       ? std::string("expr") + std::to_string(count)
                       : expr->expr_name_;
      } else {
        col_name = expr->alias;
      }
      tuple_descriptor.push_back(
          GetColumnFieldForValueType(col_name, expr->GetValueType()));
    }
  }
  return tuple_descriptor;
}

FieldInfoType TrafficCop::GetColumnFieldForValueType(
    std::string column_name, common::Type::TypeId column_type) {
  switch (column_type) {
    case common::Type::INTEGER:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_INTEGER, 4);
    case common::Type::DECIMAL:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_DOUBLE, 8);
    case common::Type::VARCHAR:
    case common::Type::VARBINARY:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TEXT, 255);
    case common::Type::TIMESTAMP:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TIMESTAMPS, 64);
    default:
      // Type not Identified
      LOG_ERROR("Unrecognized column type '%s' [%d] for column '%s'",
                TypeIdToString(column_type).c_str(), column_type,
                column_name.c_str());
  }
  // return String
  return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TEXT, 255);
}

FieldInfoType TrafficCop::GetColumnFieldForAggregates(
    std::string name, ExpressionType expr_type) {
  // For now we only return INT for (MAX , MIN)
  // TODO: Check if column type is DOUBLE and return it for (MAX. MIN)

  // Check the expression type and return the corresponding description
  if (expr_type == EXPRESSION_TYPE_AGGREGATE_MAX ||
      expr_type == EXPRESSION_TYPE_AGGREGATE_MIN ||
      expr_type == EXPRESSION_TYPE_AGGREGATE_COUNT) {
    return std::make_tuple(name, POSTGRES_VALUE_TYPE_INTEGER, 4);
  }

  // Return double if function is AVERAGE
  if (expr_type == EXPRESSION_TYPE_AGGREGATE_AVG) {
    return std::make_tuple(name, POSTGRES_VALUE_TYPE_DOUBLE, 8);
  }

  if (expr_type == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) {
    return std::make_tuple("COUNT(*)", POSTGRES_VALUE_TYPE_INTEGER, 4);
  }

  return std::make_tuple(name, POSTGRES_VALUE_TYPE_TEXT, 255);
}
}  // End tcop namespace
}  // End peloton namespace
