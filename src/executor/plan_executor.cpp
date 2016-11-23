//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.cpp
//
// Identification: src/executor/plan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/executors.h"
#include "executor/plan_executor.h"
#include "optimizer/util.h"
#include "storage/tuple_iterator.h"
#include "planner/insert_plan.h"

namespace peloton {
namespace bridge {

/*
 * Added for network invoking efficiently
 */
executor::ExecutorContext *BuildExecutorContext(
    const std::vector<common::Value> &params, concurrency::Transaction *txn);

executor::AbstractExecutor *BuildExecutorTree(
    executor::AbstractExecutor *root, const planner::AbstractPlan *plan,
    executor::ExecutorContext *executor_context);

void CleanExecutorTree(executor::AbstractExecutor *root);

void HashCallback::TaskComplete(
    UNUSED_ATTRIBUTE std::shared_ptr<executor::AbstractTask> task) {
  // Increment the number of tasks completed
  int task_num = tasks_complete_.fetch_add(1);
  // This is the last task
  if (task_num == total_tasks_ - 1) {
    // Get the total number of partition
    size_t num_partitions = PL_NUM_PARTITIONS();

    // Group the results based on partitions
    executor::LogicalTileLists partitioned_result_tile_lists(num_partitions);
    for (auto &result_tile_list : *(task->result_tile_lists)) {
      for (auto &result_tile : result_tile_list) {
        size_t partition = result_tile->GetPartition();
        partitioned_result_tile_lists[partition]
            .push_back(std::move(result_tile));
      }
    }
    // Populate tasks for each partition and re-chunk the tiles
    std::shared_ptr<executor::LogicalTileLists> result_tile_lists(
        new executor::LogicalTileLists());
    for (size_t partition = 0; partition < num_partitions; partition++) {
      executor::LogicalTileList next_result_tile_list;

      for (auto &result_tile_list : partitioned_result_tile_lists) {
        // TODO we should re-chunk based on number of tuples?
        for (auto &result_tile : result_tile_list) {
          next_result_tile_list.push_back(std::move(result_tile));
          // Reached the limit of each chunk
          if (next_result_tile_list.size() >= TASK_TILEGROUP_COUNT) {
            result_tile_lists->push_back(std::move(next_result_tile_list));
            next_result_tile_list = executor::LogicalTileList();
          }
        }
        // Check the remaining result tiles
        if (next_result_tile_list.size() > 0) {
          result_tile_lists->push_back(std::move(next_result_tile_list));
        }
      }
    }

    size_t num_tasks = result_tile_lists->size();
    // A list of all tasks to execute
    std::vector<std::shared_ptr<executor::AbstractTask>> tasks;
    for (size_t task_id = 0; task_id < num_tasks; task_id++) {
      // Construct a hash task
      std::shared_ptr<executor::AbstractTask> next_task(new executor::HashTask(
          hash_executor_->GetRawNode(), hash_executor_, task_id,
          result_tile_lists->at(task_id).at(0)->GetPartition(),
          result_tile_lists));
      // next_task->Init(next_callback, num_tasks);
      tasks.push_back(next_task);
    }

    // TODO Launch the new tasks?
    // for (auto &task : tasks) {

    //}
  }
}

/**
 * @brief Build a executor tree and execute it.
 * Use std::vector<common::Value> as params to make it more elegant for
 * networking
 * Before ExecutePlan, a node first receives value list, so we should pass
 * value list directly rather than passing Postgres's ParamListInfo
 * @return status of execution.
 */
void PlanExecutor::ExecutePlanLocal(ExchangeParams **exchg_params_arg) {
  peloton_status p_status;
  ExchangeParams *exchg_params = *exchg_params_arg;

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        exchg_params->statement->GetQueryString(), DEFAULT_DB_ID);
  }

  LOG_TRACE("PlanExecutor Start ");

  bool status;

  LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());
  LOG_TRACE("Building the executor tree");

  // Use const std::vector<common::Value> &params to make it more elegant for
  // network
  std::unique_ptr<executor::ExecutorContext> executor_context(
      BuildExecutorContext(exchg_params->params, exchg_params->txn));

  PL_ASSERT(exchg_params->task != nullptr);
  executor_context->SetTask(exchg_params->task);

  // Build the executor tree
  std::unique_ptr<executor::AbstractExecutor> executor_tree(
      BuildExecutorTree(nullptr, exchg_params->statement->GetPlanTree().get(),
                        executor_context.get()));

  LOG_TRACE("Initializing the executor tree");

  // Initialize the executor tree
  status = executor_tree->Init();

  // Abort and cleanup
  if (status == false) {
    exchg_params->init_failure = true;
    exchg_params->txn->SetResult(Result::RESULT_FAILURE);
  } else {
    LOG_TRACE("Running the executor tree");
    exchg_params->result.clear();

    // Execute the tree until we get result tiles from root node
    while (status == true) {
      status = executor_tree->Execute();

      // FIXME We should push the logical tile to the result field in the tasks
      // instead of being processed here immediately)
      std::unique_ptr<executor::LogicalTile> logical_tile(
          executor_tree->GetOutput());
      // Some executors don't return logical tiles (e.g., Update).
      if (logical_tile.get() != nullptr) {
        LOG_TRACE("Final Answer: %s",
                  logical_tile->GetInfo().c_str());  // Printing the answers
        std::unique_ptr<catalog::Schema> output_schema(
            logical_tile->GetPhysicalSchema());  // Physical schema of the tile
        std::vector<std::vector<std::string>> answer_tuples;
        answer_tuples = std::move(
            logical_tile->GetAllValuesAsStrings(exchg_params->result_format));

        // Construct the returned results
        for (auto &tuple : answer_tuples) {
          unsigned int col_index = 0;
          auto &schema_cols = output_schema->GetColumns();
          for (auto &column : schema_cols) {
            auto column_name = column.GetName();
            auto res = ResultType();
            PlanExecutor::copyFromTo(column_name, res.first);
            LOG_TRACE("column name: %s", column_name.c_str());
            PlanExecutor::copyFromTo(tuple[col_index++], res.second);
            if (tuple[col_index - 1].c_str() != nullptr) {
              LOG_TRACE("column content: %s", tuple[col_index - 1].c_str());
            }
            exchg_params->result.push_back(res);
>>>>>>> ad036ba... Applied exchange_txn as patch over master
          }
        }
      }
    }
    // Set the result
    p_status.m_processed = executor_context->num_processed;
    p_status.m_result_slots = nullptr;
  }

  // clean up executor tree
  CleanExecutorTree(executor_tree.get());

  exchg_params->TaskComplete(p_status);
}

/**
 * @brief Build a executor tree and execute it.
 * Use std::vector<common::Value> as params to make it more elegant for
 * networking
 * Before ExecutePlan, a node first receives value list, so we should pass
 * value list directly rather than passing Postgres's ParamListInfo
 * @return number of executed tuples and logical_tile_list
 */
void PlanExecutor::ExecutePlanRemote(
    const planner::AbstractPlan *plan, const std::vector<common::Value> &params,
    std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list,
    boost::promise<int> &p) {
  if (plan == nullptr) return p.set_value(-1);

  LOG_TRACE("PlanExecutor Start ");

  bool status;
  bool init_failure = false;
  bool single_statement_txn = false;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = peloton::concurrency::current_txn;

  // This happens for single statement queries in PG
  // if (txn == nullptr) {
  single_statement_txn = true;
  auto txn = txn_manager.BeginTransaction();
  // }
  PL_ASSERT(txn);

  LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());
  LOG_TRACE("Building the executor tree");

  // Use const std::vector<common::Value> &params to make it more elegant for
  // network
  std::unique_ptr<executor::ExecutorContext> executor_context(
      BuildExecutorContext(params, txn));

  // Build the executor tree
  std::unique_ptr<executor::AbstractExecutor> executor_tree(
      BuildExecutorTree(nullptr, plan, executor_context.get()));

  LOG_TRACE("Initializing the executor tree");

  // Initialize the executor tree
  status = executor_tree->Init();

  // Abort and cleanup
  if (status == false) {
    init_failure = true;
    txn->SetResult(Result::RESULT_FAILURE);
    goto cleanup;
  }

  LOG_TRACE("Running the executor tree");

  // Execute the tree until we get result tiles from root node
  for (;;) {
    status = executor_tree->Execute();

    // Stop
    if (status == false) {
      break;
    }

    std::unique_ptr<executor::LogicalTile> logical_tile(
        executor_tree->GetOutput());

    // Some executors don't return logical tiles (e.g., Update).
    if (logical_tile.get() == nullptr) {
      continue;
    }

    logical_tile_list.push_back(std::move(logical_tile));
  }

// final cleanup
cleanup:

  LOG_TRACE("About to commit: single stmt: %d, init_failure: %d, status: %d",
            single_statement_txn, init_failure, txn->GetResult());

  // clean up executor tree
  CleanExecutorTree(executor_tree.get());

  // should we commit or abort ?
  if (single_statement_txn == true || init_failure == true) {
    auto status = txn->GetResult();
    switch (status) {
      case Result::RESULT_SUCCESS:
        // Commit
        return p.set_value(executor_context->num_processed);

        break;

      case Result::RESULT_FAILURE:
      default:
        // Abort
        return p.set_value(-1);
    }
  }
  return p.set_value(executor_context->num_processed);
}

/**
 * @brief Pretty print the plan tree.
 * @param The plan tree
 * @return none.
 */
void PlanExecutor::PrintPlan(const planner::AbstractPlan *plan,
                             std::string prefix) {
  if (plan == nullptr) {
    LOG_TRACE("Plan is null");
    return;
  }

  prefix += "  ";
  LOG_TRACE("Plan Type: %s",
            PlanNodeTypeToString(plan->GetPlanNodeType()).c_str());
  LOG_TRACE("%s->Plan Info :: %s ", prefix.c_str(), plan->GetInfo().c_str());

  auto &children = plan->GetChildren();

  LOG_TRACE("Number of children in plan: %d ", (int)children.size());

  for (auto &child : children) {
    PrintPlan(child.get(), prefix);
  }
}

/**
 * @brief Build Executor Context
 */
executor::ExecutorContext *BuildExecutorContext(
    const std::vector<common::Value> &params, concurrency::Transaction *txn) {
  return new executor::ExecutorContext(txn, params);
}

/**
 * @brief Build the executor tree.
 * @param The current executor tree
 * @param The plan tree
 * @param Transation context
 * @return The updated executor tree.
 */
executor::AbstractExecutor *BuildExecutorTree(
    executor::AbstractExecutor *root, const planner::AbstractPlan *plan,
    executor::ExecutorContext *executor_context) {
  // Base case
  if (plan == nullptr) return root;

  executor::AbstractExecutor *child_executor = nullptr;

  auto plan_node_type = plan->GetPlanNodeType();
  switch (plan_node_type) {
    case PLAN_NODE_TYPE_INVALID:
      LOG_ERROR("Invalid plan node type ");
      break;

    case PLAN_NODE_TYPE_SEQSCAN:
      LOG_TRACE("Adding Sequential Scan Executor");
      child_executor = new executor::SeqScanExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_PARALLEL_SEQSCAN:
      LOG_TRACE("Adding Parallel Sequential Scan Executor");
      child_executor =
          new executor::ParallelSeqScanExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_INDEXSCAN:
      LOG_TRACE("Adding Index Scan Executor");
      child_executor = new executor::IndexScanExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_INSERT: {
      LOG_TRACE("Adding Insert Executor");
      child_executor = new executor::InsertExecutor(executor_context);
      break;
    }
    case PLAN_NODE_TYPE_DELETE:
      LOG_TRACE("Adding Delete Executor");
      child_executor = new executor::DeleteExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_UPDATE:
      LOG_TRACE("Adding Update Executor");
      child_executor = new executor::UpdateExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_LIMIT:
      LOG_TRACE("Adding Limit Executor");
      child_executor = new executor::LimitExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_NESTLOOP:
      LOG_TRACE("Adding Nested Loop Joing Executor");
      child_executor =
          new executor::NestedLoopJoinExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_MERGEJOIN:
      LOG_TRACE("Adding Merge Join Executor");
      child_executor = new executor::MergeJoinExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_HASH:
      LOG_TRACE("Adding Hash Executor");
      child_executor = new executor::HashExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_HASHJOIN:
      LOG_TRACE("Adding Hash Join Executor");
      child_executor = new executor::HashJoinExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_PROJECTION:
      LOG_TRACE("Adding Projection Executor");
      child_executor = new executor::ProjectionExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_MATERIALIZE:
      LOG_TRACE("Adding Materialization Executor");
      child_executor =
          new executor::MaterializationExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_AGGREGATE_V2:
      LOG_TRACE("Adding Aggregate Executor");
      child_executor = new executor::AggregateExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_ORDERBY:
      LOG_TRACE("Adding Order By Executor");
      child_executor = new executor::OrderByExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_DROP:
      LOG_TRACE("Adding Drop Executor");
      child_executor = new executor::DropExecutor(plan, executor_context);
      break;

    case PLAN_NODE_TYPE_CREATE:
      LOG_TRACE("Adding Create Executor");
      child_executor = new executor::CreateExecutor(plan, executor_context);
      break;
    case PLAN_NODE_TYPE_COPY:
      LOG_TRACE("Adding Copy Executer");
      child_executor = new executor::CopyExecutor(plan, executor_context);
      break;

    default:
      LOG_ERROR("Unsupported plan node type : %d ", plan_node_type);
      break;
  }

  // Base case
  if (child_executor != nullptr) {
    if (root != nullptr)
      root->AddChild(child_executor);
    else
      root = child_executor;
  }

  // Recurse
  auto &children = plan->GetChildren();
  for (auto &child : children) {
    child_executor =
        BuildExecutorTree(child_executor, child.get(), executor_context);
  }

  return root;
}

/**
 * @brief Clean up the executor tree.
 * @param The current executor tree
 * @return none.
 */
void CleanExecutorTree(executor::AbstractExecutor *root) {
  if (root == nullptr) return;

  // Recurse
  auto children = root->GetChildren();
  for (auto child : children) {
    CleanExecutorTree(child);
    delete child;
  }
}

}  // namespace bridge
}  // namespace peloton
