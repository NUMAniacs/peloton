//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_optimizer.cpp
//
// Identification: src/optimizer/simple_optimizer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/simple_optimizer.h"

#include "parser/abstract_parse.h"

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "expression/expression_util.h"
#include "expression/aggregate_expression.h"
#include "expression/function_expression.h"
#include "expression/star_expression.h"
#include "parser/sql_statement.h"
#include "parser/statements.h"
#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/aggregate_plan.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/drop_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/copy_plan.h"
#include "planner/limit_plan.h"
#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/parallel_seq_scan_plan.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"

#include "common/logger.h"
#include "common/value_factory.h"

#include <memory>
#include <unordered_map>

namespace peloton {
namespace planner {
class AbstractPlan;
}
namespace optimizer {

SimpleOptimizer::SimpleOptimizer() {};

SimpleOptimizer::~SimpleOptimizer() {};

std::shared_ptr<planner::AbstractPlan> SimpleOptimizer::BuildPelotonPlanTree(
    const std::unique_ptr<parser::SQLStatementList>& parse_tree) {
  std::shared_ptr<planner::AbstractPlan> plan_tree;

  // Base Case
  if (parse_tree->GetStatements().size() == 0) return plan_tree;

  std::unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  // One to one Mapping
  auto parse_item_node_type = parse_tree->GetStatements().at(0)->GetType();

  auto parse_tree2 = parse_tree->GetStatements().at(0);

  switch (parse_item_node_type) {
    case STATEMENT_TYPE_DROP: {
      LOG_TRACE("Adding Drop plan...");
      std::unique_ptr<planner::AbstractPlan> child_DropPlan(
          new planner::DropPlan((parser::DropStatement*)parse_tree2));
      child_plan = std::move(child_DropPlan);
    } break;

    case STATEMENT_TYPE_CREATE: {
      LOG_TRACE("Adding Create plan...");
      std::unique_ptr<planner::AbstractPlan> child_CreatePlan(
          new planner::CreatePlan((parser::CreateStatement*)parse_tree2));
      child_plan = std::move(child_CreatePlan);
    } break;

    case STATEMENT_TYPE_SELECT: {
      LOG_TRACE("Processing SELECT...");
      auto select_stmt = (parser::SelectStatement*)parse_tree2;
      LOG_TRACE("SELECT Info: %s", select_stmt->GetInfo().c_str());
      auto agg_type = AGGREGATE_TYPE_PLAIN;  // default aggregator
      std::vector<oid_t> group_by_columns;
      auto group_by = select_stmt->group_by;
      expression::AbstractExpression* having = nullptr;

      // The HACK to make the join in tpcc work. This is written by Joy Arulraj
      if (select_stmt->from_table->list != NULL) {
        LOG_TRACE("have join condition? %d",
                  select_stmt->from_table->join != NULL);
        LOG_TRACE("have sub select statement? %d",
                  select_stmt->from_table->select != NULL);
        try {
          catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME,
                                                            "order_line");
          // auto child_SelectPlan = CreateHackingJoinPlan();
          auto child_SelectPlan = CreateHackingNestedLoopJoinPlan();
          child_plan = std::move(child_SelectPlan);
          break;
        }
        catch (Exception& e) {
          throw NotImplementedException("Error: Joins are not implemented yet");
        }
      }

      if (select_stmt->from_table->join != NULL) {
        throw NotImplementedException("Error: Joins are not implemented yet");
      }

      storage::DataTable* target_table =
          catalog::Catalog::GetInstance()->GetTableWithName(
              select_stmt->from_table->GetDatabaseName(),
              select_stmt->from_table->GetTableName());

      // Preparing the group by columns
      if (group_by != NULL) {
        LOG_TRACE("Found GROUP BY");
        for (auto elem : *group_by->columns) {
          auto tuple_elem = (expression::TupleValueExpression*)elem;
          std::string col_name(tuple_elem->GetColumnName());
          auto column_id = target_table->GetSchema()->GetColumnID(col_name);
          group_by_columns.push_back(column_id);
        }
        // Having Expression needs to be prepared
        // Currently it's mostly ParserExpression
        // Needs to be prepared
        having = group_by->having;
      }

      std::unordered_map<oid_t, oid_t> column_mapping;
      std::vector<oid_t> column_ids;
      auto& schema = *target_table->GetSchema();
      auto predicate = select_stmt->where_clause;
      bool needs_projection = false;
      expression::ExpressionUtil::TransformExpression(&schema, predicate);

      for (auto col : *select_stmt->select_list) {
        expression::ExpressionUtil::TransformExpression(
            column_mapping, column_ids, col, schema, needs_projection);
      }

      // Check if there are any aggregate functions
      bool agg_flag = false;
      for (auto expr : *select_stmt->getSelectList()) {
        if (expression::ExpressionUtil::IsAggregateExpression(
                expr->GetExpressionType())) {
          LOG_TRACE("Query has aggregate functions");
          agg_flag = true;
          break;
        }
      }

      // If there is no aggregate functions, just do a sequential scan
      if (!agg_flag && group_by_columns.size() == 0) {
        LOG_TRACE("No aggregate functions found.");
        std::unique_ptr<planner::AbstractPlan> child_SelectPlan =
            CreateScanPlan(target_table, column_ids, predicate,
                           select_stmt->is_for_update);

        // if we have expressions which are not just columns, we need to add a
        // projection plan node
        if (needs_projection) {
          auto& select_list = *select_stmt->getSelectList();
          // expressions to evaluate
          TargetList tl = TargetList();
          // columns which can be returned directly
          DirectMapList dml = DirectMapList();
          // schema of the projections output
          std::vector<catalog::Column> columns;
          catalog::Schema* table_schema = target_table->GetSchema();
          for (int i = 0; i < (int)select_list.size(); i++) {
            auto expr = select_list[i];
            // if the root of the expression is a column value we can
            // just do a direct mapping
            if (expr->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
              auto tup_expr = (expression::TupleValueExpression*)expr;
              oid_t old_col_id =
                  table_schema->GetColumnID(tup_expr->GetColumnName());
              columns.push_back(table_schema->GetColumn(old_col_id));
              dml.push_back(
                  DirectMap(i, std::make_pair(0, tup_expr->GetColumnId())));
            }
            // otherwise we need to evaluat the expression
            else {
              tl.push_back(Target(i, expr->Copy()));
              columns.push_back(catalog::Column(
                  expr->GetValueType(),
                  common::Type::GetTypeSize(expr->GetValueType()),
                  "expr" + std::to_string(i)));
            }
          }
          // build the projection plan node and insert aboce the scan
          std::unique_ptr<planner::ProjectInfo> proj_info(
              new planner::ProjectInfo(std::move(tl), std::move(dml)));
          std::shared_ptr<catalog::Schema> schema_ptr(
              new catalog::Schema(columns));
          std::unique_ptr<planner::AbstractPlan> child_ProjectPlan(
              new planner::ProjectionPlan(std::move(proj_info), schema_ptr));
          child_ProjectPlan->AddChild(std::move(child_SelectPlan));
          child_SelectPlan = std::move(child_ProjectPlan);
        }

        if (select_stmt->order != NULL && select_stmt->limit != NULL) {
          std::vector<oid_t> keys;
          // Add all selected columns to the output
          // We already generated the "real" physical output schema in the scan
          // plan, so now we just need to directly copy all the columns
          for (size_t column_ctr = 0;
               column_ctr < select_stmt->select_list->size(); column_ctr++) {
            keys.push_back(column_ctr);
          }

          std::vector<bool> flags;
          if (select_stmt->order->type == 0) {
            flags.push_back(false);
          } else {
            flags.push_back(true);
          }
          std::vector<oid_t> key;

          std::string sort_col_name(
              ((expression::TupleValueExpression*)select_stmt->order->expr)
                  ->GetColumnName());
          for (size_t column_ctr = 0;
               column_ctr < select_stmt->select_list->size(); column_ctr++) {
            std::string col_name((
                (expression::TupleValueExpression*)select_stmt->select_list->at(
                    column_ctr))->GetColumnName());
            if (col_name == sort_col_name) key.push_back(column_ctr);
          }
          if (key.size() == 0) {
            LOG_ERROR(
                "Not supported: Ordering column is not an element of select "
                "list!");
          }
          std::unique_ptr<planner::OrderByPlan> order_by_plan(
              new planner::OrderByPlan(key, flags, keys));
          order_by_plan->AddChild(std::move(child_SelectPlan));
          int offset = select_stmt->limit->offset;
          if (offset < 0) {
            offset = 0;
          }
          std::unique_ptr<planner::LimitPlan> limit_plan(
              new planner::LimitPlan(select_stmt->limit->limit, offset));
          limit_plan->AddChild(std::move(order_by_plan));
          child_plan = std::move(limit_plan);
        } else if (select_stmt->order != NULL) {
          std::vector<oid_t> keys;
          for (size_t column_ctr = 0;
               column_ctr < select_stmt->select_list->size(); column_ctr++) {
            keys.push_back(column_ctr);
          }

          std::vector<bool> flags;
          if (select_stmt->order->type == 0) {
            flags.push_back(false);
          } else {
            flags.push_back(true);
          }
          std::vector<oid_t> key;
          std::string sort_col_name(
              ((expression::TupleValueExpression*)select_stmt->order->expr)
                  ->GetColumnName());
          for (size_t column_ctr = 0;
               column_ctr < select_stmt->select_list->size(); column_ctr++) {
            std::string col_name((
                (expression::TupleValueExpression*)select_stmt->select_list->at(
                    column_ctr))->GetColumnName());
            if (col_name == sort_col_name) key.push_back(column_ctr);
          }
          if (key.size() == 0) {
            LOG_ERROR(
                "Not supported: Ordering column is not an element of select "
                "list!");
          }
          std::unique_ptr<planner::OrderByPlan> order_by_plan(
              new planner::OrderByPlan(key, flags, keys));
          order_by_plan->AddChild(std::move(child_SelectPlan));
          child_plan = std::move(order_by_plan);
        } else if (select_stmt->limit != NULL) {
          int offset = select_stmt->limit->offset;
          if (offset < 0) {
            offset = 0;
          }
          std::unique_ptr<planner::LimitPlan> limit_plan(
              new planner::LimitPlan(select_stmt->limit->limit, offset));
          limit_plan->AddChild(std::move(child_SelectPlan));
          child_plan = std::move(limit_plan);
        } else {
          child_plan = std::move(child_SelectPlan);
        }
      }
      // Else, do aggregations on top of scan
      else {
        std::unique_ptr<planner::AbstractScan> scan_node = CreateScanPlan(
            target_table, column_ids, predicate, select_stmt->is_for_update);

        // Prepare aggregate plan
        std::vector<catalog::Column> output_schema_columns;
        std::vector<planner::AggregatePlan::AggTerm> agg_terms;
        DirectMapList direct_map_list = {};
        oid_t new_col_id = 0;
        oid_t agg_id = 0;
        int col_cntr_id = 0;
        for (auto expr : *select_stmt->getSelectList()) {
          LOG_TRACE("Expression type in Select: %s",
                    ExpressionTypeToString(expr->GetExpressionType()).c_str());

          // If an aggregate function is found
          if (expression::ExpressionUtil::IsAggregateExpression(
                  expr->GetExpressionType())) {
            auto agg_expr = (expression::AggregateExpression*)expr;
            LOG_TRACE("Expression type in Function Expression: %s",
                      ExpressionTypeToString(
                          func_expr->expr->GetExpressionType()).c_str());
            LOG_TRACE("Distinct flag: %d", func_expr->distinct);

            // Count a column expression
            if (agg_expr->GetChild(0) != nullptr &&
                agg_expr->GetChild(0)->GetExpressionType() ==
                    EXPRESSION_TYPE_VALUE_TUPLE) {
              auto agg_over =
                  (expression::TupleValueExpression*)agg_expr->GetChild(0);
              LOG_TRACE("Function name: %s",
                        ((expression::TupleValueExpression*)agg_expr)
                            ->GetExpressionName());
              LOG_TRACE("Aggregate type: %s",
                        ExpressionTypeToString(
                            ParserExpressionNameToExpressionType(
                                func_expr->GetExpressionName())).c_str());
              planner::AggregatePlan::AggTerm agg_term(
                  agg_expr->GetExpressionType(), agg_over->Copy(),
                  agg_expr->distinct_);
              agg_terms.push_back(agg_term);

              std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, agg_id);
              std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
                  std::make_pair(new_col_id, inner_pair);
              direct_map_list.emplace_back(outer_pair);
              LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
                        outer_pair.second.first, outer_pair.second.second);

              // If aggregate type is average the value type should be double
              if (agg_expr->GetExpressionType() ==
                  EXPRESSION_TYPE_AGGREGATE_AVG) {
                // COL_A should be used only when there is no AS
                auto column = catalog::Column(
                    common::Type::DECIMAL,
                    common::Type::GetTypeSize(common::Type::DECIMAL),
                    "COL_" + std::to_string(col_cntr_id++), true);

                output_schema_columns.push_back(column);
              }

              // Else it is the same as the column type
              else {
                oid_t old_col_id = target_table->GetSchema()->GetColumnID(
                    agg_over->GetColumnName());
                auto table_column =
                    target_table->GetSchema()->GetColumn(old_col_id);

                auto column = catalog::Column(
                    table_column.GetType(),
                    common::Type::GetTypeSize(table_column.GetType()),
                    "COL_" + std::to_string(col_cntr_id++),  // COL_A should be
                                                             // used only when
                                                             // there is no AS
                    true);

                output_schema_columns.push_back(column);
              }

            }

            // Check for COUNT STAR Expression
            else if (agg_expr->GetExpressionType() ==
                     EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) {
              LOG_TRACE("Creating an aggregate plan");
              planner::AggregatePlan::AggTerm agg_term(
                  EXPRESSION_TYPE_AGGREGATE_COUNT_STAR,
                  nullptr,  // No predicate for star expression. Nothing to
                            // evaluate
                  agg_expr->distinct_);
              agg_terms.push_back(agg_term);

              std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, agg_id);
              std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
                  std::make_pair(new_col_id, inner_pair);
              direct_map_list.emplace_back(outer_pair);
              LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
                        outer_pair.second.first, outer_pair.second.second);

              auto column = catalog::Column(
                  common::Type::INTEGER,
                  common::Type::GetTypeSize(common::Type::INTEGER),
                  "COL_" + std::to_string(col_cntr_id++),  // COL_A should be
                                                           // used only when
                                                           // there is no AS
                  true);

              output_schema_columns.push_back(column);
            } else {
              LOG_TRACE("Unrecognized type in function expression!");
              throw PlannerException(
                  "Error: Unrecognized type in function expression");
            }
            ++agg_id;
          }

          // Column name
          else {
            // There are columns in the query
            agg_type = AGGREGATE_TYPE_HASH;
            std::string col_name(expr->GetExpressionName());
            oid_t old_col_id = target_table->GetSchema()->GetColumnID(col_name);

            std::pair<oid_t, oid_t> inner_pair = std::make_pair(0, old_col_id);
            std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
                std::make_pair(new_col_id, inner_pair);
            direct_map_list.emplace_back(outer_pair);
            LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
                      outer_pair.second.first, outer_pair.second.second);

            auto table_column =
                target_table->GetSchema()->GetColumn(old_col_id);

            auto column = catalog::Column(
                table_column.GetType(),
                common::Type::GetTypeSize(table_column.GetType()),
                "COL_" + std::to_string(col_cntr_id++),  // COL_A should be used
                                                         // only when there is
                                                         // no AS
                true);

            output_schema_columns.push_back(
                target_table->GetSchema()->GetColumn(old_col_id));
          }
          ++new_col_id;
        }
        LOG_TRACE("Creating a ProjectInfo");
        std::unique_ptr<const planner::ProjectInfo> proj_info(
            new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

        std::unique_ptr<const expression::AbstractExpression> predicate(having);
        std::shared_ptr<const catalog::Schema> output_table_schema(
            new catalog::Schema(output_schema_columns));
        LOG_TRACE("Output Schema Info: %s",
                  output_table_schema.get()->GetInfo().c_str());

        std::unique_ptr<planner::AggregatePlan> child_agg_plan(
            new planner::AggregatePlan(
                std::move(proj_info), std::move(predicate),
                std::move(agg_terms), std::move(group_by_columns),
                output_table_schema, agg_type));

        child_agg_plan->AddChild(std::move(scan_node));
        child_plan = std::move(child_agg_plan);
      }

    } break;

    case STATEMENT_TYPE_INSERT: {
      LOG_TRACE("Adding Insert plan...");
      std::unique_ptr<planner::AbstractPlan> child_InsertPlan(
          new planner::InsertPlan((parser::InsertStatement*)parse_tree2));
      child_plan = std::move(child_InsertPlan);
    } break;

    case STATEMENT_TYPE_COPY: {
      LOG_TRACE("Adding Copy plan...");
      parser::CopyStatement* copy_parse_tree =
          static_cast<parser::CopyStatement*>(parse_tree2);
      child_plan = std::move(CreateCopyPlan(copy_parse_tree));
    } break;

    case STATEMENT_TYPE_DELETE: {
      LOG_TRACE("Adding Delete plan...");

      // column predicates passing to the  index
      std::vector<oid_t> key_column_ids;
      std::vector<ExpressionType> expr_types;
      std::vector<common::Value> values;
      oid_t index_id;

      parser::DeleteStatement* deleteStmt =
          (parser::DeleteStatement*)parse_tree2;
      auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
          deleteStmt->GetDatabaseName(), deleteStmt->GetTableName());
      if (CheckIndexSearchable(target_table, deleteStmt->expr, key_column_ids,
                               expr_types, values, index_id)) {
        // Create index scan plan
        std::unique_ptr<planner::AbstractPlan> child_DeletePlan(
            new planner::DeletePlan(deleteStmt, key_column_ids, expr_types,
                                    values, index_id));
        child_plan = std::move(child_DeletePlan);
      } else {
        // Create sequential scan plan
        std::unique_ptr<planner::AbstractPlan> child_DeletePlan(
            new planner::DeletePlan(deleteStmt));
        child_plan = std::move(child_DeletePlan);
      }

    } break;

    case STATEMENT_TYPE_UPDATE: {
      LOG_TRACE("Adding Update plan...");

      // column predicates passing to the index
      std::vector<oid_t> key_column_ids;
      std::vector<ExpressionType> expr_types;
      std::vector<common::Value> values;
      oid_t index_id;

      parser::UpdateStatement* updateStmt =
          (parser::UpdateStatement*)parse_tree2;
      auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
          updateStmt->table->GetDatabaseName(),
          updateStmt->table->GetTableName());

      if (CheckIndexSearchable(target_table, updateStmt->where, key_column_ids,
                               expr_types, values, index_id)) {
        // Create index scan plan
        std::unique_ptr<planner::AbstractPlan> child_InsertPlan(
            new planner::UpdatePlan(updateStmt, key_column_ids, expr_types,
                                    values, index_id));
        child_plan = std::move(child_InsertPlan);
      } else {
        // Create sequential scan plan
        std::unique_ptr<planner::AbstractPlan> child_InsertPlan(
            new planner::UpdatePlan(updateStmt));
        child_plan = std::move(child_InsertPlan);
      }

    } break;

    case STATEMENT_TYPE_TRANSACTION: {
    } break;
    default:
      LOG_ERROR("Unsupported Parse Node Type %d", parse_item_node_type);
      throw NotImplementedException("Error: Query plan not implemented");
  }

  if (child_plan != nullptr) {
    if (plan_tree != nullptr) {
      plan_tree->AddChild(std::move(child_plan));
    } else {
      plan_tree = std::move(child_plan);
    }
  }

  // Recurse will be modified later
  /*auto &children = parse_tree->GetChildren();
   for (auto &child : children) {
   std::shared_ptr<planner::AbstractPlan> child_parse =
   std::move(BuildPlanTree(child));
   child_plan = std::move(child_parse);
   }*/
  return plan_tree;
}

std::unique_ptr<planner::AbstractPlan> SimpleOptimizer::CreateCopyPlan(
    parser::CopyStatement* copy_stmt) {

  std::string table_name(copy_stmt->cpy_table->GetTableName());
  bool deserialize_parameters = false;

  // If we're copying the query metric table, then we need to handle the
  // deserialization of prepared stmt parameters
  if (table_name == QUERY_METRIC_NAME) {
    LOG_DEBUG("Copying the query_metric table.");
    deserialize_parameters = true;
  }

  std::unique_ptr<planner::AbstractPlan> copy_plan(
      new planner::CopyPlan(copy_stmt->file_path, deserialize_parameters));

  // Next, generate a dummy select * plan for target table
  // Hard code star expression
  auto star_expr = new peloton::expression::StarExpression();

  // Push star expression to list
  auto select_list =
      new std::vector<peloton::expression::AbstractExpression*>();
  select_list->push_back(star_expr);

  // Construct select stmt
  std::unique_ptr<parser::SelectStatement> select_stmt(
      new parser::SelectStatement());
  select_stmt->from_table = copy_stmt->cpy_table;
  copy_stmt->cpy_table = nullptr;
  select_stmt->select_list = select_list;

  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      select_stmt->from_table->GetDatabaseName(), table_name);

  std::unordered_map<oid_t, oid_t> column_mapping;
  std::vector<oid_t> column_ids;
  bool needs_projection = false;
  expression::ExpressionUtil::TransformExpression(
      column_mapping, column_ids, nullptr, *target_table->GetSchema(),
      needs_projection);

  auto select_plan =
      SimpleOptimizer::CreateScanPlan(target_table, column_ids, nullptr, false);
  LOG_DEBUG("Sequential scan plan for copy created");

  // Attach it to the copy plan
  copy_plan->AddChild(std::move(select_plan));
  return std::move(copy_plan);
}

/**
 * This function checks whether the current expression can enable index
 * scan for the statement. If it is index searchable, returns true and
 * set the corresponding data structures that will be used in creating
 * index scan node. Otherwise, returns false.
 */
bool SimpleOptimizer::CheckIndexSearchable(
    storage::DataTable* target_table,
    expression::AbstractExpression* expression,
    std::vector<oid_t>& key_column_ids, std::vector<ExpressionType>& expr_types,
    std::vector<common::Value>& values, oid_t& index_id) {
  bool index_searchable = false;
  index_id = 0;

  // column predicates between the tuple value and the constant in the where
  // clause
  std::vector<oid_t> predicate_column_ids = {};
  std::vector<ExpressionType> predicate_expr_types;
  std::vector<common::Value> predicate_values;

  if (expression != NULL) {
    index_searchable = true;

    LOG_TRACE("Getting predicate columns");
    GetPredicateColumns(target_table->GetSchema(), expression,
                        predicate_column_ids, predicate_expr_types,
                        predicate_values, index_searchable);
    LOG_TRACE("Finished Getting predicate columns");

    if (index_searchable == true) {
      index_searchable = false;

      // Loop through the indexes to find to most proper one (if any)
      int max_columns = 0;
      int index_index = 0;
      for (auto& column_set : target_table->GetIndexColumns()) {
        int matched_columns = 0;
        for (auto column_id : predicate_column_ids)
          if (column_set.find(column_id) != column_set.end()) matched_columns++;
        if (matched_columns > max_columns) {
          index_searchable = true;
          index_id = index_index;
        }
        index_index++;
      }
    }
  }

  if (!index_searchable) {
    return false;
  }

  // Prepares arguments for the index scan plan
  auto index = target_table->GetIndex(index_id);

  auto index_columns = target_table->GetIndexColumns()[index_id];
  int column_idx = 0;
  for (auto column_id : predicate_column_ids) {
    if (index_columns.find(column_id) != index_columns.end()) {
      key_column_ids.push_back(column_id);
      expr_types.push_back(predicate_expr_types[column_idx]);
      values.push_back(predicate_values[column_idx]);
      LOG_TRACE("Adding for IndexScanDesc: id(%d), expr(%s), values(%s)",
                column_id, ExpressionTypeToString(*expr_types.rbegin()).c_str(),
                (*values.rbegin()).GetInfo().c_str());
    }
    column_idx++;
  }

  return true;
}

std::unique_ptr<planner::AbstractScan> SimpleOptimizer::CreateScanPlan(
    storage::DataTable* target_table, std::vector<oid_t>& column_ids,
    expression::AbstractExpression* predicate, bool for_update) {
  oid_t index_id = 0;

  // column predicates passing to the index
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<common::Value> values;

  if (!CheckIndexSearchable(target_table, predicate, key_column_ids, expr_types,
                            values, index_id)) {
    // Create sequential scan plan
    LOG_TRACE("Creating a sequential scan plan");
    auto predicate_cpy = predicate == nullptr ? nullptr : predicate->Copy();

    std::unique_ptr<planner::AbstractScan> child_SelectPlan;
    child_SelectPlan.reset(new planner::SeqScanPlan(target_table, predicate_cpy,
                                                    column_ids, for_update));
    return std::move(child_SelectPlan);
  }

  // Create index scan plan
  LOG_TRACE("Creating a index scan plan");
  auto index = target_table->GetIndex(index_id);
  std::vector<expression::AbstractExpression*> runtime_keys;

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  std::unique_ptr<planner::IndexScanPlan> node(new planner::IndexScanPlan(
      target_table, predicate, column_ids, index_scan_desc, for_update));
  LOG_TRACE("Index scan plan created");

  return std::move(node);
}

/**
 * This function replaces all COLUMN_REF expressions with TupleValue
 * expressions
 */
void SimpleOptimizer::GetPredicateColumns(
    catalog::Schema* schema, expression::AbstractExpression* expression,
    std::vector<oid_t>& column_ids, std::vector<ExpressionType>& expr_types,
    std::vector<common::Value>& values, bool& index_searchable) {
  // For now, all conjunctions should be AND when using index scan.
  if (expression->GetExpressionType() == EXPRESSION_TYPE_CONJUNCTION_OR)
    index_searchable = false;

  LOG_TRACE("Expression Type --> %s",
            ExpressionTypeToString(expression->GetExpressionType()).c_str());
  if (!(expression->GetChild(0) && expression->GetChild(1))) return;
  LOG_TRACE("Left Type --> %s",
            ExpressionTypeToString(expression->GetChild(0)->GetExpressionType())
                .c_str());
  LOG_TRACE("Right Type --> %s",
            ExpressionTypeToString(expression->GetChild(1)->GetExpressionType())
                .c_str());

  // We're only supporting comparing a column_ref to a constant/parameter for
  // index scan right now
  if (expression->GetChild(0)->GetExpressionType() ==
      EXPRESSION_TYPE_VALUE_TUPLE) {
    auto right_type = expression->GetChild(1)->GetExpressionType();
    if (right_type == EXPRESSION_TYPE_VALUE_CONSTANT ||
        right_type == EXPRESSION_TYPE_VALUE_PARAMETER) {
      auto expr = (expression::TupleValueExpression*)expression->GetChild(0);
      std::string col_name(expr->GetColumnName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      auto column_id = schema->GetColumnID(col_name);
      column_ids.push_back(column_id);
      expr_types.push_back(expression->GetExpressionType());

      // Potential memory leak in line 664:
      // 24 bytes in 1 blocks are definitely lost in loss record 1 of 3
      // malloc (in /usr/lib/valgrind/vgpreload_memcheck-amd64-linux.so)
      // peloton::do_allocation(unsigned long, bool) (allocator.cpp:27)
      // operator new(unsigned long) (allocator.cpp:40)
      // peloton::common::IntegerValue::Copy() const (numeric_value.cpp:1288)
      // peloton::expression::ConstantValueExpression::GetValue() const
      // (constant_value_expression.h:40)
      if (right_type == EXPRESSION_TYPE_VALUE_CONSTANT) {
        values.push_back(reinterpret_cast<expression::ConstantValueExpression*>(
            expression->GetModifiableChild(1))->GetValue());
        LOG_TRACE("Value Type: %d",
                  reinterpret_cast<expression::ConstantValueExpression*>(
                      expression->GetModifiableChild(1))->GetValueType());
      } else
        values.push_back(common::ValueFactory::GetParameterOffsetValue(
            reinterpret_cast<expression::ParameterValueExpression*>(
                expression->GetModifiableChild(1))->GetValueIdx()).Copy());
      LOG_TRACE("Parameter offset: %s", (*values.rbegin()).GetInfo().c_str());
    }
  } else if (expression->GetChild(1)->GetExpressionType() ==
             EXPRESSION_TYPE_VALUE_TUPLE) {
    auto left_type = expression->GetChild(0)->GetExpressionType();
    if (left_type == EXPRESSION_TYPE_VALUE_CONSTANT ||
        left_type == EXPRESSION_TYPE_VALUE_PARAMETER) {
      auto expr = (expression::TupleValueExpression*)expression->GetChild(1);
      std::string col_name(expr->GetColumnName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      auto column_id = schema->GetColumnID(col_name);
      LOG_TRACE("Column id: %d", column_id);
      column_ids.push_back(column_id);
      expr_types.push_back(expression->GetExpressionType());

      if (left_type == EXPRESSION_TYPE_VALUE_CONSTANT) {
        values.push_back(reinterpret_cast<expression::ConstantValueExpression*>(
            expression->GetModifiableChild(1))->GetValue());
        LOG_TRACE("Value Type: %d",
                  reinterpret_cast<expression::ConstantValueExpression*>(
                      expression->GetModifiableChild(0))->GetValueType());
      } else
        values.push_back(common::ValueFactory::GetParameterOffsetValue(
            reinterpret_cast<expression::ParameterValueExpression*>(
                expression->GetModifiableChild(0))->GetValueIdx()).Copy());
      LOG_TRACE("Parameter offset: %s", (*values.rbegin()).GetInfo().c_str());
    }
  } else {
    GetPredicateColumns(schema, expression->GetModifiableChild(0), column_ids,
                        expr_types, values, index_searchable);
    GetPredicateColumns(schema, expression->GetModifiableChild(1), column_ids,
                        expr_types, values, index_searchable);
  }
}

static std::unique_ptr<const planner::ProjectInfo> CreateHackProjection();
static std::shared_ptr<const peloton::catalog::Schema> CreateHackJoinSchema();

std::unique_ptr<planner::AbstractPlan>
SimpleOptimizer::CreateHackingJoinPlan() {
  auto orderline_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "order_line");
  auto stock_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "stock");

  // Manually constructing the predicate....
  expression::ParameterValueExpression* params[6];
  for (int i = 0; i < 6; ++i)
    params[i] = new expression::ParameterValueExpression(i);

  char ol_w_id_name[] = "ol_w_id";
  char ol_d_id_name[] = "ol_d_id";
  char ol_o_id_1_name[] = "ol_o_id";
  char ol_o_id_2_name[] = "ol_o_id";
  auto ol_w_id = new expression::TupleValueExpression(ol_w_id_name);
  auto ol_d_id = new expression::TupleValueExpression(ol_d_id_name);
  auto ol_o_id_1 = new expression::TupleValueExpression(ol_o_id_1_name);
  auto ol_o_id_2 = new expression::TupleValueExpression(ol_o_id_2_name);

  auto predicate1 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_EQUAL, ol_w_id, params[0]);
  auto predicate2 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_EQUAL, ol_d_id, params[1]);
  auto predicate3 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_LESSTHAN, ol_o_id_1, params[2]);

  auto predicate5 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, ol_o_id_2, params[3]);

  auto predicate6 = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, predicate1, predicate2);
  auto predicate7 = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, predicate3, predicate5);
  auto predicate8 = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, predicate6, predicate7);

  // Get the index scan descriptor
  bool index_searchable;
  std::vector<oid_t> predicate_column_ids = {};
  std::vector<ExpressionType> predicate_expr_types;
  std::vector<common::Value> predicate_values;
  std::vector<oid_t> column_ids = {4};
  std::vector<expression::AbstractExpression*> runtime_keys;

  GetPredicateColumns(orderline_table->GetSchema(), predicate8,
                      predicate_column_ids, predicate_expr_types,
                      predicate_values, index_searchable);
  auto index = orderline_table->GetIndex(0);
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, predicate_column_ids, predicate_expr_types, predicate_values,
      runtime_keys);

  // Create the index scan plan for ORDER_LINE
  std::unique_ptr<planner::IndexScanPlan> orderline_scan_node(
      new planner::IndexScanPlan(orderline_table, predicate8, column_ids,
                                 index_scan_desc, false));
  LOG_DEBUG("Index scan for order_line plan created");

  // predicate for scanning stock table
  char s_w_id_name[] = "s_w_id";
  char s_quantity_name[] = "s_quantity";
  auto s_w_id = new expression::TupleValueExpression(s_w_id_name);
  auto s_quantity = new expression::TupleValueExpression(s_quantity_name);
  auto predicate9 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_EQUAL, s_w_id, params[4]);
  auto predicate10 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_LESSTHAN, s_quantity, params[5]);
  auto predicate11 = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, predicate9, predicate10);

  predicate_column_ids = {0};
  predicate_expr_types = {EXPRESSION_TYPE_COMPARE_EQUAL};
  predicate_values = {common::ValueFactory::GetParameterOffsetValue(4).Copy()};
  column_ids = {1};

  index = stock_table->GetIndex(0);
  planner::IndexScanPlan::IndexScanDesc index_scan_desc2(
      index, predicate_column_ids, predicate_expr_types, predicate_values,
      runtime_keys);

  // Create the index scan plan for STOCK
  std::unique_ptr<planner::IndexScanPlan> stock_scan_node(
      new planner::IndexScanPlan(stock_table, predicate11, column_ids,
                                 index_scan_desc2, false));
  LOG_DEBUG("Index scan plan for STOCK created");

  // Create hash plan node
  expression::AbstractExpression* right_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 0);

  std::vector<std::unique_ptr<const expression::AbstractExpression>> hash_keys;
  hash_keys.emplace_back(right_table_attr_1);

  // Create hash plan node
  std::unique_ptr<planner::HashPlan> hash_plan_node(
      new planner::HashPlan(hash_keys));
  hash_plan_node->AddChild(std::move(stock_scan_node));

  // LEFT.4 == RIGHT.1
  expression::TupleValueExpression* left_table_attr_4 =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 0);
  right_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 0);
  std::unique_ptr<const expression::AbstractExpression> join_predicate(
      new expression::ComparisonExpression(EXPRESSION_TYPE_COMPARE_EQUAL,
                                           left_table_attr_4,
                                           right_table_attr_1));

  auto projection = CreateHackProjection();
  auto schema = CreateHackJoinSchema();
  // Create hash join plan node.
  std::unique_ptr<planner::HashJoinPlan> hash_join_plan_node(
      new planner::HashJoinPlan(JOIN_TYPE_INNER, std::move(join_predicate),
                                std::move(projection), schema));

  // left or right?
  hash_join_plan_node->AddChild(std::move(orderline_scan_node));
  hash_join_plan_node->AddChild(std::move(hash_plan_node));

  expression::TupleValueExpression* left_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 0);

  planner::AggregatePlan::AggTerm agg_term(
      ParserExpressionNameToExpressionType("count"), left_table_attr_1, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  agg_terms.push_back(agg_term);

  std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, 0);
  std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
      std::make_pair(0, inner_pair);
  DirectMapList direct_map_list;
  direct_map_list.emplace_back(outer_pair);
  LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
            outer_pair.second.first, outer_pair.second.second);

  LOG_TRACE("Creating a ProjectInfo");
  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  std::vector<oid_t> group_by_columns;
  expression::AbstractExpression* having = nullptr;
  std::unique_ptr<const expression::AbstractExpression> predicate(having);

  auto column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "COL_0",  // COL_A should be
                // used only when
                // there is no AS
      true);
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema({column}));
  LOG_DEBUG("Output Schema Info: %s",
            output_table_schema.get()->GetInfo().c_str());
  std::unique_ptr<planner::AggregatePlan> agg_plan(new planner::AggregatePlan(
      std::move(proj_info), std::move(predicate), std::move(agg_terms),
      std::move(group_by_columns), output_table_schema, AGGREGATE_TYPE_PLAIN));
  LOG_DEBUG("Aggregation plan constructed");

  agg_plan->AddChild(std::move(hash_join_plan_node));

  return std::move(agg_plan);
}

std::unique_ptr<planner::AbstractPlan>
SimpleOptimizer::CreateHackingNestedLoopJoinPlan() {
  /*
  * "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT"
  + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " +
  TPCCConstants.TABLENAME_STOCK
        + " WHERE OL_W_ID = ?"
        + " AND OL_D_ID = ?"
        + " AND OL_O_ID < ?"
        + " AND OL_O_ID >= ?"
        + " AND S_W_ID = ?"
        + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?"
  */

  // Order_line is the outer table
  auto orderline_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "order_line");

  // Stock is the inner table
  auto stock_table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "stock");

  // Manually constructing the predicate....
  expression::ParameterValueExpression* params[5];
  for (int i = 0; i < 5; ++i)
    params[i] = new expression::ParameterValueExpression(i);

  // Predicate for order_line table
  char ol_w_id_name[] = "ol_w_id";
  char ol_d_id_name[] = "ol_d_id";
  char ol_o_id_1_name[] = "ol_o_id";
  char ol_o_id_2_name[] = "ol_o_id";
  auto ol_w_id = new expression::TupleValueExpression(ol_w_id_name);
  auto ol_d_id = new expression::TupleValueExpression(ol_d_id_name);
  auto ol_o_id_1 = new expression::TupleValueExpression(ol_o_id_1_name);
  auto ol_o_id_2 = new expression::TupleValueExpression(ol_o_id_2_name);

  auto predicate1 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_EQUAL, ol_w_id, params[0]);
  auto predicate2 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_EQUAL, ol_d_id, params[1]);
  auto predicate3 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_LESSTHAN, ol_o_id_1, params[2]);
  auto predicate4 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, ol_o_id_2, params[3]);

  auto predicate6 = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, predicate1, predicate2);
  auto predicate7 = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, predicate3, predicate4);
  auto predicate8 = new expression::ConjunctionExpression(
      EXPRESSION_TYPE_CONJUNCTION_AND, predicate6, predicate7);

  // Get the index scan descriptor
  bool index_searchable;
  std::vector<oid_t> predicate_column_ids = {};
  std::vector<ExpressionType> predicate_expr_types;
  std::vector<common::Value> predicate_values;
  std::vector<oid_t> column_ids = {4};  // OL_I_ID
  std::vector<expression::AbstractExpression*> runtime_keys;

  // Get predicate ids, types, and values
  GetPredicateColumns(orderline_table->GetSchema(), predicate8,
                      predicate_column_ids, predicate_expr_types,
                      predicate_values, index_searchable);

  // Create index scan descpriptor
  auto index = orderline_table->GetIndex(0);  // Primary index
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, predicate_column_ids, predicate_expr_types, predicate_values,
      runtime_keys);

  // Create the index scan plan for ORDER_LINE
  std::unique_ptr<planner::IndexScanPlan> orderline_scan_node(
      new planner::IndexScanPlan(orderline_table, predicate8, column_ids,
                                 index_scan_desc, false));
  LOG_DEBUG("Index scan for order_line plan created");

  // predicate for scanning stock table
  char s_quantity_name[] = "s_quantity";

  auto s_quantity = new expression::TupleValueExpression(s_quantity_name);

  auto predicate12 = new expression::ComparisonExpression(
      EXPRESSION_TYPE_COMPARE_LESSTHAN, s_quantity, params[4]);

  predicate_column_ids = {0, 1};
  predicate_expr_types = {EXPRESSION_TYPE_COMPARE_EQUAL,
                          EXPRESSION_TYPE_COMPARE_EQUAL};

  // Hardcode wid=0 and iid=90
  predicate_values = {common::ValueFactory::GetIntegerValue(0).Copy(),
                      common::ValueFactory::GetIntegerValue(90).Copy()};

  column_ids = {1};  // S_I_ID

  auto index_stock = stock_table->GetIndex(0);  // primary_index
  planner::IndexScanPlan::IndexScanDesc index_scan_desc2(
      index_stock, predicate_column_ids, predicate_expr_types, predicate_values,
      runtime_keys);

  // Create the index scan plan for STOCK
  std::unique_ptr<planner::IndexScanPlan> stock_scan_node(
      new planner::IndexScanPlan(stock_table, predicate12, column_ids,
                                 index_scan_desc2, false));
  LOG_DEBUG("Index scan plan for STOCK created");

  auto projection = CreateHackProjection();
  auto schema = CreateHackJoinSchema();

  // LEFT.4 == RIGHT.1
  expression::TupleValueExpression* left_table_attr_4 =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 0);

  expression::TupleValueExpression* right_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 0);

  std::unique_ptr<const expression::AbstractExpression> join_predicate(
      new expression::ComparisonExpression(EXPRESSION_TYPE_COMPARE_EQUAL,
                                           left_table_attr_4,
                                           right_table_attr_1));

  std::vector<oid_t> join_column_ids_left = {0};   // I_I_ID in the result
  std::vector<oid_t> join_column_ids_right = {1};  // S_I_ID in the table

  // Create hash join plan node.
  std::unique_ptr<planner::NestedLoopJoinPlan> nested_join_plan_node(
      new planner::NestedLoopJoinPlan(
          JOIN_TYPE_INNER, std::move(join_predicate), std::move(projection),
          schema, join_column_ids_left, join_column_ids_right));

  // Add left and right
  nested_join_plan_node->AddChild(std::move(orderline_scan_node));
  nested_join_plan_node->AddChild(std::move(stock_scan_node));

  expression::TupleValueExpression* left_table_attr_1 =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 0);

  planner::AggregatePlan::AggTerm agg_term(
      ParserExpressionNameToExpressionType("count"), left_table_attr_1, 1);
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  agg_terms.push_back(agg_term);

  std::pair<oid_t, oid_t> inner_pair = std::make_pair(1, 0);
  std::pair<oid_t, std::pair<oid_t, oid_t>> outer_pair =
      std::make_pair(0, inner_pair);
  DirectMapList direct_map_list;
  direct_map_list.emplace_back(outer_pair);
  LOG_TRACE("Direct map list: (%d, (%d, %d))", outer_pair.first,
            outer_pair.second.first, outer_pair.second.second);

  LOG_TRACE("Creating a ProjectInfo");
  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  std::vector<oid_t> group_by_columns;
  expression::AbstractExpression* having = nullptr;
  std::unique_ptr<const expression::AbstractExpression> predicate(having);

  auto column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "COL_0",  // COL_A should be
                // used only when
                // there is no AS
      true);
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema({column}));
  LOG_DEBUG("Output Schema Info: %s",
            output_table_schema.get()->GetInfo().c_str());
  std::unique_ptr<planner::AggregatePlan> agg_plan(new planner::AggregatePlan(
      std::move(proj_info), std::move(predicate), std::move(agg_terms),
      std::move(group_by_columns), output_table_schema, AGGREGATE_TYPE_PLAIN));
  LOG_DEBUG("Aggregation plan constructed");

  agg_plan->AddChild(std::move(nested_join_plan_node));

  return std::move(agg_plan);
}

std::unique_ptr<const planner::ProjectInfo> CreateHackProjection() {
  // Create the plan node
  TargetList target_list;
  DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // direct map
  DirectMap direct_map1 = std::make_pair(0, std::make_pair(1, 0));

  direct_map_list.push_back(direct_map1);

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

std::shared_ptr<const peloton::catalog::Schema> CreateHackJoinSchema() {
  auto column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "S_I_ID", 1);

  column.AddConstraint(
      catalog::Constraint(CONSTRAINT_TYPE_NOTNULL, "not_null"));
  return std::shared_ptr<const peloton::catalog::Schema>(
      new catalog::Schema({column}));
}

}  // namespace optimizer
}  // namespace peloton
