//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.h
//
// Identification: src/include/tcop/tcop.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <vector>

#include "common/portal.h"
#include "common/statement.h"
#include "common/type.h"
#include "common/types.h"
#include "parser/sql_statement.h"

#include "executor/plan_executor.h"

#include <boost/algorithm/string.hpp>
#include <boost/thread/future.hpp>

// used to set number of parallel tasks relative to the number of
// processing units available
#define TASK_MULTIPLIER 4

namespace peloton {
namespace tcop {

//===--------------------------------------------------------------------===//
// TRAFFIC COP
//===--------------------------------------------------------------------===//

class TrafficCop {
  TrafficCop(TrafficCop const &) = delete;

 public:
  // global singleton
  static TrafficCop &GetInstance(void);

  TrafficCop();
  ~TrafficCop();

  // PortalExec - Execute query string
  Result ExecuteStatement(const std::string &query,
                          std::vector<ResultType> &result,
                          std::vector<FieldInfoType> &tuple_descriptor,
                          int &rows_changed, std::string &error_message);

  // ExecPrepStmt - Execute a statement from a prepared and bound statement
  Result ExecuteStatement(
      const std::shared_ptr<Statement> &statement,
      const std::vector<common::Value> &params, const bool unnamed,
      std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
      const std::vector<int> &result_format, std::vector<ResultType> &result,
      int &rows_change, std::string &error_message);

  /*
 * @brief Based on the Volcano 'Exchange' intra-query parallelism model.
 * This operator hands off the query from the libevent thread to the
 * query executor pool and blocks the libevent thread till the equery executes
 */
  static bridge::peloton_status ExchangeOperator(
      const std::shared_ptr<Statement> &statement,
      const std::vector<common::Value> &params,
      std::vector<ResultType>& result, const std::vector<int> &result_format);

  // InitBindPrepStmt - Prepare and bind a query from a query string
  std::shared_ptr<Statement> PrepareStatement(const std::string &statement_name,
                                              const std::string &query_string,
                                              std::string &error_message);

  std::vector<FieldInfoType> GenerateTupleDescriptor(parser::SQLStatement* select_stmt);

  FieldInfoType GetColumnFieldForValueType(std::string column_name,
                                           common::Type::TypeId column_type);

  FieldInfoType GetColumnFieldForAggregates(std::string name,
                                            ExpressionType expr_type);

  int BindParameters(std::vector<std::pair<int, std::string>> &parameters,
                     Statement **stmt, std::string &error_message);
};

}  // End tcop namespace
}  // End peloton namespace
