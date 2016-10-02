//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.h
//
// Identification: src/include/executor/plan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/types.h"
#include "common/statement.h"
#include "executor/abstract_executor.h"
#include "boost/thread/future.hpp"


namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Plan Executor
//===--------------------------------------------------------------------===//

typedef struct peloton_status {
  peloton::Result m_result;
  int *m_result_slots;

  // number of tuples processed
  uint32_t m_processed;

  peloton_status() {
    m_processed = 0;
    m_result = peloton::RESULT_SUCCESS;
    m_result_slots = nullptr;
  }

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(peloton::SerializeOutput &output);
  bool DeserializeFrom(peloton::SerializeInput &input);

} peloton_status;

/*
 * Struct to hold parameters used by the exchange operator
 */
struct ExchangeParams {
  boost::promise<bridge::peloton_status> p;
  boost::unique_future<bridge::peloton_status> f;
  std::vector<ResultType> results;
  const planner::AbstractPlan *plan;
  const std::vector<common::Value *> params;
  int parallelism_count, partition_id;
  ExchangeParams *self;

  inline ExchangeParams(const planner::AbstractPlan *plan,
                        const std::vector<common::Value *>& params,
                        int parallelism_count, int partition_id)
      : plan(plan), params(params),
        parallelism_count(parallelism_count),
        partition_id(partition_id) {
    f = p.get_future();
  }
};


class PlanExecutor {
 public:
  PlanExecutor(const PlanExecutor &) = delete;
  PlanExecutor &operator=(const PlanExecutor &) = delete;
  PlanExecutor(PlanExecutor &&) = delete;
  PlanExecutor &operator=(PlanExecutor &&) = delete;

  PlanExecutor(){};

  static void PrintPlan(const planner::AbstractPlan *plan,
                        std::string prefix = "");

  // Copy From
  static inline void copyFromTo(const char *src, std::vector<unsigned char> &dst) {
    if (src == nullptr) {
      return;
    }
    size_t len = strlen(src);
    for(unsigned int i = 0; i < len; i++){
      dst.push_back((unsigned char)src[i]);
    }
  }

  /* TODO: Delete this method
    static peloton_status ExecutePlan(const planner::AbstractPlan *plan,
                                      ParamListInfo m_param_list,
                                      TupleDesc m_tuple_desc);
  */

  /*
   * @brief Use std::vector<common::Value *> as params to make it more elegant for
   * networking
   * Before ExecutePlan, a node first receives value list, so we should pass
   * value list directly rather than passing Postgres's ParamListInfo.
   *
   * Also pass parallelism details if we are doing intra-query parallelism
   *
   */
  static void ExecutePlanLocal(ExchangeParams **exchg_params_arg);

  /*
   * @brief When a peloton node recvs a query plan in rpc mode,
   * this function is invoked
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  static void ExecutePlanRemote(
      const planner::AbstractPlan *plan, const std::vector<common::Value *> &params,
      std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list,
      boost::promise<int> &p);
};

}  // namespace bridge
}  // namespace peloton
