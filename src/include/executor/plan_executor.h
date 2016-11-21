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
#include <condition_variable>

#include "common/statement.h"
#include "common/types.h"
#include "concurrency/transaction_manager.h"
#include "executor/abstract_executor.h"
#include "executor/abstract_task.h"
#include "boost/thread/future.hpp"

namespace peloton {

namespace executor {
class ParallelHashExecutor;
}

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
* This class can be notified when a task completes
*/
class Notifiable {
 public:
  virtual ~Notifiable() {}
  virtual void TaskComplete(std::shared_ptr<executor::AbstractTask> task) = 0;
};

/*
* This class can be notified when a task completes
*/
class BlockingWait : public Notifiable {
 public:
  BlockingWait(int total_tasks)
      : Notifiable(),
        total_tasks_(total_tasks),
        tasks_complete_(0),
        all_done(false) {}

  ~BlockingWait() {}

  // when a task completes it will call this
  void TaskComplete(
      UNUSED_ATTRIBUTE std::shared_ptr<executor::AbstractTask> task) override {
    int task_num = tasks_complete_.fetch_add(1);

    if (task_num == total_tasks_ - 1) {
      // we are the last task to complete
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
  int total_tasks_;
  std::atomic<int> tasks_complete_;
  bool all_done;

  // dirty mutex is okay for now since this class will be removed
  std::mutex done_lock;
  std::condition_variable cv;
};

/*
 * Struct to hold parameters used by the exchange operator
 */
struct ExchangeParams {
  bridge::peloton_status p_status;
  std::vector<ResultType> result;
  concurrency::Transaction *txn;
  const std::shared_ptr<Statement> statement;
  const std::vector<common::Value> params;
  std::shared_ptr<executor::AbstractTask> task;
  const std::vector<int> result_format;
  bool init_failure;
  ExchangeParams *self;

  inline ExchangeParams(concurrency::Transaction *txn,
                        const std::shared_ptr<Statement> &statement,
                        const std::vector<common::Value> &params,
                        const std::shared_ptr<executor::AbstractTask> &task,
                        const std::vector<int> &result_format,
                        const bool &init_failure)
      : txn(txn),
        statement(statement),
        params(params),
        task(task),
        result_format(result_format),
        init_failure(init_failure) {
    self = this;
  }

  void TaskComplete(const bridge::peloton_status &p_status) {
    this->p_status = p_status;
    PL_ASSERT(task->callback != nullptr);
    task->callback->TaskComplete(task);
  }
};

class PlanExecutor {
 public:
  PlanExecutor(const PlanExecutor &) = delete;
  PlanExecutor &operator=(const PlanExecutor &) = delete;
  PlanExecutor(PlanExecutor &&) = delete;
  PlanExecutor &operator=(PlanExecutor &&) = delete;

  PlanExecutor() {};

  static void PrintPlan(const planner::AbstractPlan *plan,
                        std::string prefix = "");

  // Copy From
  static inline void copyFromTo(const std::string &src,
                                std::vector<unsigned char> &dst) {
    if (src.c_str() == nullptr) {
      return;
    }
    size_t len = src.size();
    for (unsigned int i = 0; i < len; i++) {
      dst.push_back((unsigned char)src[i]);
    }
  }

  /*
   * @brief Use std::vector<common::Value *> as params to make it more elegant
   * for networking Before ExecutePlan, a node first receives value list,
   * so we should pass value list directly rather than passing Postgres's
   * ParamListInfo.
   */
  static void ExecutePlanLocal(ExchangeParams **exchg_params_arg);
  /*
   * @brief When a peloton node recvs a query plan in rpc mode,
   * this function is invoked
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  static void ExecutePlanRemote(
      const planner::AbstractPlan *plan,
      const std::vector<common::Value> &params,
      std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list,
      boost::promise<int> &p);
};

}  // namespace bridge
}  // namespace peloton
