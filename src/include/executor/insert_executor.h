//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.h
//
// Identification: src/include/executor/insert_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"
#include "executor/abstract_task.h"

#include <vector>

namespace peloton {
namespace executor {

class InsertExecutor : public AbstractExecutor {
 public:
  InsertExecutor(const InsertExecutor &) = delete;
  InsertExecutor &operator=(const InsertExecutor &) = delete;
  InsertExecutor(InsertExecutor &&) = delete;
  InsertExecutor &operator=(InsertExecutor &&) = delete;

  explicit InsertExecutor(ExecutorContext *executor_context);

  static void ExecuteTask(std::shared_ptr<AbstractTask> task);

 protected:
  bool DInit();

  bool DExecute();

 private:
  // The task object for insert
  std::shared_ptr<AbstractTask> task_;

  // Whether execution is done
  bool done_ = false;
};

}  // namespace executor
}  // namespace peloton
