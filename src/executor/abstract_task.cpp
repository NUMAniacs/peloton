//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_task.cpp
//
// Identification: src/executor/abstract_task.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"
#include "executor/abstract_executor.h"
#include "executor/abstract_task.h"

namespace peloton {
namespace executor {

// Initialize the task with callbacks
void AbstractTask::Init(executor::Trackable *trackable,
                        planner::Dependent *dependent, size_t num_tasks) {
  this->trackable = trackable;
  this->dependent = dependent;
  this->num_tasks = num_tasks;
  trackable->SetNumTasks(num_tasks);
  if (result_tile_lists != nullptr) {
    result_tile_lists->resize(num_tasks);
  }
  initialized = true;
}

}  // namespace executor
}  // namespace peloton
