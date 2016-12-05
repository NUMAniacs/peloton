//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_dependent.h
//
// Identification: src/include/planner/abstract_dependent.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <vector>
#include <vector>

#include "catalog/schema.h"
#include "common/printable.h"
#include "common/serializeio.h"
#include "common/serializer.h"
#include "common/types.h"
#include "common/value.h"
#include "common/macros.h"

namespace peloton {

namespace executor {
class AbstractTask;
}

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Dependent
//===--------------------------------------------------------------------===//
/*
* This class is notified when dependency completes.
*/
class Dependent {
 public:
  typedef std::chrono::high_resolution_clock::time_point time_type;
  virtual ~Dependent() {}

  virtual void DependencyComplete(
      std::shared_ptr<executor::AbstractTask> task) = 0;

  void RecordTaskGenStart() {
    task_gen_begin_ = std::chrono::high_resolution_clock::now();
  }

  void RecordTaskGenEnd() {
    task_gen_end_ = std::chrono::high_resolution_clock::now();
  }

  void RecordTaskExecutionStart() {
    task_ex_begin_ = std::chrono::high_resolution_clock::now();
  }

  void RecordTaskExecutionEnd() {
    task_ex_end_ = std::chrono::high_resolution_clock::now();
  }

  double GetTaskGenTimeMS() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        task_gen_end_ - task_gen_begin_).count();
  }

  double GetTaskExecutionTimeMS() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        task_ex_end_ - task_ex_begin_).count();
  }

  // The parent dependent planner
  Dependent *parent_dependent = nullptr;

  // Keep track of multiple dependents
  size_t dependency_count = 1;

 private:
  time_type task_gen_begin_;
  time_type task_gen_end_;
  time_type task_ex_begin_;
  time_type task_ex_end_;
};

}  // namespace planner
}  // namespace peloton
