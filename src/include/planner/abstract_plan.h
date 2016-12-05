//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.h
//
// Identification: src/include/planner/abstract_plan.h
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
#include "planner/abstract_dependent.h"

namespace peloton {

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace catalog {
class Schema;
}

namespace expression {
class AbstractExpression;
}

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Plan
//===--------------------------------------------------------------------===//

class AbstractPlan : public Printable {
 public:
  AbstractPlan(const AbstractPlan &) = delete;
  AbstractPlan &operator=(const AbstractPlan &) = delete;
  AbstractPlan(AbstractPlan &&) = delete;
  AbstractPlan &operator=(AbstractPlan &&) = delete;

  AbstractPlan();

  virtual ~AbstractPlan();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(std::unique_ptr<AbstractPlan> &&child);

  const std::vector<std::unique_ptr<AbstractPlan>> &GetChildren() const;

  const AbstractPlan *GetParent();

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  // Setting values of the parameters in the prepare statement
  virtual void SetParameterValues(std::vector<common::Value> *values);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

  virtual std::unique_ptr<AbstractPlan> Copy() const = 0;

  // A plan will be sent to anther node via serialization
  // So serialization should be implemented by the derived classes

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  // Each sub-class will have to implement these functions
  // After the implementation for each sub-class, we should set these to pure
  // virtual
  //===--------------------------------------------------------------------===//
  virtual bool SerializeTo(SerializeOutput &output UNUSED_ATTRIBUTE) const {
    PL_ASSERT(&output != nullptr);
    return false;
  }
  virtual bool DeserializeFrom(SerializeInput &input UNUSED_ATTRIBUTE) {
    PL_ASSERT(&input != nullptr);
    return false;
  }
  virtual int SerializeSize() { return 0; }

 public:
  // XXX Temporary function and variable for experiment
  bool random_partition_execution = false;

  // We should have an abstract parallel plan instead
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

 protected:
  // only used by its derived classes (when deserialization)
  AbstractPlan *Parent() { return parent_; }

 private:
  // A plan node can have multiple children
  std::vector<std::unique_ptr<AbstractPlan>> children_;

  // The immediate parent plan node
  AbstractPlan *parent_ = nullptr;

 private:
  typedef std::chrono::high_resolution_clock::time_point time_type;
  time_type task_gen_begin_;
  time_type task_gen_end_;
  time_type task_ex_begin_;
  time_type task_ex_end_;
};

}  // namespace planner
}  // namespace peloton
