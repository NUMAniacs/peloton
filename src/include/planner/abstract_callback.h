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

namespace peloton {

namespace executor {
class AbstractTask;
// class LogicalTile;
}
//
// namespace catalog {
// class Schema;
//}
//
// namespace expression {
// class AbstractExpression;
//}

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Callback
//===--------------------------------------------------------------------===//
/*
* This class can be notified when a task completes
* This class is used when dependency completes. Need another class for task
* completion
*/
class Notifiable {
 public:
  virtual ~Notifiable() {}
  // TODO Rename this function to Dependency complete..
  virtual void TaskComplete(std::shared_ptr<executor::AbstractTask> task) = 0;
};

}  // namespace planner
}  // namespace peloton
