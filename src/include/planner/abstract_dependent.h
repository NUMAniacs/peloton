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
  virtual ~Dependent() {}

  virtual void DependencyComplete(
      std::shared_ptr<executor::AbstractTask> task) = 0;

  // TODO Also keep an dependency count in this class
};

}  // namespace planner
}  // namespace peloton
