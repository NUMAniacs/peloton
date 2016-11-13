//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_Task.h
//
// Identification: src/include/Task/abstract_Task.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

// TODO move me to type.h
#define DEFAULT_PARTITION_ID 0

namespace peloton {

namespace planner {
class AbstractPlan;
}

namespace executor {

class AbstractTask {
 public:
  virtual ~AbstractTask() {}

  explicit AbstractTask(const planner::AbstractPlan *node,
                        int partition_id = DEFAULT_PARTITION_ID)
      : node(node), partition_id(partition_id) {}

  /** @brief Plan node corresponding to this Task. */
  const planner::AbstractPlan *node = nullptr;

  // The target partition's id
  int partition_id = DEFAULT_PARTITION_ID;
};

class InsertTask : public AbstractTask {
 public:
  /**
   * @brief Constructor for insert Task.
   * @param node Insert node corresponding to this Task.
   */
  explicit InsertTask(const planner::AbstractPlan *node, int bulk_insert_count,
                      int partition_id = DEFAULT_PARTITION_ID)
      : AbstractTask(node, partition_id) {
    // By default we insert all the tuples
    tuple_bitmap.resize(bulk_insert_count, true);
  }

  // TODO use bitset instead to save space
  // The bitmap of tuples to insert
  std::vector<bool> tuple_bitmap;
};

}  // namespace executor
}  // namespace peloton
