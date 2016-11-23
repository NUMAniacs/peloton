//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_hash_plan.h
//
// Identification: src/include/planner/parallel_hash_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "abstract_plan.h"
#include "common/types.h"
#include "expression/abstract_expression.h"
#include "planner/abstract_dependent.h"
#include "executor/parallel_hash_executor.h"

namespace peloton {
namespace planner {

/**
 * @brief
 *
 */
class ParallelHashPlan : public AbstractPlan, public Dependent {
 public:
  ParallelHashPlan(const ParallelHashPlan &) = delete;
  ParallelHashPlan &operator=(const ParallelHashPlan &) = delete;
  ParallelHashPlan(const ParallelHashPlan &&) = delete;
  ParallelHashPlan &operator=(const ParallelHashPlan &&) = delete;

  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;

  ParallelHashPlan(std::vector<HashKeyPtrType> &hashkeys)
      : hash_keys_(std::move(hashkeys)) {}

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_PARALLEL_HASH;
  }

  const std::string GetInfo() const { return "ParallelHashPlan"; }

  inline const std::vector<HashKeyPtrType> &GetHashKeys() const {
    return this->hash_keys_;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::vector<HashKeyPtrType> copied_hash_keys;
    for (const auto &key : hash_keys_) {
      copied_hash_keys.push_back(std::unique_ptr<HashKeyType>(key->Copy()));
    }
    return std::unique_ptr<AbstractPlan>(
        new ParallelHashPlan(copied_hash_keys));
  }

  // when a task completes it will call this
  // XXX Assume the task is seq scan task for now
  void DependencyComplete(std::shared_ptr<executor::AbstractTask> task)
      override {
    (void)task;
  }

  // TODO Hacky function which should be removed. Used for join test..
  std::shared_ptr<executor::ParallelHashExecutor> DependencyComplete(
      std::shared_ptr<executor::AbstractTask> task, bool hack);

 private:
  std::vector<HashKeyPtrType> hash_keys_;
};
}
}
