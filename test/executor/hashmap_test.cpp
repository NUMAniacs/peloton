//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_test.cpp
//
// Identification: test/executor/limit_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/harness.h"
#include "common/types.h"
#include "common/value.h"
#include "common/container_tuple.h"
#include "executor/logical_tile.h"
#include "executor/parallel_hash_executor.h"
#include "executor/hashmap.h"
#include "executor/logical_tile_factory.h"
#include "storage/data_table.h"

#define NUM_ITERATION 5000

namespace peloton {
namespace test {

class HashmapTests : public PelotonTest {};

namespace {

typedef executor::Hashmap<
    int,                                                                // Key
    std::shared_ptr<executor::ParallelHashExecutor::ConcurrentVector>,  // T
    std::hash<int>,                                                     // Hash
    std::equal_to<int>,                                                 // Pred
    4,  // Bucket size
    1   // Probe step size
    > TestHashMapType;

void PutTest(TestHashMapType *hashmap, int num_keys,
             UNUSED_ATTRIBUTE uint64_t thread_itr) {
  for (oid_t itr = 0; itr < NUM_ITERATION; itr++) {
    for (int k = 0; k < num_keys; k++) {
      std::shared_ptr<executor::ParallelHashExecutor::ConcurrentVector> val;
      bool success = hashmap->Get(k, val);
      if (success == false) {
        val.reset(new executor::ParallelHashExecutor::ConcurrentVector());
        if (hashmap->Put(k, val) == false) {
          success = hashmap->Get(k, val);
          EXPECT_TRUE(success);
        }
      }
      val->Insert(std::make_tuple(1, 1, 1));
    }
  }
}

TEST_F(HashmapTests, BasicTest) {

  int num_keys = 4;
  TestHashMapType hashmap;
  hashmap.Reserve(num_keys * 2);

  std::shared_ptr<executor::ParallelHashExecutor::ConcurrentVector> val1(
      new executor::ParallelHashExecutor::ConcurrentVector());
  val1->Insert(std::make_tuple(1, 1, 1));
  int key1 = 1;
  std::shared_ptr<executor::ParallelHashExecutor::ConcurrentVector> result;
  bool success = hashmap.Put(key1, val1);
  EXPECT_TRUE(success);

  // Get existing key should succeed
  success = hashmap.Get(key1, result);
  EXPECT_TRUE(success);

  auto result_size = val1->GetVector().size();
  EXPECT_EQ(result_size, val1->GetVector().size());

  // Get non-existing key should fail
  int key2 = 2;
  success = hashmap.Get(key2, result);
  EXPECT_FALSE(success);
}

// Insert to hash map in parallel
TEST_F(HashmapTests, MultiThreadTest) {
  int num_keys = 4;
  TestHashMapType hashmap;
  hashmap.Reserve(num_keys * 2, false);

  size_t num_threads = 8;
  LaunchParallelTest(num_threads, PutTest, &hashmap, num_keys);

  // Validate the number of elements for each key
  std::shared_ptr<executor::ParallelHashExecutor::ConcurrentVector> v;
  for (int k = 0; k < num_keys; k++) {
    bool success = hashmap.Get(k, v);
    EXPECT_TRUE(success);
    EXPECT_EQ(v->GetVector().size(), num_threads * NUM_ITERATION);
  }
}
}

}  // namespace test
}  // namespace peloton
