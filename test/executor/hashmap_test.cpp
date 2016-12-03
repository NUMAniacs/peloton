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

#define NUM_ITERATION 5000000

namespace peloton {
namespace test {

class HashmapTests : public PelotonTest {};

namespace {

typedef executor::Hashmap<
    expression::ContainerTuple<executor::LogicalTile>,               // Key
    std::shared_ptr<executor::ParallelHashExecutor::ConcurrentSet>,  // T
    expression::ContainerTupleHasher<executor::LogicalTile>,         // Hash
    expression::ContainerTupleComparator<executor::LogicalTile>,     // Pred
    4,  // Bucket size
    1   // Probe step size
    > HashMapType;

typedef executor::Hashmap<int,                 // Key
                          int,                 // T
                          std::hash<int>,      // Hash
                          std::equal_to<int>,  // Pred
                          4,                   // Bucket size
                          1                    // Probe step size
                          > TestHashMapType;

void PutTest(TestHashMapType *hashmap, int num_keys,
             UNUSED_ATTRIBUTE uint64_t thread_itr) {
  for (oid_t itr = 0; itr < NUM_ITERATION; itr++) {
    for (int k = 0; k < num_keys; k++) {
      hashmap->Put(k, k);
    }
  }
}

TEST_F(HashmapTests, BasicTest) {

  int num_keys = 4;
  TestHashMapType hashmap;
  hashmap.Reserve(num_keys * 2);

  int key1 = 1;
  int val1 = 1;
  int val1_result = 0;
  hashmap.Put(key1, val1);
  hashmap.Get(key1, val1_result);
  EXPECT_EQ(val1, val1_result);
  size_t num_threads = 4;

  LaunchParallelTest(num_threads, PutTest, &hashmap, num_keys);
  int v = 0;
  for (int k = 0; k < num_keys; k++) {
    hashmap.Get(k, v);
    EXPECT_EQ(k, v);
  }
}
}

}  // namespace test
}  // namespace peloton
