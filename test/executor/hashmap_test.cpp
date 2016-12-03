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

TEST_F(HashmapTests, BasicTest) {
  TestHashMapType hashmap;
  hashmap.Reserve(10);
  std::hash<int> hasher;
  int key1 = 1;
  int val1 = 1;
  int val1_result = 0;
  hashmap.Put(key1, val1);
  hashmap.Get(key1, val1_result);
  // EXPECT_EQ(val1, val1_result);
  EXPECT_EQ(hashmap.GetHash(key1), hasher(key1));
}
}

}  // namespace test
}  // namespace peloton
