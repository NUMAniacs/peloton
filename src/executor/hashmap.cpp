//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hashmap.cpp
//
// Identification: src/executor/hashmap.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/platform.h"
#include "common/macros.h"
#include "common/logger.h"
#include "executor/hashmap.h"
#include <numa.h>
#include "common/value.h"
#include "executor/logical_tile.h"
#include "executor/parallel_hash_executor.h"
#include "planner/parallel_hash_plan.h"
#include "expression/tuple_value_expression.h"
#include "common/types.h"
#include "executor/abstract_executor.h"
#include "executor/hashmap.h"
#include "executor/logical_tile.h"
#include "common/container_tuple.h"

namespace peloton {

namespace executor {

HASHMAP_TEMPLATE_ARGUMENTS
void HASHMAP_TYPE::Reserve(size_t size, size_t partition) {
  // Compute the appropriate size so that we allocate pages
  num_buckets_ = (size + BUCKET_SIZE - 1) / BUCKET_SIZE;
  num_buckets_ = RoundUp(num_buckets_);
  num_slots_ = num_buckets_ * BUCKET_SIZE;

  // Malloc the arrays
  size_t bucket_malloc_size = sizeof(Bucket) * num_buckets_;

  LOG_TRACE("size of bucket %d", (int)sizeof(Bucket));
  if (interleave_memory_) {
    // Interleave the memory so that they don't end up in just one region
    // TODO Move numa_alloc_interleaved to macros
    buckets_ = (Bucket *)numa_alloc_interleaved(bucket_malloc_size);
  } else {
    buckets_ = (Bucket *)PL_PARTITION_ALLOC(bucket_malloc_size, partition);
  }

  // Initialize the buckets
  auto bucket_begin = std::chrono::high_resolution_clock::now();
  PL_MEMSET(buckets_, 0, bucket_malloc_size);
  auto bucket_end = std::chrono::high_resolution_clock::now();
  double bucket_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          bucket_end - bucket_begin).count();

  LOG_ERROR("bucket time : %d", (int)bucket_duration);
}

HASHMAP_TEMPLATE_ARGUMENTS
// Returns false for duplicate keys
bool HASHMAP_TYPE::Put(Key &key, Value val) {
  size_t hash = GetHash(key);
  auto bucket_itr = hash / BUCKET_SIZE;
  auto slot_itr = hash % BUCKET_SIZE;

  while (true) {
    Bucket &bucket = buckets_[bucket_itr];
    bucket.lock.Lock();
    for (size_t i = slot_itr; i < BUCKET_SIZE; i++) {

      // Found an empty slot. Success
      if (bucket.occupied[i] == false) {

        // We don't want to construct a kv pair and use copy assign it to the
        // array. Instead construct the kv pair using allocator in place
        static std::allocator<std::pair<const Key, Value>> pair_alloc;
        pair_alloc.construct(&(bucket.kv_pairs[i]), std::forward<Key>(key),
                             std::forward<Value>(val));

        // Mark occupied
        bucket.occupied[i] = true;

        // Unlock before return
        bucket.lock.Unlock();
        return true;
      } else {

        // Duplicate keys
        if (equal_fct_(key, bucket.kv_pairs[i].first)) {

          // Unlock before return
          bucket.lock.Unlock();
          return false;
        }
      }
    }

    // Unlock before return
    bucket.lock.Unlock();
    bucket_itr = Probe(bucket_itr);
    slot_itr = 0;
  }
  return false;
}

// Get the value specified by the key. We don't need to lock anything
HASHMAP_TEMPLATE_ARGUMENTS
bool HASHMAP_TYPE::Get(const Key &key, Value &val) const {
  size_t hash = GetHash(key);
  auto bucket_itr = hash / BUCKET_SIZE;
  auto slot_itr = hash % BUCKET_SIZE;

  while (true) {
    const Bucket &bucket = buckets_[bucket_itr];
    for (size_t i = slot_itr; i < BUCKET_SIZE; i++) {
      // An empty slot. Fail to found one
      if (bucket.occupied[i] == false) {
        return false;
      } else {
        // Matched key
        if (equal_fct_(key, bucket.kv_pairs[i].first)) {
          val = bucket.kv_pairs[i].second;
          return true;
        }
      }
    }
    bucket_itr = Probe(bucket_itr);
    slot_itr = 0;
  }
  return false;
}

template class Hashmap<
    expression::ContainerTuple<LogicalTile>,                  // Key
    std::shared_ptr<ParallelHashExecutor::ConcurrentVector>,  // T
    expression::ContainerTupleHasher<LogicalTile>,            // Hash
    expression::ContainerTupleComparator<LogicalTile>,        // Pred
    5,                                                        // Bucket size
    1                                                         // Probe step size
    >;

}  // namespace executor
}  // namespace peloton
