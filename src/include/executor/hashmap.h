//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hashmap.h
//
// Identification: src/include/executor/hashmap.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "common/platform.h"
#include "common/macros.h"
#include "common/logger.h"
#include <bitset>
#include <numa.h>

namespace peloton {

namespace executor {

#define PAGE_SIZE 4096

#define HASHMAP_TEMPLATE_ARGUMENTS                          \
  template <class Key, class Value, class Hash, class Pred, \
            const size_t BUCKET_SIZE, const size_t PROBE_STEP_SIZE>

#define HASHMAP_TYPE \
  Hashmap<Key, Value, Hash, Pred, BUCKET_SIZE, PROBE_STEP_SIZE>

HASHMAP_TEMPLATE_ARGUMENTS
class Hashmap {

  // Classes and types
  typedef std::pair<Key, Value> KVPair;

  struct Bucket {
    std::array<KVPair, BUCKET_SIZE> kv_pairs;
    std::bitset<BUCKET_SIZE> occupied;
  };

  // XXX duplication of the cuckoo hashmap interface
 public:
  inline void reserve(size_t size) { Reserve(size); }

  inline bool insert(Key &key, Value val) { return Put(key, val); }

  inline bool find(Key &key, Value &val) { return Get(key, val); }

 public:
  Hashmap(bool interleave_memory = true)
      : interleave_memory_(interleave_memory) {}

  void Reserve(size_t size);

  // Returns false for duplicate keys
  bool Put(Key &key, Value val);

  // Get the value specified by the key. We don't need to lock anything
  bool Get(const Key &key, Value &val) const;

  // Functions
 private:
  inline size_t Probe(size_t bucket_itr) const {
    return bucket_itr + PROBE_STEP_SIZE;
  }

  inline size_t GetHash(const Key &key) const {
    return hasher_(key) % num_slots_;
  }

  inline size_t RoundUp(size_t n) const {
    size_t num_pages = (n * sizeof(Bucket) + PAGE_SIZE - 1) / PAGE_SIZE;
    return num_pages * PAGE_SIZE / sizeof(Bucket);
  }

  // Members
 private:
  bool interleave_memory_;

  Bucket *buckets_ = nullptr;
  Spinlock *locks_ = nullptr;

  Hash hasher_;
  Pred equal_fct_;

  size_t num_slots_ = 0;
  size_t num_buckets_ = 0;
};

}  // namespace executor
}  // namespace peloton

