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

namespace peloton {

namespace executor {

template <
    class Key,                    // The key
    class Value,                  // The value
    class Hash,                   // The hash function
    class Pred,                   // The comparator
    const size_t BUCKET_SIZE,     // The bucket size for lock striping
    const size_t PROBE_STEP_SIZE  // The number of buckets per step for probe
    >
class Hashmap {

  // Classes and types
  typedef std::pair<Key, Value> KVPair;

  enum Status : char {
    FAIL_DUPLICATE_KEY,
    FAIL_BUCKET_FULL,
    SUCCESS,
  };

  class Bucket {
   public:
    std::array<KVPair, BUCKET_SIZE> kv_pairs;
    std::bitset<BUCKET_SIZE> occupied;

    // Insert a kv pair to this bucket
    Status Insert(KVPair &pair) {
      for (size_t i = 0; i < BUCKET_SIZE; i++) {
        if (occupied[i] == false) {
          kv_pairs[i] = pair;
          occupied[i] = true;
          return SUCCESS;

        } else {
          // TODO Check duplicates
          return FAIL_DUPLICATE_KEY;
        }
      }
      return FAIL_BUCKET_FULL;
    }

    // Get a value from this bucket
    Status Get(Key &key, Value &val) {
      (void)key;
      (void)val;
      return SUCCESS;
    }
  };

 public:
  void Reserve(size_t size) {
    // TODO compute the appropriate size
    num_buckets_ = (size + BUCKET_SIZE - 1) / BUCKET_SIZE;
    buckets_.resize(num_buckets_);
    locks_.resize(num_buckets_);
  }

  // Duplication is not checked during put.
  void Put(Key &key, Value &val) {
    KVPair kv_pair(key, val);
    size_t hash = GetHash(key);
    // TODO Check the status
    bool success = false;
    while (success == false) {
      locks_[hash].Lock();
      success = buckets_[hash].Insert(kv_pair);
      locks_[hash].Unlock();
      hash = Probe(hash);
    }
  }

  // Get the value specified by the key. No lock is acquired
  bool Get(Key &key, Value &val) const { return false; }

  // Functions
 private:
  inline size_t Probe(size_t hash) {
    return (hash + PROBE_STEP_SIZE) % num_buckets_;
  }

  inline size_t GetHash(Key &key) { return hasher_(key) % num_buckets_; }

  // Members
 private:
  std::vector<Bucket> buckets_;
  std::vector<Spinlock> locks_;
  Hash hasher_;
  size_t num_buckets_ = 0;
};

}  // namespace executor
}  // namespace peloton
