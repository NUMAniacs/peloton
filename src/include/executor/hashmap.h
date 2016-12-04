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
  void Reserve(size_t size) {
    // TODO compute the appropriate size
    num_buckets_ = (size + BUCKET_SIZE - 1) / BUCKET_SIZE;
    buckets_.resize(num_buckets_);
    locks_.resize(num_buckets_);
  }

  // Returns false for duplicate keys
  bool Put(Key &key, Value val) {
    size_t hash = GetHash(key);
    while (true) {
      locks_[hash].Lock();
      Bucket &bucket = buckets_[hash];
      for (size_t i = 0; i < BUCKET_SIZE; i++) {

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
          locks_[hash].Unlock();
          return true;
        } else {

          // Duplicate keys
          if (equal_fct_(key, bucket.kv_pairs[i].first)) {

            // Unlock before return
            locks_[hash].Unlock();
            return false;
          }
        }
      }

      // Unlock before return
      locks_[hash].Unlock();
      hash = Probe(hash);
    }
    return false;
  }

  // Get the value specified by the key.
  // XXX Do we need locks for GET()?
  bool Get(const Key &key, Value &val) const {
    size_t hash = GetHash(key);
    while (true) {
      const Bucket &bucket = buckets_[hash];
      for (size_t i = 0; i < BUCKET_SIZE; i++) {
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
      hash = Probe(hash);
    }
    return false;
  }

  // Functions
 private:
  inline size_t Probe(size_t hash) const {
    return (hash + PROBE_STEP_SIZE) % num_buckets_;
  }

  inline size_t GetHash(const Key &key) const {
    return hasher_(key) % num_buckets_;
  }

  // Members
 private:
  std::vector<Bucket> buckets_;
  std::vector<Spinlock> locks_;

  Hash hasher_;
  Pred equal_fct_;

  size_t num_buckets_ = 0;
};

}  // namespace executor
}  // namespace peloton
