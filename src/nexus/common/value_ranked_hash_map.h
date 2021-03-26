#ifndef NEXUS_COMMON_VALUE_RANKED_HASH_MAP_H_
#define NEXUS_COMMON_VALUE_RANKED_HASH_MAP_H_

#include <algorithm>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "nexus/common/functional.h"

namespace nexus {

template <typename Key, typename Value, typename CompareKeyFn = Identity>
class ValueRankedHashMap {
 public:
  struct RefPair {
    std::reference_wrapper<const Key> key;
    std::reference_wrapper<const Value> value;
  };
  using CompareKey =
      typename std::invoke_result<CompareKeyFn, const Value&>::type;

  void Upsert(Key key, Value value);            // O(1)
  void Remove(const Key& key);                  // O(1)
  size_t Size() const;                          // O(1)
  bool Contains(const Key& key) const;          // O(1)
  const Value& GetByKey(const Key& key) const;  // O(1)
  RefPair GetByRank(size_t rank) const;         // O(n)
  size_t Rank(const Key& key) const;            // O(n)

  // sum([1 for (key, value) in map_ if value < ck])
  size_t CountLessEqual(const CompareKey& ck) const;  // O(n)

 private:
  using Map = std::unordered_map<Key, Value>;
  using MapIterator = typename Map::const_iterator;

  struct MapIteratorCompare {
    MapIteratorCompare();
    bool operator()(const MapIterator& lhs, const MapIterator& rhs);

    CompareKeyFn compare_key_fn;
  };

  Map map_;
  MapIteratorCompare comp_;
};

//============ Implementation ============

template <typename Key, typename Value, typename CompareKeyFn>
ValueRankedHashMap<Key, Value,
                   CompareKeyFn>::MapIteratorCompare::MapIteratorCompare()
    : compare_key_fn() {}

template <typename Key, typename Value, typename CompareKeyFn>
bool ValueRankedHashMap<Key, Value, CompareKeyFn>::MapIteratorCompare::
operator()(const MapIterator& lhs, const MapIterator& rhs) {
  return compare_key_fn(lhs->second) < compare_key_fn(rhs->second);
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedHashMap<Key, Value, CompareKeyFn>::Upsert(Key key,
                                                          Value value) {
  map_[key] = value;
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedHashMap<Key, Value, CompareKeyFn>::Remove(const Key& key) {
  map_.erase(key);
}

template <typename Key, typename Value, typename CompareKeyFn>
size_t ValueRankedHashMap<Key, Value, CompareKeyFn>::Size() const {
  return map_.size();
}

template <typename Key, typename Value, typename CompareKeyFn>
bool ValueRankedHashMap<Key, Value, CompareKeyFn>::Contains(
    const Key& key) const {
  auto map_iter = map_.find(key);
  return map_iter != map_.end();
}

template <typename Key, typename Value, typename CompareKeyFn>
const Value& ValueRankedHashMap<Key, Value, CompareKeyFn>::GetByKey(
    const Key& key) const {
  return map_.at(key);
}

template <typename Key, typename Value, typename CompareKeyFn>
typename ValueRankedHashMap<Key, Value, CompareKeyFn>::RefPair
ValueRankedHashMap<Key, Value, CompareKeyFn>::GetByRank(size_t rank) const {
  if (rank >= map_.size()) {
    throw std::out_of_range("rank > Size()");
  }
  std::vector<MapIterator> v;
  v.reserve(map_.size());
  for (auto iter = map_.cbegin(); iter != map_.cend(); ++iter) {
    v.push_back(iter);
  }
  std::nth_element(v.begin(), v.begin() + rank, v.end(), comp_);
  return {std::cref(v[rank]->first), std::cref(v[rank]->second)};
}

template <typename Key, typename Value, typename CompareKeyFn>
size_t ValueRankedHashMap<Key, Value, CompareKeyFn>::Rank(
    const Key& key) const {
  if (!Contains(key)) {
    throw std::out_of_range("!Contains(key)");
  }
  const auto& value = map_.at(key);
  size_t rank = 0;
  for (const auto& pair : map_) {
    if (comp_.compare_key_fn(pair.second) < comp_.compare_key_fn(value)) {
      ++rank;
    }
  }
  return rank;
}

template <typename Key, typename Value, typename CompareKeyFn>
size_t ValueRankedHashMap<Key, Value, CompareKeyFn>::CountLessEqual(
    const CompareKey& ck) const {
  size_t cnt = 0;
  for (const auto& pair : map_) {
    if (comp_.compare_key_fn(pair.second) <= ck) {
      ++cnt;
    }
  }
  return cnt;
}

}  // namespace nexus

#endif
