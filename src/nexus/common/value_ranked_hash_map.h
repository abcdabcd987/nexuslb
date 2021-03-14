#ifndef NEXUS_COMMON_VALUE_RANKED_HASH_MAP_H_
#define NEXUS_COMMON_VALUE_RANKED_HASH_MAP_H_

#include <algorithm>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace nexus {

template <typename Key, typename Value, typename Compare = std::less<Value>>
class ValueRankedHashMap {
 public:
  struct RefPair {
    std::reference_wrapper<const Key> key;
    std::reference_wrapper<const Value> value;
  };

  void Upsert(Key key, Value value);            // O(1)
  void Remove(const Key& key);                  // O(1)
  size_t Size() const;                          // O(1)
  bool Contains(const Key& key) const;          // O(1)
  const Value& GetByKey(const Key& key) const;  // O(1)
  RefPair GetByRank(size_t rank) const;         // O(n)
  size_t Rank(const Key& key) const;            // O(n)

 private:
  using Map = std::unordered_map<Key, Value>;
  using MapIterator = typename Map::const_iterator;

  struct MapIteratorCompare {
    MapIteratorCompare();
    bool operator()(const MapIterator& lhs, const MapIterator& rhs);

    Compare comp_;
  };

  Map map_;
  MapIteratorCompare comp_;
};

//============ Implementation ============

template <typename Key, typename Value, typename Compare>
ValueRankedHashMap<Key, Value,
                   Compare>::MapIteratorCompare::MapIteratorCompare()
    : comp_(Compare{}) {}

template <typename Key, typename Value, typename Compare>
bool ValueRankedHashMap<Key, Value, Compare>::MapIteratorCompare::operator()(
    const MapIterator& lhs, const MapIterator& rhs) {
  return comp_(lhs->second, rhs->second);
}

template <typename Key, typename Value, typename Compare>
void ValueRankedHashMap<Key, Value, Compare>::Upsert(Key key, Value value) {
  map_[key] = value;
}

template <typename Key, typename Value, typename Compare>
void ValueRankedHashMap<Key, Value, Compare>::Remove(const Key& key) {
  map_.erase(key);
}

template <typename Key, typename Value, typename Compare>
size_t ValueRankedHashMap<Key, Value, Compare>::Size() const {
  return map_.size();
}

template <typename Key, typename Value, typename Compare>
bool ValueRankedHashMap<Key, Value, Compare>::Contains(const Key& key) const {
  auto map_iter = map_.find(key);
  return map_iter != map_.end();
}

template <typename Key, typename Value, typename Compare>
const Value& ValueRankedHashMap<Key, Value, Compare>::GetByKey(
    const Key& key) const {
  return map_.at(key);
}

template <typename Key, typename Value, typename Compare>
typename ValueRankedHashMap<Key, Value, Compare>::RefPair
ValueRankedHashMap<Key, Value, Compare>::GetByRank(size_t rank) const {
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

template <typename Key, typename Value, typename Compare>
size_t ValueRankedHashMap<Key, Value, Compare>::Rank(const Key& key) const {
  if (!Contains(key)) {
    throw std::out_of_range("!Contains(key)");
  }
  const auto& value = map_.at(key);
  size_t rank = 0;
  for (const auto& pair : map_) {
    if ((comp_.comp_)(pair.second, value)) {
      ++rank;
    }
  }
  return rank;
}

}  // namespace nexus

#endif
