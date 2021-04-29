#ifndef NEXUS_COMMON_VALUE_RANKED_SPLAY_MAP_H_
#define NEXUS_COMMON_VALUE_RANKED_SPLAY_MAP_H_

#include <algorithm>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "nexus/common/functional.h"

namespace nexus {

template <typename Key, typename Value, typename CompareKeyFn = Identity>
class ValueRankedSplayMap {
 public:
  struct RefPair {
    std::reference_wrapper<const Key> key;
    std::reference_wrapper<const Value> value;
  };
  using CompareKey =
      typename std::invoke_result<CompareKeyFn, const Value&>::type;

  ValueRankedSplayMap();
  ~ValueRankedSplayMap();
  void Upsert(Key key, Value value);      // amortized O(log(n))
  void Remove(const Key& key);            // amortized O(log(n))
  size_t Size() const;                    // O(1)
  bool Contains(const Key& key) const;    // O(1)
  const Value& GetByKey(const Key& key);  // amortized O(log(n))
  RefPair GetByRank(size_t rank);         // amortized O(log(n))
  size_t Rank(const Key& key);            // amortized O(log(n))

  // sum([1 for (key, value) in map_ if value <= ck])
  size_t CountLessEqual(const CompareKey& ck);  // amortized O(log(n))

 private:
  struct Node {
    Node(Key key, Value value);

    Key key;
    Value value;
    Node* parent;
    Node* child[2];
    size_t size;
  };

  static size_t NodeSize(Node* x);
  void UpdateNodeSize(Node* x);
  void RotateNode(Node* x, int c);
  void SplayNode(Node* x, const Node* goal = nullptr);
  Node* GetMinNode(Node* x);
  Node* GetMaxNode(Node* x);
  void RemoveNode(Node* x);
  void InsertNode(Node* x);
  Node* GetNthNode(size_t n);

  CompareKeyFn compare_key_fn_;
  Node* root_;
  std::unordered_map<Key, Node*> map_;
};

//============ Implementation ============

template <typename Key, typename Value, typename CompareKeyFn>
ValueRankedSplayMap<Key, Value, CompareKeyFn>::Node::Node(Key key, Value value)
    : key(std::move(key)), value(std::move(value)), parent(nullptr), size(1) {
  child[0] = child[1] = nullptr;
}

template <typename Key, typename Value, typename CompareKeyFn>
ValueRankedSplayMap<Key, Value, CompareKeyFn>::ValueRankedSplayMap()
    : compare_key_fn_(), root_(nullptr) {}

template <typename Key, typename Value, typename CompareKeyFn>
ValueRankedSplayMap<Key, Value, CompareKeyFn>::~ValueRankedSplayMap() {
  for (auto& pair : map_) delete pair.second;
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedSplayMap<Key, Value, CompareKeyFn>::Upsert(Key key,
                                                           Value value) {
  if (Contains(key)) {
    Remove(key);
  }
  auto* x = new Node(std::move(key), std::move(value));
  InsertNode(x);
  map_[x->key] = x;
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedSplayMap<Key, Value, CompareKeyFn>::Remove(const Key& key) {
  auto iter = map_.find(key);
  if (iter != map_.end()) {
    RemoveNode(iter->second);
    delete iter->second;
    map_.erase(iter);
  }
}

template <typename Key, typename Value, typename CompareKeyFn>
size_t ValueRankedSplayMap<Key, Value, CompareKeyFn>::Size() const {
  return map_.size();
}

template <typename Key, typename Value, typename CompareKeyFn>
bool ValueRankedSplayMap<Key, Value, CompareKeyFn>::Contains(
    const Key& key) const {
  auto iter = map_.find(key);
  return iter != map_.end();
}

template <typename Key, typename Value, typename CompareKeyFn>
const Value& ValueRankedSplayMap<Key, Value, CompareKeyFn>::GetByKey(
    const Key& key) {
  auto* x = map_.at(key);
  SplayNode(x);
  return x->value;
}

template <typename Key, typename Value, typename CompareKeyFn>
typename ValueRankedSplayMap<Key, Value, CompareKeyFn>::RefPair
ValueRankedSplayMap<Key, Value, CompareKeyFn>::GetByRank(size_t rank) {
  if (rank >= map_.size()) {
    throw std::out_of_range("rank >= Size()");
  }
  Node* x = GetNthNode(rank);
  return {std::cref(x->key), std::cref(x->value)};
}

template <typename Key, typename Value, typename CompareKeyFn>
size_t ValueRankedSplayMap<Key, Value, CompareKeyFn>::Rank(const Key& key) {
  Node* x = map_.at(key);
  SplayNode(x);
  return NodeSize(x->child[0]);
}

template <typename Key, typename Value, typename CompareKeyFn>
size_t ValueRankedSplayMap<Key, Value, CompareKeyFn>::CountLessEqual(
    const CompareKey& ck) {
  Node *x = root_, *y = nullptr;
  while (x) {
    if (compare_key_fn_(x->value) <= ck) {
      y = x;
      x = x->child[1];
    } else {
      x = x->child[0];
    }
  }
  SplayNode(y);
  return y ? NodeSize(y->child[0]) + 1 : 0;
}

//============ Splay Tree ============
// Appearently, I no longer know how to write a Splay tree :-(
// Therefore, I copied the code I wrote in high school     :-)
// See: https://oi.abcdabcd987.com/bzoj2329/
//      https://oi.abcdabcd987.com/hdu2871/

template <typename Key, typename Value, typename CompareKeyFn>
size_t ValueRankedSplayMap<Key, Value, CompareKeyFn>::NodeSize(Node* x) {
  return x ? x->size : 0;
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedSplayMap<Key, Value, CompareKeyFn>::UpdateNodeSize(Node* x) {
  x->size = 1 + NodeSize(x->child[0]) + NodeSize(x->child[1]);
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedSplayMap<Key, Value, CompareKeyFn>::RotateNode(Node* x, int c) {
  Node* y = x->parent;
  y->child[c ^ 1] = x->child[c];
  if (x->child[c]) x->child[c]->parent = y;
  x->parent = y->parent;
  if (x->parent) x->parent->child[y == y->parent->child[1]] = x;
  y->parent = x;
  x->child[c] = y;
  UpdateNodeSize(y);
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedSplayMap<Key, Value, CompareKeyFn>::SplayNode(
    Node* x, const Node* goal) {
  if (!x) return;
  for (Node* y; (y = x->parent) != goal;) {
    if (y->parent == goal) {
      RotateNode(x, x == y->child[0]);
      break;
    }
    const int c = y == y->parent->child[0];
    if (x == y->child[c]) {
      RotateNode(x, c ^ 1);
      RotateNode(x, c);
    } else {
      RotateNode(y, c);
      RotateNode(x, c);
    }
  }
  UpdateNodeSize(x);
  if (!goal) root_ = x;
}

template <typename Key, typename Value, typename CompareKeyFn>
typename ValueRankedSplayMap<Key, Value, CompareKeyFn>::Node*
ValueRankedSplayMap<Key, Value, CompareKeyFn>::GetMinNode(Node* x) {
  while (x->child[0]) x = x->child[0];
  return x;
}

template <typename Key, typename Value, typename CompareKeyFn>
typename ValueRankedSplayMap<Key, Value, CompareKeyFn>::Node*
ValueRankedSplayMap<Key, Value, CompareKeyFn>::GetMaxNode(Node* x) {
  while (x->child[1]) x = x->child[1];
  return x;
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedSplayMap<Key, Value, CompareKeyFn>::RemoveNode(Node* x) {
  SplayNode(x);
  Node* succ = x->child[1] ? GetMinNode(x->child[1]) : nullptr;
  Node* prev = x->child[0] ? GetMaxNode(x->child[0]) : nullptr;
  SplayNode(succ);
  SplayNode(prev, succ);
  if (prev) {
    prev->child[1] = nullptr;
    --prev->size;
    if (succ) --succ->size;
  } else if (succ) {
    succ->child[0] = nullptr;
  } else {
    root_ = nullptr;
  }
}

template <typename Key, typename Value, typename CompareKeyFn>
void ValueRankedSplayMap<Key, Value, CompareKeyFn>::InsertNode(Node* x) {
  if (!root_) {
    root_ = x;
    return;
  }
  for (Node* p = root_; p;) {
    x->parent = p;
    p = p->child[compare_key_fn_(p->value) < compare_key_fn_(x->value) ? 1 : 0];
  }
  int c = compare_key_fn_(x->parent->value) < compare_key_fn_(x->value) ? 1 : 0;
  x->parent->child[c] = x;
  SplayNode(x);
}

template <typename Key, typename Value, typename CompareKeyFn>
typename ValueRankedSplayMap<Key, Value, CompareKeyFn>::Node*
ValueRankedSplayMap<Key, Value, CompareKeyFn>::GetNthNode(size_t n) {
  Node* x = root_;
  while (NodeSize(x->child[0]) != n) {
    if (NodeSize(x->child[0]) > n) {
      x = x->child[0];
    } else {
      n -= NodeSize(x->child[0]) + 1;
      x = x->child[1];
    }
  }
  SplayNode(x);
  return x;
}

}  // namespace nexus

#endif
