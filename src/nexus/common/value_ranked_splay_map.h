#ifndef NEXUS_COMMON_VALUE_RANKED_SPLAY_MAP_H_
#define NEXUS_COMMON_VALUE_RANKED_SPLAY_MAP_H_

#include <algorithm>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace nexus {

template <typename Key, typename Value, typename Compare = std::less<Value>>
class ValueRankedSplayMap {
 public:
  struct RefPair {
    std::reference_wrapper<const Key> key;
    std::reference_wrapper<const Value> value;
  };

  ValueRankedSplayMap();
  ~ValueRankedSplayMap();
  void Upsert(Key key, Value value);      // O(log(n))
  void Remove(const Key& key);            // O(log(n))
  size_t Size() const;                    // O(1)
  bool Contains(const Key& key) const;    // O(1)
  const Value& GetByKey(const Key& key);  // O(1)
  RefPair GetByRank(size_t rank);         // O(log(n))
  size_t Rank(const Key& key);            // O(log(n))

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

  Compare comp_;
  Node* root_;
  std::unordered_map<Key, Node*> map_;
};

//============ Implementation ============

template <typename Key, typename Value, typename Compare>
ValueRankedSplayMap<Key, Value, Compare>::Node::Node(Key key, Value value)
    : key(std::move(key)), value(std::move(value)), parent(nullptr), size(1) {
  child[0] = child[1] = nullptr;
}

template <typename Key, typename Value, typename Compare>
ValueRankedSplayMap<Key, Value, Compare>::ValueRankedSplayMap()
    : comp_(), root_(nullptr) {}

template <typename Key, typename Value, typename Compare>
ValueRankedSplayMap<Key, Value, Compare>::~ValueRankedSplayMap() {
  for (auto& pair : map_) delete pair.second;
}

template <typename Key, typename Value, typename Compare>
void ValueRankedSplayMap<Key, Value, Compare>::Upsert(Key key, Value value) {
  if (Contains(key)) {
    Remove(key);
  }
  auto* x = new Node(std::move(key), std::move(value));
  InsertNode(x);
  map_[x->key] = x;
}

template <typename Key, typename Value, typename Compare>
void ValueRankedSplayMap<Key, Value, Compare>::Remove(const Key& key) {
  auto iter = map_.find(key);
  if (iter != map_.end()) {
    RemoveNode(iter->second);
    delete iter->second;
    map_.erase(iter);
  }
}

template <typename Key, typename Value, typename Compare>
size_t ValueRankedSplayMap<Key, Value, Compare>::Size() const {
  return map_.size();
}

template <typename Key, typename Value, typename Compare>
bool ValueRankedSplayMap<Key, Value, Compare>::Contains(const Key& key) const {
  auto iter = map_.find(key);
  return iter != map_.end();
}

template <typename Key, typename Value, typename Compare>
const Value& ValueRankedSplayMap<Key, Value, Compare>::GetByKey(
    const Key& key) {
  auto* x = map_.at(key);
  SplayNode(x);
  return x->value;
}

template <typename Key, typename Value, typename Compare>
typename ValueRankedSplayMap<Key, Value, Compare>::RefPair
ValueRankedSplayMap<Key, Value, Compare>::GetByRank(size_t rank) {
  if (rank >= map_.size()) {
    throw std::out_of_range("rank > Size()");
  }
  Node* x = GetNthNode(rank);
  return {std::cref(x->key), std::cref(x->value)};
}

template <typename Key, typename Value, typename Compare>
size_t ValueRankedSplayMap<Key, Value, Compare>::Rank(const Key& key) {
  Node* x = map_.at(key);
  SplayNode(x);
  return NodeSize(x->child[0]);
}

//============ Splay Tree ============
// Appearently, I no longer know how to write a Splay tree :-(
// Therefore, I copied the code I wrote in high school     :-)
// See: https://oi.abcdabcd987.com/bzoj2329/
//      https://oi.abcdabcd987.com/hdu2871/

template <typename Key, typename Value, typename Compare>
size_t ValueRankedSplayMap<Key, Value, Compare>::NodeSize(Node* x) {
  return x ? x->size : 0;
}

template <typename Key, typename Value, typename Compare>
void ValueRankedSplayMap<Key, Value, Compare>::UpdateNodeSize(Node* x) {
  x->size = 1 + NodeSize(x->child[0]) + NodeSize(x->child[1]);
}

template <typename Key, typename Value, typename Compare>
void ValueRankedSplayMap<Key, Value, Compare>::RotateNode(Node* x, int c) {
  Node* y = x->parent;
  y->child[c ^ 1] = x->child[c];
  if (x->child[c]) x->child[c]->parent = y;
  x->parent = y->parent;
  if (x->parent) x->parent->child[y == y->parent->child[1]] = x;
  y->parent = x;
  x->child[c] = y;
  UpdateNodeSize(y);
}

template <typename Key, typename Value, typename Compare>
void ValueRankedSplayMap<Key, Value, Compare>::SplayNode(Node* x,
                                                         const Node* goal) {
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

template <typename Key, typename Value, typename Compare>
typename ValueRankedSplayMap<Key, Value, Compare>::Node*
ValueRankedSplayMap<Key, Value, Compare>::GetMinNode(Node* x) {
  while (x->child[0]) x = x->child[0];
  return x;
}

template <typename Key, typename Value, typename Compare>
typename ValueRankedSplayMap<Key, Value, Compare>::Node*
ValueRankedSplayMap<Key, Value, Compare>::GetMaxNode(Node* x) {
  while (x->child[1]) x = x->child[1];
  return x;
}

template <typename Key, typename Value, typename Compare>
void ValueRankedSplayMap<Key, Value, Compare>::RemoveNode(Node* x) {
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

template <typename Key, typename Value, typename Compare>
void ValueRankedSplayMap<Key, Value, Compare>::InsertNode(Node* x) {
  if (!root_) {
    root_ = x;
    return;
  }
  for (Node* p = root_; p;) {
    x->parent = p;
    p = p->child[comp_(p->value, x->value) ? 1 : 0];
  }
  x->parent->child[comp_(x->parent->value, x->value) ? 1 : 0] = x;
  SplayNode(x);
}

template <typename Key, typename Value, typename Compare>
typename ValueRankedSplayMap<Key, Value, Compare>::Node*
ValueRankedSplayMap<Key, Value, Compare>::GetNthNode(size_t n) {
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
