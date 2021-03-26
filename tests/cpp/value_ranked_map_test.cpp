#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <functional>
#include <limits>
#include <ostream>
#include <random>
#include <unordered_map>

#include "nexus/common/value_ranked_hash_map.h"
#include "nexus/common/value_ranked_splay_map.h"

namespace {

class MyInt {
 public:
  MyInt() : value_(0) {}
  explicit MyInt(int value) : value_(value) {}
  int value() const { return value_; }
  friend bool operator==(const MyInt& lhs, const MyInt& rhs) {
    return lhs.value() == rhs.value();
  }

 private:
  int value_;
};

std::ostream& operator<<(std::ostream& os, const MyInt& obj) {
  return os << "MyInt(" << obj.value() << ")";
}

struct MyIntCompareKeyFn {
  int operator()(const MyInt& obj) const { return obj.value(); }
};

}  // namespace

namespace std {
template <>
struct hash<MyInt> {
  std::size_t operator()(const MyInt& obj) const noexcept {
    return std::hash<int>{}(obj.value());
  }
};
}  // namespace std

TEST(ValueRankedMapTest, RandomizedTest) {
  using Key = int;
  using Value = MyInt;
  using Uniform = std::uniform_int_distribution<int>;
  constexpr int kSwitchInsert = 2;
  constexpr int kSwitchUpdate = 2 + kSwitchInsert;
  constexpr int kSwitchRemove = 2 + kSwitchUpdate;
  constexpr int kSwitchSize = 1 + kSwitchRemove;
  constexpr int kSwitchContainsTrue = 1 + kSwitchSize;
  constexpr int kSwitchContainsNonExist = 1 + kSwitchContainsTrue;
  constexpr int kSwitchContainsRemoved = 1 + kSwitchContainsNonExist;
  constexpr int kSwitchGetByKey = 4 + kSwitchContainsRemoved;
  constexpr int kSwitchGetByRank = 4 + kSwitchGetByKey;
  constexpr int kSwitchRank = 4 + kSwitchGetByRank;
  constexpr int kSwitchCountLessEqual = 4 + kSwitchRank;
  constexpr int kNumOps = 3000000;

  std::mt19937 gen(0xabcdabcd987LL);
  Uniform op_d(0, kSwitchCountLessEqual - 1);
  nexus::ValueRankedHashMap<Key, Value, MyIntCompareKeyFn> naive;
  nexus::ValueRankedSplayMap<Key, Value, MyIntCompareKeyFn> splay;
  nexus::ValueRankedSplayMap<Key, Value, MyIntCompareKeyFn>::CompareKey k;
  int keys_begin = 0;
  int keys_end = 0;

  printf("Progress: 0/%d", kNumOps);
  fflush(stdout);
  auto st = std::chrono::system_clock::now();
  auto last = st;
  for (int i = 0; i < kNumOps;) {
    int op = op_d(gen);
    if (op < kSwitchInsert) {
      int key = keys_end++;
      int value = gen();
      naive.Upsert(key, MyInt(value));
      splay.Upsert(key, MyInt(value));

    } else if (op < kSwitchUpdate) {
      if (keys_end == keys_begin) continue;
      int key = Uniform{keys_begin, keys_end - 1}(gen);
      int value = gen();
      naive.Upsert(key, MyInt(value));
      splay.Upsert(key, MyInt(value));

    } else if (op < kSwitchRemove) {
      if (keys_end == keys_begin) continue;
      int key = keys_begin++;
      naive.Remove(key);
      splay.Remove(key);

    } else if (op < kSwitchSize) {
      ASSERT_EQ(naive.Size(), keys_end - keys_begin);
      ASSERT_EQ(naive.Size(), splay.Size());

    } else if (op < kSwitchContainsTrue) {
      if (keys_end == keys_begin) continue;
      int key = Uniform{keys_begin, keys_end - 1}(gen);
      ASSERT_TRUE(naive.Contains(key));
      ASSERT_TRUE(splay.Contains(key));

    } else if (op < kSwitchContainsNonExist) {
      int key = Uniform{keys_end, std::numeric_limits<Key>::max()}(gen);
      ASSERT_FALSE(naive.Contains(key));
      ASSERT_FALSE(splay.Contains(key));

    } else if (op < kSwitchContainsRemoved) {
      if (keys_begin == 0) continue;
      int key = Uniform{0, keys_begin - 1}(gen);
      ASSERT_FALSE(naive.Contains(key));
      ASSERT_FALSE(splay.Contains(key));

    } else if (op < kSwitchGetByKey) {
      if (keys_end == keys_begin) continue;
      int key = Uniform{keys_begin, keys_end - 1}(gen);
      const auto& vnaive = naive.GetByKey(key);
      const auto& vsplay = splay.GetByKey(key);
      ASSERT_EQ(vnaive, vsplay);

    } else if (op < kSwitchGetByRank) {
      if (keys_end == keys_begin) continue;
      int rank = Uniform{0, keys_end - keys_begin - 1}(gen);
      auto pair_naive = naive.GetByRank(rank);
      auto pair_splay = splay.GetByRank(rank);
      ASSERT_EQ(pair_naive.key, pair_naive.key);
      ASSERT_EQ(pair_naive.value, pair_naive.value);

    } else if (op < kSwitchRank) {
      if (keys_end == keys_begin) continue;
      int key = Uniform{keys_begin, keys_end - 1}(gen);
      ASSERT_EQ(naive.Rank(key), splay.Rank(key));

    } else if (op < kSwitchCountLessEqual) {
      int v = gen();
      ASSERT_EQ(naive.CountLessEqual(v), splay.CountLessEqual(v));

    } else {
      FAIL() << "Unreachable";
    }

    ++i;
    auto now = std::chrono::system_clock::now();
    if (i == kNumOps || now - last > std::chrono::milliseconds(31)) {
      double elapse =
          std::chrono::duration_cast<std::chrono::nanoseconds>(now - st)
              .count() /
          1e9;
      double eta = (kNumOps - i) / (i / elapse);
      printf("\rProgress: %d/%d (%d%%) Elapse %.3fs ETA %.3fs", i, kNumOps,
             i * 100 / kNumOps, elapse, eta);
      fflush(stdout);
      last = now;
    }
  }
  printf("\n");
}
