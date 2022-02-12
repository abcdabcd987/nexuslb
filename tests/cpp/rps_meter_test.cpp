#include "nexus/common/rps_meter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <numeric>

#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"

using namespace nexus;

AvgStd CalcAvgStd(const std::deque<double>& v) {
  double sum = std::accumulate(v.begin(), v.end(), 0.0);
  double avg = sum / v.size();
  double sqr_sum = std::inner_product(v.begin(), v.end(), v.begin(), 0.0);
  double std = std::sqrt(sqr_sum / v.size() - avg * avg);
  return AvgStd(avg, std);
}

TEST(RpsMeter, Test) {
  std::vector<int> cnt = {100, 800, 119, 120, 532, 231,
                          215, 643, 843, 134, 534, 124};
  size_t window = 3;
  long gap_ms = 50;

  RpsMeter rps_meter(window);
  std::deque<double> v;
  ASSERT_FALSE(rps_meter.Get().has_value());

  TimePoint time(std::chrono::nanoseconds(0xabcdabcd987LL));
  rps_meter.SealCounter(time);
  ASSERT_FALSE(rps_meter.Get().has_value());

  rps_meter.SealCounter(time);
  ASSERT_FALSE(rps_meter.Get().has_value());

  for (size_t i = 0; i < cnt.size(); ++i) {
    int hits = cnt[i];
    for (int j = 0; j < hits; ++j) {
      rps_meter.Hit();
    }
    rps_meter.SealCounter(time + std::chrono::milliseconds(gap_ms * (i + 1)));
    auto x = rps_meter.Get();
    ASSERT_TRUE(x.has_value());

    if (v.size() == window) {
      v.pop_front();
    }
    v.push_back(hits);
    auto ref = CalcAvgStd(v);

    ASSERT_FLOAT_EQ(x->avg, ref.avg / (gap_ms * 1e-3));
    ASSERT_FLOAT_EQ(x->std, ref.std / (gap_ms * 1e-3));
  }
}
