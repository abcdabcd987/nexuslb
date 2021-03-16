#include "nexus/common/rps_meter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>

#include "nexus/common/time_util.h"

using namespace nexus;
using namespace std::chrono;

TEST(RpsMeter, Test) {
  constexpr double kWindowSpanSecond = 10;
  constexpr size_t kHistoryLength = 6;
  auto start = Clock::now();
  RpsMeter rps_meter(kWindowSpanSecond, kHistoryLength, start);
  rps_meter.Hit(start + seconds(1));
  rps_meter.Hit(start + seconds(11));
  rps_meter.Hit(start + seconds(21));
  rps_meter.Hit(start + seconds(31));
  rps_meter.Hit(start + seconds(41));
  rps_meter.Hit(start + seconds(51));
  auto avgstd = rps_meter.Get(start + seconds(59));
  ASSERT_TRUE(avgstd.has_value());
  ASSERT_EQ(avgstd->avg, 0.1);
  ASSERT_EQ(avgstd->std, 0.0);

  avgstd = rps_meter.Get(start + seconds(69));
  ASSERT_TRUE(avgstd.has_value());
  double avg = 5.0 / 6;
  ASSERT_EQ(avgstd->avg, avg / 10);
  ASSERT_EQ(avgstd->std,
            std::sqrt(((1 - avg) * (1 - avg) * 5 + avg * avg) / 5) / 10);

  avgstd = rps_meter.Get(start + seconds(79));
  ASSERT_TRUE(avgstd.has_value());
  avg = 4.0 / 6;
  ASSERT_EQ(avgstd->avg, avg / 10);
  ASSERT_EQ(avgstd->std,
            std::sqrt(((1 - avg) * (1 - avg) * 4 + avg * avg * 2) / 5) / 10);

  avgstd = rps_meter.Get(start + seconds(69));
  ASSERT_TRUE(avgstd.has_value());
  avg = 4.0 / 5;
  ASSERT_EQ(avgstd->avg, avg / 10);
  ASSERT_EQ(avgstd->std,
            std::sqrt(((1 - avg) * (1 - avg) * 4 + avg * avg * 1) / 4) / 10);

  avgstd = rps_meter.Get(start + seconds(200));
  ASSERT_TRUE(avgstd.has_value());
  ASSERT_EQ(avgstd->avg, 0.0);
  ASSERT_EQ(avgstd->std, 0.0);

  avgstd = rps_meter.Get(start + seconds(59));
  ASSERT_FALSE(avgstd.has_value());
}
