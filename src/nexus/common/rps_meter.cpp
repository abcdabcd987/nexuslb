#include "nexus/common/rps_meter.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <optional>
#include <string>

#include "nexus/common/util.h"

DEFINE_string(hack_rpsmeter, "", "");

namespace nexus {

namespace {
int64_t ns(TimePoint time) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             time.time_since_epoch())
      .count();
}

std::atomic<size_t> cnt_objects = 0;

}  // namespace

RpsMeter::RpsMeter(double window_span_second, size_t history_length,
                   TimePoint start_time)
    : window_span_ns_(static_cast<int64_t>(window_span_second * 1e9)),
      earliest_time_ns_(ns(start_time)),
      counters_(history_length, 0) {
#ifdef HACK_RPSMETER
  size_t idx = cnt_objects++;
  std::vector<std::string> tokens;
  SplitString(FLAGS_hack_rpsmeter, ',', &tokens);
  LOG_IF(FATAL, idx >= tokens.size())
      << "Not enough entries of `-hack_rpsmeter` specified.";
  hack_rps_ = std::stod(tokens[idx]);
  LOG(WARNING) << "RpsMeter created with hack_rps_=" << hack_rps_;
#endif
}

void RpsMeter::PopLeft(int64_t time_ns) {
  while (earliest_time_ns_ + counters_.size() * window_span_ns_ <= time_ns) {
    counters_.pop_front();
    counters_.push_back(0);
    earliest_time_ns_ += window_span_ns_;
  }
}

size_t RpsMeter::Index(int64_t time_ns) {
  return (time_ns - earliest_time_ns_) / window_span_ns_;
}

void RpsMeter::Hit(TimePoint time) {
  auto time_ns = ns(time);
  if (time_ns < earliest_time_ns_) {
    LOG(WARNING) << "Ignoring past time. time: " << time_ns
                 << ", earliest:" << earliest_time_ns_;
    return;
  }
  PopLeft(time_ns);
  auto idx = Index(time_ns);
  CHECK_LT(idx, counters_.size());
  ++counters_[idx];
}

std::optional<AvgStd> RpsMeter::Get(TimePoint now) {
#ifdef HACK_RPSMETER
  // FIXME
  return {{hack_rps_, 0.0}};
#endif

  auto time_ns = ns(now);
  if (time_ns < earliest_time_ns_) {
    return std::nullopt;
  }
  PopLeft(time_ns);
  auto idx = Index(time_ns);
  CHECK_LT(idx, counters_.size());

  double avg = 0;
  for (size_t i = 0; i <= idx; ++i) avg += counters_[i];
  avg /= (idx + 1);
  double var = 0;
  for (size_t i = 0; i <= idx; ++i)
    var += (counters_[i] - avg) * (counters_[i] - avg);
  double std = idx ? std::sqrt(var / idx) : 0;
  double span = window_span_ns_ * 1e-9;
  return {{avg / span, std / span}};
}

}  // namespace nexus
