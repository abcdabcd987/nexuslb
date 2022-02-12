#include "nexus/common/rps_meter.h"

#include <glog/logging.h>

#include <cmath>

namespace nexus {

RpsMeter::RpsMeter(size_t window_size) : size_(window_size) {
  window_.reserve(size_);
}

void RpsMeter::Hit() { ++counter_; }

void RpsMeter::SealCounter(TimePoint now) {
  CHECK(now >= counter_start_at_) << "Time goes backwards.";
  auto last_time = counter_start_at_;
  counter_start_at_ = now;

  // Wait until the first request arrives.
  if (window_.empty() && counter_ == 0) {
    return;
  }

  // Skip the first seal.
  if (last_time.time_since_epoch().count() == 0) {
    counter_ = 0;
    return;
  }

  // Maintain mean and n*variance
  double rps = counter_ / ((now - last_time).count() * 1e-9);
  if (window_.size() == size_) {
    double old = window_[idx_];
    double mean = avg_ + (rps - old) / size_;
    nvar_ += (rps - old) * (rps - mean + old - avg_);
    avg_ = mean;
  } else if (window_.size() == 0) {
    avg_ = rps;
    nvar_ = 0;
  } else {
    double mean = avg_ + (rps - avg_) / (window_.size() + 1);
    nvar_ += (rps - mean) * (rps - avg_);
    avg_ = mean;
  }

  // Maintain window
  if (window_.size() == size_) {
    window_[idx_] = rps;
  } else {
    window_.push_back(rps);
  }
  idx_ = (idx_ + 1) % size_;
  counter_ = 0;

  // Maintain stddev
  std_ = std::sqrt(nvar_ / window_.size());
}

std::optional<AvgStd> RpsMeter::Get() {
  if (!window_.empty()) {
    return AvgStd(avg_, std_);
  }
  return std::nullopt;
}

}  // namespace nexus
