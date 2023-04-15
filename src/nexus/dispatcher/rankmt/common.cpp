#include "nexus/dispatcher/rankmt/common.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <optional>
#include <string_view>

#include "nexus/dispatcher/batch_policy.h"

using nexus::dispatcher::rankmt::RankmtConfig;

DEFINE_string(rankmt_schedulable,
              RankmtConfig::Default().schedulable.ToString(),
              "Rankmt: condition for a Candidate to become schedulable. "
              "Options: kImmediately, kTargetBatchSize, kTargetQueuingDelay, "
              "kFrontrun, kLatest, kCredit");
DEFINE_string(rankmt_drop, RankmtConfig::Default().drop.ToString(),
              "Rankmt: Whether to drop head of queue during batching. "
              "Options: kDropTimeout, kWindowDrop, kWindowFCFS");
DEFINE_uint32(rankmt_dctrl, RankmtConfig::Default().ctrl_latency.count() / 1000,
              "Rankmt: control plane latency in microseconds.");
DEFINE_uint32(rankmt_ddata, RankmtConfig::Default().data_latency.count() / 1000,
              "Rankmt: data plane latency in microseconds.");
DEFINE_uint32(rankmt_dresp, RankmtConfig::Default().resp_latency.count() / 1000,
              "Rankmt: result latency in microseconds.");
DEFINE_uint32(rankmt_rpsmeter_rate,
              RankmtConfig::Default().rpsmeter_rate.count() / 1000000,
              "Rankmt: RpsMeter sample rate in milliseconds.");
DEFINE_uint64(rankmt_rpsmeter_window, RankmtConfig::Default().rpsmeter_window,
              "Rankmt: RpsMeter window length.");
DEFINE_uint32(
    rankmt_credit_reset_period, RankmtConfig::Default().credit_reset_period,
    "Rankmt: Credit-based scheduling: credit reset period multiplier.");
DEFINE_uint32(
    rankmt_credit_reset_value, RankmtConfig::Default().credit_reset_value,
    "Rankmt: Credit-based scheduling: credit reset value multiplier.");

namespace nexus {
namespace dispatcher {

constexpr const char* DropPolicy::ToString(DropPolicy c) {
  switch (c.value_) {
    case kDropTimeout:
      return "kDropTimeout";
    case kWindowDrop:
      return "kWindowDrop";
    case kWindowFCFS:
      return "kWindowFCFS";
  }
  LOG(FATAL) << "DropPolicy: unreachable";
}

constexpr const char* DropPolicy::ToString() const { return ToString(*this); }

constexpr std::optional<DropPolicy> DropPolicy::Parse(std::string_view s) {
  if (s == "kDropTimeout") return DropPolicy::kDropTimeout;
  if (s == "kWindowDrop") return DropPolicy::kWindowDrop;
  if (s == "kWindowFCFS") return DropPolicy::kWindowFCFS;
  return std::nullopt;
}

namespace rankmt {

constexpr const char* SchedulableCondition::ToString(SchedulableCondition c) {
  switch (c.value_) {
    case kImmediately:
      return "kImmediately";
    case kTargetBatchSize:
      return "kTargetBatchSize";
    case kTargetQueuingDelay:
      return "kTargetQueuingDelay";
    case kFrontrun:
      return "kFrontrun";
    case kLatest:
      return "kLatest";
    case kCredit:
      return "kCredit";
  }
  CHECK(false) << "unreachable";
}

constexpr const char* SchedulableCondition::ToString() const {
  return ToString(*this);
}

constexpr std::optional<SchedulableCondition> SchedulableCondition::Parse(
    std::string_view s) {
  if (s == "kImmediately") return SchedulableCondition::kImmediately;
  if (s == "kTargetBatchSize") return SchedulableCondition::kTargetBatchSize;
  if (s == "kTargetQueuingDelay")
    return SchedulableCondition::kTargetQueuingDelay;
  if (s == "kFrontrun") return SchedulableCondition::kFrontrun;
  if (s == "kLatest") return SchedulableCondition::kLatest;
  if (s == "kCredit") return SchedulableCondition::kCredit;
  return std::nullopt;
}

RankmtConfig RankmtConfig::FromFlags() {
  RankmtConfig rankmt;
  auto schedulable = SchedulableCondition::Parse(FLAGS_rankmt_schedulable);
  if (!schedulable.has_value()) {
    LOG(FATAL) << "Invalid value for --rankmt_schedulable";
  }
  auto drop = DropPolicy::Parse(FLAGS_rankmt_drop);
  if (!drop.has_value()) {
    LOG(FATAL) << "Invalid value for --rankmt_drop";
  }
  rankmt.schedulable = schedulable.value();
  rankmt.drop = drop.value();
  rankmt.ctrl_latency = std::chrono::microseconds(FLAGS_rankmt_dctrl);
  rankmt.data_latency = std::chrono::microseconds(FLAGS_rankmt_ddata);
  rankmt.resp_latency = std::chrono::microseconds(FLAGS_rankmt_dresp);
  rankmt.rpsmeter_rate = std::chrono::milliseconds(FLAGS_rankmt_rpsmeter_rate);
  rankmt.rpsmeter_window = FLAGS_rankmt_rpsmeter_window;
  rankmt.credit_reset_period = FLAGS_rankmt_credit_reset_period;
  rankmt.credit_reset_value = FLAGS_rankmt_credit_reset_value;
  return rankmt;
}

std::chrono::nanoseconds EstimateExecElapse(const ModelProfile& profile,
                                            uint32_t batch_size) {
  double micros = 0;
  micros += profile.GetPreprocessLatency();
  micros += profile.GetPostprocessLatency();
  micros += profile.GetForwardLatency(batch_size);
  return std::chrono::nanoseconds(static_cast<long>(micros * 1e3));
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
