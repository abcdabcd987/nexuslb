#include "nexus_scheduler/model_handler.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <typeinfo>

#include "nexus/common/model_def.h"
#include "nexus_scheduler/fake_nexus_backend.h"

DEFINE_int32(count_interval, 1, "Interval to count number of requests in sec");

namespace nexus {
namespace app {

ModelHandler::ModelHandler(const std::string& model_session_id,
                           const FakeObjectAccessor* accessor)
    : model_session_id_(model_session_id),
      accessor_(*accessor),
      total_throughput_(0.),
      rand_gen_(rd_()) {
  ParseModelSession(model_session_id, &model_session_);
  counter_ =
      MetricRegistry::Singleton().CreateIntervalCounter(FLAGS_count_interval);
  running_ = true;
}

ModelHandler::~ModelHandler() {
  MetricRegistry::Singleton().RemoveMetric(counter_);
  running_ = false;
}

void ModelHandler::UpdateRoute(const ModelRouteProto& route) {
  std::lock_guard<std::mutex> lock(route_mu_);
  backends_.clear();
  backend_rates_.clear();
  total_throughput_ = 0.;

  double min_rate = std::numeric_limits<double>::max();
  for (auto itr : route.backend_rate()) {
    min_rate = std::min(min_rate, itr.throughput());
  }
  quantum_to_rate_ratio_ = 1. / min_rate;

  for (auto itr : route.backend_rate()) {
    uint32_t backend_id = itr.info().node_id();
    backends_.push_back(backend_id);
    const auto rate = itr.throughput();
    backend_rates_.emplace(backend_id, rate);
    total_throughput_ += rate;
    LOG(INFO) << "- backend " << backend_id << ": " << rate;
    if (backend_quanta_.count(backend_id) == 0) {
      backend_quanta_.emplace(backend_id, rate * quantum_to_rate_ratio_);
    }
  }
  LOG(INFO) << "Total throughput: " << total_throughput_;
  std::sort(backends_.begin(), backends_.end());
  current_drr_index_ %= backends_.size();
  for (auto iter = backend_quanta_.begin(); iter != backend_quanta_.end();) {
    if (backend_rates_.count(iter->first) == 0) {
      iter = backend_quanta_.erase(iter);
    } else {
      ++iter;
    }
  }
}

std::vector<uint32_t> ModelHandler::BackendList() {
  std::vector<uint32_t> ret;
  std::lock_guard<std::mutex> lock(route_mu_);
  for (auto iter : backend_rates_) {
    ret.push_back(iter.first);
  }
  return ret;
}

FakeNexusBackend* ModelHandler::GetBackend() {
  std::lock_guard<std::mutex> lock(route_mu_);
  return GetBackendDeficitRoundRobin();
}

FakeNexusBackend* ModelHandler::GetBackendDeficitRoundRobin() {
  for (size_t i = 0; i < 2 * backends_.size(); ++i) {
    size_t idx = (current_drr_index_ + i) % backends_.size();
    uint32_t backend_id = backends_[idx];
    if (backend_quanta_.at(backend_id) >= 1. - 1e-6) {
      backend_quanta_[backend_id] -= 1.;
      return accessor_.backends.at(backend_id);
    } else {
      auto rate = backend_rates_[backend_id];
      backend_quanta_[backend_id] += rate * quantum_to_rate_ratio_;
      current_drr_index_ = (current_drr_index_ + 1) % backends_.size();
    }
  }
  CHECK(false) << "Deficit Round Robin could not decide.";
}

}  // namespace app
}  // namespace nexus
