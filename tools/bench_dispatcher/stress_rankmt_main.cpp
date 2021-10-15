#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ario/ario.h"
#include "bench_dispatcher/fake_accessor.h"
#include "bench_dispatcher/fake_backend.h"
#include "bench_dispatcher/fake_frontend.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/rankmt/scheduler.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

using namespace nexus;
using namespace nexus::dispatcher;

// FIXME: see rps_meter.cpp
DECLARE_string(hack_rpsmeter);

struct Options {
  int64_t seed;
  int backends;
  int models;
  int slope;
  int intercept;
  int slo;
  double duration;
  double lag;
  double rps;

  static Options FromGflags();
};

class DispatcherRunner {
 public:
  explicit DispatcherRunner(Options options)
      : options_(std::move(options)),
        model_rps_(options_.rps / options_.models),
        tolerance_(static_cast<long>(options_.lag * 1e9)),
        gen_(options_.seed),
        fail_stop_(false),
        should_stop_(false) {
    main_executor_ =
        std::make_shared<ario::EpollExecutor>(ario::PollerType::kSpinning);
    rank_executor_ =
        std::make_shared<ario::EpollExecutor>(ario::PollerType::kSpinning);

    BuildWorkloads();
    LOG(INFO) << "Preparing the benchmark";
    BuildMultiThreadRankScheduler();
    BuildFakeServers();
  }

  int Run() {
    SpawnThreads();

    TimePoint now = Clock::now();
    begin_time_ = now + std::chrono::milliseconds(10 * options_.models);
    stop_time_ = begin_time_ + std::chrono::nanoseconds(
                                   static_cast<long>(options_.duration * 1e9));

    loadgen_contexts_.resize(model_sessions_.size());
    for (size_t i = 0; i < model_sessions_.size(); ++i) {
      InitLoadGen(i);
      PrepareNextRequest(i);
      auto& l = loadgen_contexts_[i];
      l.timer = ario::Timer(
          *model_executors_[i], l.next_time,
          [this, i](ario::ErrorCode error) { ContinueLoadGen(error, i); });
    }

    ario::Timer wait_begin(*main_executor_, begin_time_);
    wait_begin.AsyncWait(
        [this](ario::ErrorCode) { LOG(INFO) << "Stress test begins"; });

    ario::Timer wait_finish(*main_executor_, stop_time_);
    wait_finish.AsyncWait([this](ario::ErrorCode) {
      std::unique_lock lock(stop_mutex_);
      should_stop_ = true;
      lock.unlock();
      stop_cv_.notify_all();
    });
    {
      std::unique_lock lock(stop_mutex_);
      stop_cv_.wait(lock, [this] { return should_stop_; });
    }

    scheduler_->Stop();
    for (auto& e : model_executors_) {
      e->StopEventLoop();
    }
    rank_executor_->StopEventLoop();
    main_executor_->StopEventLoop();

    for (auto iter = threads_.rbegin(); iter != threads_.rend(); ++iter) {
      iter->join();
    }
    LOG(INFO) << "All threads joined.";

    return fail_stop_;
  }

 private:
  void BuildWorkloads() {
    ModelSession m;
    for (int i = 0; i < options_.models; ++i) {
      std::ostringstream ss;
      ss << "sleep#" << options_.slope << "," << options_.intercept << ",0,0";
      m.set_framework(ss.str());
      m.set_model_name("model_" + std::to_string(i));
      m.set_latency_sla(options_.slo);
      model_sessions_.push_back(std::move(m));
    }

    std::ostringstream ss;
    ss << model_rps_;
    for (int i = 1; i < options_.models; ++i) {
      ss << "," << model_rps_;
    }
    FLAGS_hack_rpsmeter = ss.str();
  }

  void BuildMultiThreadRankScheduler() {
    MultiThreadRankScheduler::Builder builder(main_executor_.get(),
                                              rank_executor_.get());
    scheduler_ = builder.Build();
  }

  void BuildFakeServers() {
    uint32_t next_backend_id = 10001;
    for (int i = 0; i < options_.backends; ++i) {
      auto backend_id = next_backend_id++;
      auto backend = std::make_shared<FakeBackendDelegate>(
          main_executor_.get(), backend_id, &accessor_);
      accessor_.AddBackend(NodeId(backend_id), backend);
      scheduler_->AddBackend(NodeId(backend_id), backend);
      backends_.push_back(backend);
    }

    for (size_t i = 0; i < model_sessions_.size(); ++i) {
      uint32_t frontend_id = 60001 + i;
      size_t reserved_size = 0;
      auto frontend = std::make_shared<FakeFrontendDelegate>(
          [this](size_t cnt_done, size_t workload_idx) {}, frontend_id,
          model_sessions_[i], i, reserved_size);
      accessor_.AddFrontend(NodeId(frontend_id), frontend);
      scheduler_->AddFrontend(NodeId(frontend_id), frontend);
      frontends_.push_back(frontend);

      model_executors_.push_back(
          std::make_shared<ario::EpollExecutor>(ario::PollerType::kSpinning));
      auto entrance = scheduler_->AddModelSession(model_executors_.back().get(),
                                                  model_sessions_[i]);
      request_entrances_.push_back(entrance);
      model_index_table_.push_back(entrance.model_index());
    }
  }

  void InitLoadGen(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    l.rand_gen = std::mt19937(options_.seed + workload_idx * 31);
    l.gap_gen = std::exponential_distribution<double>(model_rps_);
    l.last_time = begin_time_;
    l.last_global_id = 1000000000 * (workload_idx + 1);
    l.last_query_id = 0;
    l.model_session_id = ModelSessionToString(model_sessions_[workload_idx]);
    l.frontend = frontends_[workload_idx].get();
  }

  void PrepareNextRequest(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    auto gap_ns = static_cast<long>(l.gap_gen(l.rand_gen) * 1e9);
    l.next_time = l.last_time + std::chrono::nanoseconds(gap_ns);
    auto next_time_ns = l.next_time.time_since_epoch().count();
    if (l.next_time > stop_time_) {
      return;
    }
    auto query_id = ++l.last_query_id;
    auto global_id = ++l.last_global_id;
    auto model_index = model_index_table_[workload_idx];
    l.request.set_model_index(model_index.t);
    l.request.set_query_id(query_id);
    auto* query = l.request.mutable_query_without_input();
    query->set_query_id(query_id);
    query->set_model_index(model_index.t);
    query->set_global_id(global_id);
    query->set_frontend_id(l.frontend->node_id());
    auto* clock = query->mutable_clock();
    clock->set_frontend_recv_ns(next_time_ns);
    clock->set_frontend_dispatch_ns(next_time_ns);
    clock->set_dispatcher_recv_ns(next_time_ns);

    l.frontend->ReceivedQuery(query_id, next_time_ns);
  }

  void ContinueLoadGen(ario::ErrorCode error, size_t workload_idx) {
    if (error != ario::ErrorCode::kOk) return;
    if (fail_stop_) return;
    auto& l = loadgen_contexts_[workload_idx];
    TimePoint now = Clock::now();
    if (now - l.next_time > tolerance_) {
      LOG(ERROR) << "Huge lag: " << (now - l.next_time).count() / 1e3
                 << "us. Stopping...";
      fail_stop_ = true;

      {
        std::lock_guard lock(stop_mutex_);
        should_stop_ = true;
      }
      stop_cv_.notify_all();
      return;
    }
    while (l.next_time < now && l.next_time <= stop_time_) {
      auto& entrance = request_entrances_[workload_idx];
      entrance.EnqueueQuery(std::move(l.request));
      l.last_time = l.next_time;
      PrepareNextRequest(workload_idx);
    }
    if (l.next_time <= stop_time_) {
      l.timer.SetTimeout(l.next_time);
      l.timer.AsyncWait([this, workload_idx](ario::ErrorCode error) {
        ContinueLoadGen(error, workload_idx);
      });
    }
  }

  void SpawnThreads() {
    threads_.emplace_back([ex = main_executor_.get()] {
      PinCpu(0);
      ex->RunEventLoop();
    });
    threads_.emplace_back([ex = rank_executor_.get()] {
      PinCpu(1);
      ex->RunEventLoop();
    });
    int cpu = 2;
    for (auto& e : model_executors_) {
      threads_.emplace_back([ex = e.get(), cpu] {
        PinCpu(cpu);
        ex->RunEventLoop();
      });
      ++cpu;
    }
  }

  struct LoadGenContext {
    ario::Timer timer;

    std::mt19937 rand_gen;
    std::exponential_distribution<double> gap_gen;
    TimePoint last_time;
    uint64_t last_global_id;
    uint64_t last_query_id;
    std::string model_session_id;
    FakeFrontendDelegate* frontend;

    TimePoint next_time;
    DispatchRequest request;
  };

  Options options_;
  double model_rps_;
  std::chrono::nanoseconds tolerance_;
  std::mt19937 gen_;
  std::vector<ModelSession> model_sessions_;
  std::vector<LoadGenContext> loadgen_contexts_;
  std::shared_ptr<ario::EpollExecutor> main_executor_;
  std::shared_ptr<ario::EpollExecutor> rank_executor_;
  std::vector<std::shared_ptr<ario::EpollExecutor>> model_executors_;
  std::unique_ptr<MultiThreadRankScheduler> scheduler_;
  std::vector<MultiThreadRankScheduler::RequestEntrance> request_entrances_;
  std::vector<ModelIndex> model_index_table_;
  FakeDispatcherAccessor accessor_;
  std::vector<std::shared_ptr<FakeBackendDelegate>> backends_;
  std::vector<std::shared_ptr<FakeFrontendDelegate>> frontends_;

  std::vector<std::thread> threads_;
  TimePoint begin_time_;
  TimePoint stop_time_;
  std::atomic_bool fail_stop_;

  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool should_stop_;
};

DEFINE_int64(seed, 0xabcdabcd987LL, "Random seed");
DEFINE_int32(backends, 1, "Number of backends");
DEFINE_int32(models, 1,
             "Number of models. Each model is in a separate thread.");
DEFINE_int32(slope, 1082,
             "Slope of the latency-batch_size line in microseconds");
DEFINE_int32(intercept, 5204,
             "Intercept of the latency-batch_size line in microseconds");
DEFINE_int32(slo, 100, "Latency SLO in milliseconds");
DEFINE_double(duration, 10, "Stress test duration in seconds");
DEFINE_double(lag, 0.005, "Max tolerable request lag in seconds");
DEFINE_double(rps, 1000, "Total request rate per second");

Options Options::FromGflags() {
  return {
      FLAGS_seed, FLAGS_backends, FLAGS_models, FLAGS_slope, FLAGS_intercept,
      FLAGS_slo,  FLAGS_duration, FLAGS_lag,    FLAGS_rps,
  };
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  int argp = google::ParseCommandLineFlags(&argc, &argv, false);
  google::InstallFailureSignalHandler();

  auto options = Options::FromGflags();
  DispatcherRunner bencher(std::move(options));
  int exitcode = bencher.Run();
  if (exitcode == 0) {
    LOG(INFO) << "Stress test succeeded";
  } else {
    LOG(INFO) << "Stress test failed";
  }
  return exitcode;
}
