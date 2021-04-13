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

#include "ario/epoll.h"
#include "ario/error.h"
#include "ario/timer.h"
#include "bench_dispatcher/fake_accessor.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/rankmt_scheduler.h"
#include "nexus/dispatcher/scheduler.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

DEFINE_int64(seed, 0xabcdabcd987LL, "Random seed");
DEFINE_int32(warmup, 3, "Warmup duration in seconds");
DEFINE_int32(duration, 10, "Benchmark duration in seconds");
DEFINE_bool(multithread, false, "Whether to enable multithreading");
DEFINE_int32(max_flying_per_workload, 10,
             "Max number of flying requests per workload");
DEFINE_int32(num_backends, 1, "Number of backends");
DEFINE_int32(num_models, 1, "Number of models. l(b) = slope * b + intercept.");
DEFINE_int32(profile_slope, 5973, "Slope in microseconds");
DEFINE_int32(profile_intercept, 20732, "Intercept in microseconds");
DEFINE_double(profile_noise, 0.1,
              "Gaussian noise percentage added to slope and intercept");
DEFINE_int32(model_slo_lo, 50, "Lower bound of latency SLO in milliseconds");
DEFINE_int32(model_slo_hi, 300, "Upper bound of latency SLO in milliseconds");

using namespace nexus;
using namespace nexus::dispatcher;

struct Options {
  int64_t seed;
  int warmup;
  int duration;
  bool multithread;
  int max_flying_per_workload;
  int num_backends;
  int num_models;
  int profile_slope;
  int profile_intercept;
  double profile_noise;
  int model_slo_lo;
  int model_slo_hi;

  static Options FromArgs(int argc, char** argv, int argp) {
    return Options{FLAGS_seed,
                   FLAGS_warmup,
                   FLAGS_duration,
                   FLAGS_multithread,
                   FLAGS_max_flying_per_workload,
                   FLAGS_num_backends,
                   FLAGS_num_models,
                   FLAGS_profile_slope,
                   FLAGS_profile_intercept,
                   FLAGS_profile_noise,
                   FLAGS_model_slo_lo,
                   FLAGS_model_slo_hi};
  }
};

struct Workload {
  ModelSession model_session;

  std::string ToString() const { return ModelSessionToString(model_session); }
};

class DispatcherBencher;

class FakeFrontendDelegate : public FrontendDelegate {
 public:
  enum class QueryStatus {
    kPending,
    kDropped,
    kTimeout,
    kSuccess,
  };

  struct QueryContext {
    QueryStatus status;
    int64_t frontend_recv_ns;
  };

  FakeFrontendDelegate(DispatcherBencher& bencher, uint32_t node_id,
                       ModelSession model_session, size_t workload_idx)
      : FrontendDelegate(node_id),
        bencher_(&bencher),
        model_session_(std::move(model_session)),
        workload_idx_(workload_idx) {}

  void Tick() override {
    // Ignore
  }

  void UpdateBackendList(BackendListUpdates&& request) override {
    // Ignore
  }

  void ReportRequestDone(size_t cnt_done);

  void MarkQueryDroppedByDispatcher(DispatchReply&& request) override {
    auto& qctx = queries_[request.query_id()];
    qctx.status = QueryStatus::kDropped;
    ReportRequestDone(1);
  }

  void Reserve(size_t max_queries) {
    reserved_size_ = max_queries;
    queries_.reset(new QueryContext[max_queries]);
  }

  void ReceivedQuery(uint64_t query_id, int64_t frontend_recv_ns) {
    auto& qctx = queries_[query_id];
    qctx.status = QueryStatus::kPending;
    qctx.frontend_recv_ns = frontend_recv_ns;
  }

  void GotBatchReply(const BatchPlanProto& plan) {
    for (const auto& query : plan.queries()) {
      auto query_id = query.query_without_input().query_id();
      auto& qctx = queries_[query_id];
      auto deadline_ns =
          qctx.frontend_recv_ns + model_session_.latency_sla() * 1000 * 1000;
      if (plan.expected_finish_time_ns() < deadline_ns) {
        qctx.status = QueryStatus::kSuccess;
      } else {
        qctx.status = QueryStatus::kTimeout;
      }
    }
    ReportRequestDone(plan.queries_size());
  }

  const ModelSession& model_session() const { return model_session_; }
  const QueryContext* queries() const { return queries_.get(); }

 private:
  DispatcherBencher* bencher_;
  ModelSession model_session_;
  size_t workload_idx_;
  size_t reserved_size_ = 0;
  std::unique_ptr<QueryContext[]> queries_;
};

bool HeapOrderBatchPlanByExecTimeASC(const BatchPlanProto& lhs,
                                     const BatchPlanProto& rhs) {
  return lhs.exec_time_ns() > rhs.exec_time_ns();
}

bool BatchPlanIntersects(const BatchPlanProto& a, const BatchPlanProto& b) {
  if (a.expected_finish_time_ns() <= b.exec_time_ns()) return false;
  if (b.expected_finish_time_ns() <= a.exec_time_ns()) return false;
  return true;
}

class FakeBackendDelegate : public BackendDelegate {
 public:
  FakeBackendDelegate(ario::EpollExecutor& executor, uint32_t node_id,
                      FakeDispatcherAccessor* accessor)
      : BackendDelegate(node_id, "FakeGPU", "FakeUUID", 0),
        executor_(&executor),
        accessor_(accessor),
        timer_(*executor_) {}

  void Tick() override {
    // Ignore
  }

  void SendLoadModelCommand(const ModelSession& model_session,
                            uint32_t max_batch) override {
    // Ignore
  }

  void EnqueueBatchPlan(BatchPlanProto&& request) override {
    TimePoint now = Clock::now();
    auto now_ns = now.time_since_epoch().count();

    CHECK_LT(request.exec_time_ns(), request.expected_finish_time_ns())
        << "Incorrect finish time. " << request.DebugString();
    LOG_IF(ERROR, now_ns > request.exec_time_ns())
        << "BatchPlan too late. " << request.DebugString();

    std::lock_guard lock(mutex_);
    for (const auto& plan : batchplans_) {
      CHECK(!BatchPlanIntersects(plan, request))
          << "Batchplan intersects.\n"
          << "existing plan: exec_time=base"
          << " finish_time=base+"
          << (plan.expected_finish_time_ns() - plan.exec_time_ns()) << "\n"
          << "new plan: exec_time=base+"
          << (request.exec_time_ns() - plan.exec_time_ns())
          << " finish_time=base+"
          << (request.expected_finish_time_ns() - plan.exec_time_ns());
    }
    batchplans_.emplace_back(std::move(request));
    std::push_heap(batchplans_.begin(), batchplans_.end(),
                   HeapOrderBatchPlanByExecTimeASC);
    TimePoint finish_time(
        std::chrono::nanoseconds(batchplans_[0].expected_finish_time_ns()));
    if (timer_.timeout() != finish_time) {
      timer_.SetTimeout(finish_time);
      timer_.AsyncWait([this](ario::ErrorCode error) { OnTimer(error); });
    }
  }

  void DrainBatchPlans() {
    for (const auto& plan : batchplans_) {
      OnBatchFinish(plan);
    }
    batchplans_.clear();
  }

 private:
  void OnBatchFinish(const BatchPlanProto& plan) {
    auto frontend_id = plan.queries(0).query_without_input().frontend_id();
    auto* frontend = accessor_->GetFrontend(NodeId(frontend_id)).get();
    auto* fake = static_cast<FakeFrontendDelegate*>(frontend);
    fake->GotBatchReply(plan);
  }

  void OnTimer(ario::ErrorCode) {
    TimePoint now = Clock::now();
    auto now_ns = now.time_since_epoch().count();
    std::vector<BatchPlanProto> finished_plans;
    std::unique_lock lock(mutex_);
    while (!batchplans_.empty()) {
      if (batchplans_[0].expected_finish_time_ns() > now_ns) {
        break;
      }
      finished_plans.emplace_back(std::move(batchplans_[0]));
      std::pop_heap(batchplans_.begin(), batchplans_.end(),
                    HeapOrderBatchPlanByExecTimeASC);
      batchplans_.pop_back();
    }
    lock.unlock();
    for (const auto& plan : finished_plans) {
      OnBatchFinish(plan);
    }
  }

  ario::EpollExecutor* executor_;
  FakeDispatcherAccessor* accessor_;
  ario::Timer timer_;
  std::mutex mutex_;
  std::vector<BatchPlanProto> batchplans_;
};

class DispatcherBencher {
 public:
  explicit DispatcherBencher(Options options)
      : options_(std::move(options)), gen_(options_.seed) {
    main_executor_ = std::make_shared<ario::EpollExecutor>();
    if (options_.multithread) {
      rank_executor_ = std::make_shared<ario::EpollExecutor>();
    } else {
      rank_executor_ = main_executor_;
    }

    BuildWorkloads();
    LOG(INFO) << "Preparing the benchmark";
    BuildMultiThreadRankScheduler();
    BuildFakeServers();
  }

  int Run() {
    TimePoint now = Clock::now();
    warmup_time_ = now + std::chrono::seconds(2);
    serious_time_ = warmup_time_ + std::chrono::seconds(options_.warmup);
    stop_time_ = serious_time_ + std::chrono::seconds(options_.duration);
    should_join_ = false;

    loadgen_contexts_.resize(workloads_.size());
    for (size_t i = 0; i < workloads_.size(); ++i) {
      InitLoadGen(i);
      PrepareNextRequest(i);
      auto& l = loadgen_contexts_[i];
    }
    threads_.emplace_back(&ario::EpollExecutor::RunEventLoop,
                          main_executor_.get());
    if (options_.multithread) {
      threads_.emplace_back(&ario::EpollExecutor::RunEventLoop,
                            rank_executor_.get());
      for (auto& e : model_executors_) {
        threads_.emplace_back(&ario::EpollExecutor::RunEventLoop, e.get());
      }
    }

    ario::Timer wait_warmup(*main_executor_, warmup_time_,
                            [this](ario::ErrorCode) {
                              LOG(INFO) << "Start warming up...";
                              for (size_t i = 0; i < workloads_.size(); ++i) {
                                model_executors_[i]->Post(
                                    [this, workload_idx = i](ario::ErrorCode) {
                                      SendMore(workload_idx);
                                    },
                                    ario::ErrorCode::kOk);
                              }
                            });
    ario::Timer wait_serious(
        *main_executor_, serious_time_,
        [this](ario::ErrorCode) { LOG(INFO) << "Start benchmarking..."; });
    ario::Timer wait_stop(*main_executor_, stop_time_, [this](ario::ErrorCode) {
      LOG(INFO) << "Stopped sending more requests";
    });

    uint32_t max_slo = 0;
    for (auto& w : workloads_) {
      max_slo = std::max(max_slo, w.model_session.latency_sla());
    }
    uint32_t cooldown_ms = max_slo * 1.5;
    std::mutex mutex;
    std::condition_variable cv;
    ario::Timer wait_finish(*main_executor_,
                            stop_time_ + std::chrono::milliseconds(cooldown_ms),
                            [this, &mutex, &cv](ario::ErrorCode) {
                              std::unique_lock lock(mutex);
                              should_join_ = true;
                              lock.unlock();
                              cv.notify_all();
                            });
    {
      std::unique_lock lock(mutex);
      cv.wait(lock, [this] { return should_join_; });
    }

    scheduler_->Stop();

    if (options_.multithread) {
      for (auto& e : model_executors_) {
        e->StopEventLoop();
      }
      rank_executor_->StopEventLoop();
    }
    main_executor_->StopEventLoop();

    for (auto iter = threads_.rbegin(); iter != threads_.rend(); ++iter) {
      iter->join();
    }
    LOG(INFO) << "All threads joined.";
    for (auto& backend : backends_) {
      backend->DrainBatchPlans();
    }

    char buf[256];
    snprintf(buf, sizeof(buf), "%-12s %8s %8s %8s %8s %8s %8s", "model_name",
             "noreply", "dropped", "timeout", "success", "total", "badrate");
    LOG(INFO) << "Stats:";
    LOG(INFO) << "  " << buf;
    auto serious_time_ns = serious_time_.time_since_epoch().count();
    int sum_noreply = 0, sum_dropped = 0, sum_timeout = 0, sum_success = 0;
    double worst_badrate = 0.0;
    for (int i = 0; i < options_.num_models; ++i) {
      auto& frontend = frontends_[i];
      int cnt_noreply = 0, cnt_dropped = 0, cnt_timeout = 0, cnt_success = 0;
      auto n = loadgen_contexts_[i].last_query_id - 1;
      for (size_t i = 1; i <= n; ++i) {
        const auto& qctx = frontend->queries()[i];
        if (qctx.frontend_recv_ns < serious_time_ns) {
          continue;
        }
        switch (qctx.status) {
          case FakeFrontendDelegate::QueryStatus::kDropped:
            ++cnt_dropped;
            break;
          case FakeFrontendDelegate::QueryStatus::kTimeout:
            ++cnt_timeout;
            break;
          case FakeFrontendDelegate::QueryStatus::kSuccess:
            ++cnt_success;
            break;
          default:
            ++cnt_noreply;
            break;
        }
      }
      const auto& model_name = frontend->model_session().model_name();
      int total = cnt_dropped + cnt_timeout + cnt_success + cnt_noreply;
      double badrate = 100.0 - cnt_success * 100.0 / total;
      snprintf(buf, sizeof(buf), "%-12s %8d %8d %8d %8d %8d %8.3f%%",
               model_name.c_str(), cnt_noreply, cnt_dropped, cnt_timeout,
               cnt_success, total, badrate);
      LOG(INFO) << "  " << buf;
      sum_noreply += cnt_noreply;
      sum_dropped += cnt_dropped;
      sum_timeout += cnt_timeout;
      sum_success += cnt_success;
      worst_badrate = std::max(worst_badrate, badrate);
    }

    int total_queries = sum_dropped + sum_timeout + sum_success + sum_noreply;
    double avg_badrate = 100.0 - sum_success * 100.0 / total_queries;
    snprintf(buf, sizeof(buf),
             "%-12s %8d %8d %8d %8d %8d (avg %.3f%%, worst %.3f%%)", "TOTAL",
             sum_noreply, sum_dropped, sum_timeout, sum_success, total_queries,
             avg_badrate, worst_badrate);
    LOG(INFO) << "  " << buf;

    double throughput = total_queries * 1.0 / options_.duration;
    LOG(INFO) << "  "
              << "Throughput: " << throughput << " rps";

    if (sum_noreply) {
      LOG(ERROR) << "Buggy scheduler. There are " << sum_noreply
                 << " queries having no reply.";
      return 1;
    }
    return 0;
  }

  void OnRequestDone(size_t cnt_done, size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    l.cnt_flying -= cnt_done;
    model_executors_[workload_idx]->Post(
        [this, workload_idx](ario::ErrorCode) { SendMore(workload_idx); },
        ario::ErrorCode::kOk);
  }

 private:
  void BuildWorkloads() {
    char buf[100];
    std::normal_distribution<double> normal;
    std::uniform_int_distribution<int> uniform_slo(options_.model_slo_lo,
                                                   options_.model_slo_hi);
    for (int i = 0; i < options_.num_models; ++i) {
      int slope =
          options_.profile_slope * (1 + normal(gen_) * options_.profile_noise);
      int intercept = options_.profile_intercept *
                      (1 + normal(gen_) * options_.profile_noise);

      ModelSession model;
      snprintf(buf, sizeof(buf), "sleep#%d,%d,999,0", slope, intercept);
      model.set_framework(buf);
      snprintf(buf, sizeof(buf), "model_%02d", i + 1);
      model.set_model_name(buf);
      model.set_latency_sla(uniform_slo(gen_));
      workloads_.push_back({std::move(model)});
    }

    LOG(INFO) << "Workloads:";
    for (const auto& w : workloads_) {
      LOG(INFO) << "  " << w.ToString();
    }
  }

  void BuildMultiThreadRankScheduler() {
    MultiThreadRankScheduler::Builder builder(main_executor_.get(),
                                              rank_executor_.get());
    scheduler_ = builder.Build();
  }

  void BuildFakeServers() {
    uint32_t next_backend_id = 10001;
    for (int i = 0; i < options_.num_backends; ++i) {
      auto backend_id = next_backend_id++;
      auto backend = std::make_shared<FakeBackendDelegate>(
          *main_executor_, backend_id, &accessor_);
      accessor_.AddBackend(NodeId(backend_id), backend);
      scheduler_->AddBackend(NodeId(backend_id), backend);
      backends_.push_back(backend);
    }

    for (size_t i = 0; i < workloads_.size(); ++i) {
      const auto& w = workloads_[i];
      uint32_t frontend_id = 60001 + i;
      auto frontend = std::make_shared<FakeFrontendDelegate>(
          *this, frontend_id, w.model_session, i);
      accessor_.AddFrontend(NodeId(frontend_id), frontend);
      scheduler_->AddFrontend(NodeId(frontend_id), frontend);
      frontends_.push_back(frontend);

      if (options_.multithread) {
        model_executors_.push_back(std::make_shared<ario::EpollExecutor>());
      } else {
        model_executors_.push_back(main_executor_);
      }
      auto entrance = scheduler_->AddModelSession(model_executors_.back().get(),
                                                  w.model_session);
      request_entrances_.push_back(entrance);
    }
  }

  void InitLoadGen(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    const auto& workload = workloads_[workload_idx];
    l.last_global_id = 1000000000 * (workload_idx + 1);
    l.last_query_id = 0;
    l.model_session_id = ModelSessionToString(workload.model_session);
    l.frontend = frontends_[workload_idx].get();
    l.reserved_size = (options_.warmup + options_.duration) * 1000000;
    l.frontend->Reserve(l.reserved_size);
    l.cnt_flying = 0;
  }

  void PrepareNextRequest(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    const auto& workload = workloads_[workload_idx];
    auto query_id = ++l.last_query_id;
    auto global_id = ++l.last_global_id;
    CHECK_LT(query_id, l.reserved_size) << "Reserved size not big enough.";
    l.request.mutable_model_session()->CopyFrom(workload.model_session);
    l.request.set_query_id(query_id);
    auto* query = l.request.mutable_query_without_input();
    query->set_query_id(query_id);
    query->set_model_session_id(l.model_session_id);
    query->set_global_id(global_id);
    query->set_frontend_id(l.frontend->node_id());
  }

  void SendQuery(TimePoint now, int workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    auto* query = l.request.mutable_query_without_input();
    auto* clock = query->mutable_clock();
    auto now_ns = now.time_since_epoch().count();
    clock->set_frontend_recv_ns(now_ns);
    clock->set_frontend_dispatch_ns(now_ns);
    clock->set_dispatcher_recv_ns(now_ns);
    l.frontend->ReceivedQuery(query->query_id(), now_ns);

    auto& entrance = request_entrances_[workload_idx];
    entrance.EnqueueQuery(std::move(l.request));
  }

  void SendMore(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    for (;;) {
      if (l.cnt_flying == options_.max_flying_per_workload) {
        break;
      }
      TimePoint now = Clock::now();
      if (now > stop_time_) {
        break;
      }
      ++l.cnt_flying;
      SendQuery(now, workload_idx);
      PrepareNextRequest(workload_idx);
    }
  }

  using Clock = std::chrono::high_resolution_clock;
  using TimePoint = std::chrono::time_point<Clock, std::chrono::nanoseconds>;

  struct LoadGenContext {
    uint64_t last_global_id;
    uint64_t last_query_id;
    std::string model_session_id;
    FakeFrontendDelegate* frontend;
    size_t reserved_size;

    DispatchRequest request;
    size_t cnt_flying;
  };

  Options options_;
  std::mt19937 gen_;
  std::vector<Workload> workloads_;
  std::vector<LoadGenContext> loadgen_contexts_;
  std::shared_ptr<ario::EpollExecutor> main_executor_;
  std::shared_ptr<ario::EpollExecutor> rank_executor_;
  std::vector<std::shared_ptr<ario::EpollExecutor>> model_executors_;
  std::unique_ptr<MultiThreadRankScheduler> scheduler_;
  std::vector<MultiThreadRankScheduler::RequestEntrance> request_entrances_;
  FakeDispatcherAccessor accessor_;
  std::vector<std::shared_ptr<FakeBackendDelegate>> backends_;
  std::vector<std::shared_ptr<FakeFrontendDelegate>> frontends_;

  std::vector<std::thread> threads_;
  TimePoint warmup_time_;
  TimePoint serious_time_;
  TimePoint stop_time_;
  bool should_join_;
};

void FakeFrontendDelegate::ReportRequestDone(size_t cnt_done) {
  bencher_->OnRequestDone(cnt_done, workload_idx_);
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  int argp = google::ParseCommandLineFlags(&argc, &argv, false);
  google::InstallFailureSignalHandler();

  auto options = Options::FromArgs(argc, argv, argp);
  DispatcherBencher bencher(std::move(options));
  int exitcode = bencher.Run();
  return exitcode;
}
