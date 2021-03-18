#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>

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
#include "bench_dispatcher/fake_accessor.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/rank_scheduler.h"
#include "nexus/dispatcher/round_robin_scheduler.h"
#include "nexus/dispatcher/scheduler.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

DEFINE_int64(seed, 0xabcdabcd987LL, "Random seed");
DEFINE_int32(warmup, 3, "Warmup duration in seconds");
DEFINE_int32(duration, 60, "Benchmark duration in seconds");
DEFINE_double(multiplier, 1.0, "Multiplier for avg_rps and num_backends");
DEFINE_int32(threads, 1, "Scheduler worker threads");

using namespace nexus;
using namespace nexus::dispatcher;

struct Options {
  int64_t seed;
  int warmup;
  int duration;
  double multiplier;
  int threads;
  std::vector<char*> workloads;

  static Options FromArgs(int argc, char** argv, int argp) {
    if (argp == argc) {
      LOG(FATAL) << "Please provide a list of workloads. Example: \""
                    "sleep#6817,23431,999,0:resnet_01:1:100"
                    "@avg_rps=1953,num_backends=29\"";
    }
    return Options{FLAGS_seed,       FLAGS_warmup,  FLAGS_duration,
                   FLAGS_multiplier, FLAGS_threads, {argv + argp, argv + argc}};
  }
};

struct Workload {
  ModelSession model_session;
  int avg_rps;
  int num_backends;

  std::string ToString() const {
    std::stringstream ss;
    ss << ModelSessionToString(model_session) << '@' << "avg_rps=" << avg_rps
       << ",num_backends=" << num_backends;
    return ss.str();
  }
};

int ParseIntAttribute(std::unordered_map<std::string, std::string>& kvs,
                      const std::string& key, const std::string& str) {
  auto iter = kvs.find(key);
  if (iter == kvs.end()) {
    LOG(FATAL) << "ParseWorkload: cannot find attribute: \"" << key
               << "\" str: \"" << str << '"';
  }
  int ret;
  try {
    ret = std::stoi(iter->second);
  } catch (const std::exception& e) {
    LOG(FATAL) << "ParseWorkload: invalid value for attribute \"" << key
               << "\". " << e.what() << " str: \"" << str << '"';
  }
  kvs.erase(iter);
  return ret;
}

Workload ParseWorkload(const std::string& str) {
  // e.g. sleep#6817,23431,999,0:resnet_01:1:100@avg_rps=1953,num_backends=29
  Workload ret;
  auto pos_at = str.find('@');
  if (pos_at == std::string::npos) {
    LOG(FATAL) << "ParseWorkload: cannot find '@'. str: \"" << str << '"';
  }
  bool ok = ParseModelSession(str.substr(0, pos_at), &ret.model_session);
  if (!ok) {
    LOG(FATAL) << "ParseWorkload: failed to parse model_session. str: \"" << str
               << '"';
  }

  std::vector<std::string> tokens;
  SplitString(str.substr(pos_at + 1), ',', &tokens);
  std::unordered_map<std::string, std::string> kvs;
  for (const auto& token : tokens) {
    auto pos_eq = token.find('=');
    if (pos_eq == std::string::npos) {
      LOG(FATAL) << "ParseWorkload: cannot find '='. str: \"" << str << '"';
    }
    auto res =
        kvs.try_emplace(token.substr(0, pos_eq), token.substr(pos_eq + 1));
    if (!res.second) {
      LOG(FATAL) << "ParseWorkload: duplicated attribute: \""
                 << res.first->first << "\" str: \"" << str << '"';
    }
  }
  ret.avg_rps = ParseIntAttribute(kvs, "avg_rps", str);
  ret.num_backends = ParseIntAttribute(kvs, "num_backends", str);
  return ret;
}

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

  FakeFrontendDelegate(uint32_t node_id, ModelSession model_session)
      : FrontendDelegate(node_id), model_session_(std::move(model_session)) {}

  void Tick() override {
    // Ignore
  }

  void UpdateBackendList(BackendListUpdates&& request) override {
    // Ignore
  }

  void MarkQueryDroppedByDispatcher(DispatchReply&& request) override {
    auto& qctx = queries_[request.query_id()];
    qctx.status = QueryStatus::kDropped;
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
  }

  const ModelSession& model_session() const { return model_session_; }
  QueryContext* queries() const { return queries_.get(); }
  size_t total_queries() const { return total_queries_; }
  void set_total_queries(size_t total_queries) {
    total_queries_ = total_queries;
  }

 private:
  ModelSession model_session_;
  size_t reserved_size_ = 0;
  std::unique_ptr<QueryContext[]> queries_;
  size_t total_queries_ = 0;
};

struct OrderBatchPlanByExecTimeASC {
  bool operator()(const BatchPlanProto& lhs, const BatchPlanProto& rhs) const {
    return lhs.exec_time_ns() < rhs.exec_time_ns();
  }
};

bool BatchPlanIntersects(const BatchPlanProto& a, const BatchPlanProto& b) {
  if (a.expected_finish_time_ns() <= b.exec_time_ns()) return false;
  if (b.expected_finish_time_ns() <= a.exec_time_ns()) return false;
  return true;
}

class FakeBackendDelegate : public BackendDelegate {
 public:
  FakeBackendDelegate(uint32_t node_id, FakeDispatcherAccessor* accessor)
      : BackendDelegate(node_id, "FakeGPU", "FakeUUID", 0),
        accessor_(accessor) {}

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

    std::vector<BatchPlanProto> finished_plans;
    std::unique_lock lock(mutex_);
    for (auto iter = batchplans_.begin(); iter != batchplans_.end();) {
      if (iter->expected_finish_time_ns() > now_ns) {
        finished_plans.emplace_back(std::move(*iter));
        iter = batchplans_.erase(iter);
      } else {
        CHECK(!BatchPlanIntersects(*iter, request))
            << "Batchplan intersects. iter: " << iter->DebugString()
            << " request: " << request.DebugString();
        ++iter;
      }
    }
    batchplans_.emplace(std::move(request));
    lock.unlock();
    for (const auto& plan : finished_plans) {
      OnBatchFinish(plan);
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

  FakeDispatcherAccessor* accessor_;
  std::mutex mutex_;
  std::set<BatchPlanProto, OrderBatchPlanByExecTimeASC> batchplans_;
};

class DispatcherBencher {
 public:
  explicit DispatcherBencher(Options options) : options_(std::move(options)) {
    BuildWorkloads();
    LOG(INFO) << "Preparing the benchmark";
    // BuildRoundRobinScheduler();
    BuildRankScheduler();
    BuildFakeServers();
  }

  void Run() {
    for (int i = 0; i < options_.threads; ++i) {
      threads_.emplace_back([this] {
        pthread_setname_np(pthread_self(), "Scheduler");
        executor_.RunEventLoop();
      });
    }

    for (size_t i = 0; i < workloads_.size(); ++i) {
      threads_.emplace_back([this, i] {
        pthread_setname_np(pthread_self(), "LoadGen");
        RunLoadGenWorker(i);
      });
    }

    {
      std::lock_guard lock(mutex_);
      bench_started_ = true;
      TimePoint now = Clock::now();
      warmup_time_ = now + std::chrono::seconds(2);
      serious_time_ = warmup_time_ + std::chrono::seconds(options_.warmup);
      stop_time_ = serious_time_ + std::chrono::seconds(options_.duration);
    }
    cv_.notify_all();
    std::this_thread::sleep_until(warmup_time_);
    LOG(INFO) << "Start benchmarking...";

    std::this_thread::sleep_until(stop_time_ + std::chrono::seconds(1));
    LOG(INFO) << "Stopped sending more requests";
    scheduler_->Stop();
    executor_.StopEventLoop();
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
    for (auto& frontend : frontends_) {
      int cnt_noreply = 0, cnt_dropped = 0, cnt_timeout = 0, cnt_success = 0;
      auto n = frontend->total_queries();
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
      LOG(FATAL) << "Buggy scheduler. There are " << sum_noreply
                 << " queries having no reply.";
    }
  }

 private:
  void BuildWorkloads() {
    for (auto& arg : options_.workloads) {
      auto w = ParseWorkload(arg);
      w.avg_rps = static_cast<int>(w.avg_rps * options_.multiplier);
      w.num_backends =
          static_cast<int>(std::round(w.num_backends * options_.multiplier));
      workloads_.push_back(std::move(w));
    }

    LOG(INFO) << "Workloads:";
    for (const auto& w : workloads_) {
      LOG(INFO) << "  " << w.ToString();
    }
  }

  void BuildRoundRobinScheduler() {
    YAML::Node static_config;
    for (const auto& w : workloads_) {
      const auto& model_name = w.model_session.model_name();
      static_config[model_name] = w.num_backends;
    }

    auto accessor = std::make_unique<FakeDispatcherAccessor>();
    accessor_ = accessor.get();
    RoundRobinScheduler::Builder builder(&ModelDatabase::Singleton(),
                                         std::move(static_config));
    scheduler_ = builder.Build(std::move(accessor));
  }

  void BuildRankScheduler() {
    auto accessor = std::make_unique<FakeDispatcherAccessor>();
    accessor_ = accessor.get();
    RankScheduler::Builder builder(executor_);
    scheduler_ = builder.Build(std::move(accessor));
  }

  void BuildFakeServers() {
    uint32_t next_backend_id = 10001;
    for (const auto& w : workloads_) {
      for (int j = 0; j < w.num_backends; ++j) {
        auto backend_id = next_backend_id++;
        auto backend =
            std::make_shared<FakeBackendDelegate>(backend_id, accessor_);
        accessor_->AddBackend(NodeId(backend_id), backend);
        backends_.push_back(backend);
        scheduler_->AddBackend(NodeId(backend_id));
      }
    }

    for (size_t i = 0; i < workloads_.size(); ++i) {
      const auto& w = workloads_[i];
      uint32_t frontend_id = 60001 + i;
      auto frontend =
          std::make_shared<FakeFrontendDelegate>(frontend_id, w.model_session);
      accessor_->AddFrontend(NodeId(frontend_id), frontend);
      frontends_.push_back(frontend);
      scheduler_->AddModelSession(w.model_session);
    }
  }

  void RunLoadGenWorker(size_t workload_idx) {
    const auto& workload = workloads_[workload_idx];
    std::mt19937 rand_gen(options_.seed + workload_idx * 31);
    std::exponential_distribution<double> gap_gen(workload.avg_rps);
    auto last_time = warmup_time_;
    uint64_t last_global_id = 1000000000 * (workload_idx + 1);
    uint64_t last_query_id = 0;
    DispatchRequest request;
    auto model_session_id = ModelSessionToString(workload.model_session);
    auto* frontend = frontends_[workload_idx].get();
    auto frontend_id = frontend->node_id();
    auto reserved_size = (1.0 + std::sqrt(workload.avg_rps)) *
                         workload.avg_rps *
                         (options_.warmup + options_.duration) * 3;
    frontend->Reserve(reserved_size);

    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return bench_started_; });
    }

    std::this_thread::sleep_until(warmup_time_);
    for (;;) {
      auto gap_ns = static_cast<long>(gap_gen(rand_gen) * 1e9);
      auto next_time = last_time + std::chrono::nanoseconds(gap_ns);
      auto next_time_ns = next_time.time_since_epoch().count();
      if (next_time > stop_time_) {
        break;
      }
      auto query_id = ++last_query_id;
      auto global_id = ++last_global_id;
      CHECK_LT(query_id, reserved_size) << "Reserved size not big enough.";
      request.mutable_model_session()->CopyFrom(workload.model_session);
      request.set_query_id(query_id);
      auto* query = request.mutable_query_without_input();
      query->set_query_id(query_id);
      query->set_model_session_id(model_session_id);
      query->set_global_id(global_id);
      query->set_frontend_id(frontend_id);
      auto* clock = query->mutable_clock();
      clock->set_frontend_recv_ns(next_time_ns);
      clock->set_frontend_dispatch_ns(next_time_ns);
      clock->set_dispatcher_recv_ns(next_time_ns);

      frontend->ReceivedQuery(query_id, next_time_ns);
      std::this_thread::sleep_until(next_time);
      executor_.Post(
          [this, req = std::move(request)](ario::ErrorCode) mutable {
            scheduler_->EnqueueQuery(std::move(req));
          },
          ario::ErrorCode::kOk);

      TimePoint now = Clock::now();
      if (now > serious_time_ &&
          now - next_time > std::chrono::milliseconds(1)) {
        auto diff_ns = (now - next_time).count();
        LOG(ERROR) << "Scheduler too busy. now - next_time = " << diff_ns / 1000
                   << "us";
      }
      last_time = next_time;
    }
    frontend->set_total_queries(last_query_id);
  }

  using Clock = std::chrono::high_resolution_clock;
  using TimePoint = std::chrono::time_point<Clock, std::chrono::nanoseconds>;

  Options options_;
  std::vector<Workload> workloads_;
  ario::EpollExecutor executor_;
  std::mutex scheduler_mutex_;
  std::unique_ptr<Scheduler> scheduler_;
  FakeDispatcherAccessor* accessor_;  // Owned by scheduler_
  std::vector<std::shared_ptr<FakeBackendDelegate>> backends_;
  std::vector<std::shared_ptr<FakeFrontendDelegate>> frontends_;

  std::vector<std::thread> threads_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool bench_started_ = false;
  TimePoint warmup_time_;
  TimePoint serious_time_;
  TimePoint stop_time_;
};

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  int argp = google::ParseCommandLineFlags(&argc, &argv, false);
  google::InstallFailureSignalHandler();

  auto options = Options::FromArgs(argc, argv, argp);
  DispatcherBencher bencher(std::move(options));
  bencher.Run();
}
