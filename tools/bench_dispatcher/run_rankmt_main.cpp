#include <gflags/gflags.h>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <iostream>
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
#include "nexus/common/gapgen.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/rankmt/scheduler.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

using namespace nexus;
using namespace nexus::dispatcher;

struct TraceSegment {
  double rps;
  double duration;
};

struct ModelOptions {
  ModelSession model_session;
  GapGeneratorBuilder gap_builder;
  std::vector<TraceSegment> warmup;
  std::vector<TraceSegment> bench;
  std::vector<TraceSegment> cooldown;
};

struct Options {
  int64_t seed;
  bool multithread;
  std::string dump_schedule;
  int gpus;
  std::vector<ModelOptions> models;

  static Options FromYaml(const YAML::Node& config);
};

class DispatcherRunner {
 public:
  explicit DispatcherRunner(Options options, RankmtConfig rankmt_options)
      : options_(std::move(options)),
        rankmt_options_(std::move(rankmt_options)) {
    main_executor_ =
        std::make_shared<ario::EpollExecutor>(ario::PollerType::kSpinning);
    if (options_.multithread) {
      rank_executor_ =
          std::make_shared<ario::EpollExecutor>(ario::PollerType::kSpinning);
    } else {
      rank_executor_ = main_executor_;
    }

    LOG(INFO) << "Preparing the benchmark";
    start_at_ = Clock::now() + std::chrono::seconds(2);
    BuildWorkloads();
    BuildMultiThreadRankScheduler();
    BuildFakeServers();
  }

  int Run() {
    for (size_t i = 0; i < options_.models.size(); ++i) {
      auto& l = loadgen_contexts_[i];
      l.timer = ario::Timer(*model_executors_[i], l.next_time,
                            [this, i](ario::ErrorCode error) {
                              if (error != ario::ErrorCode::kOk) return;
                              PostNextRequest(i);
                            });
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

    long stop_offset_ns = 0;
    for (size_t i = 0; i < options_.models.size(); ++i) {
      auto& w = options_.models[i];
      auto& l = loadgen_contexts_[i];
      double duration = w.model_session.latency_sla() / 1e3 * 2;
      duration += l.warmup_duration + l.bench_duration + l.cooldown_duration;
      long ns = l.start_offset_ns + duration * 1e9;
      stop_offset_ns = std::max(stop_offset_ns, ns);
    }
    auto stop_at = start_at_ + std::chrono::nanoseconds(stop_offset_ns);

    std::mutex mutex;
    std::condition_variable cv;
    bool should_join = false;
    ario::Timer wait_finish(*main_executor_, stop_at);
    wait_finish.AsyncWaitBigCallback(
        [this, &mutex, &cv, &should_join](ario::ErrorCode) {
          std::unique_lock lock(mutex);
          should_join = true;
          lock.unlock();
          cv.notify_all();
        });
    {
      std::unique_lock lock(mutex);
      cv.wait(lock, [&should_join] { return should_join; });
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

    FILE* fdump = nullptr;
    if (!options_.dump_schedule.empty()) {
      fdump = OpenLogFile(options_.dump_schedule);
    }

    char buf[256];
    snprintf(buf, sizeof(buf), "%-12s %8s %8s %8s %8s %8s %8s", "model_name",
             "noreply", "dropped", "timeout", "success", "total", "badrate");
    LOG(INFO) << "Stats:";
    LOG(INFO) << "  " << buf;
    int sum_noreply = 0, sum_dropped = 0, sum_timeout = 0, sum_success = 0;
    double worst_badrate = 0.0;
    double throughput = 0;
    for (size_t i = 0; i < options_.models.size(); ++i) {
      auto& l = loadgen_contexts_[i];
      auto& frontend = frontends_[i];
      int cnt_noreply = 0, cnt_dropped = 0, cnt_timeout = 0, cnt_success = 0;
      auto n = loadgen_contexts_[i].last_query_id - 1;
      auto bench_start_ns = start_at_.time_since_epoch().count() +
                            l.start_offset_ns +
                            static_cast<long>(l.warmup_duration * 1e9);
      auto bench_end_ns =
          bench_start_ns + static_cast<long>(l.bench_duration * 1e9);
      size_t per_second_length =
          static_cast<size_t>(std::ceil(l.bench_duration));
      std::vector<int> per_second_send(per_second_length),
          per_second_good(per_second_length);
      for (size_t j = 1; j <= n; ++j) {
        const auto& qctx = frontend->queries()[j];
        if (qctx.frontend_recv_ns < bench_start_ns ||
            qctx.frontend_recv_ns > bench_end_ns) {
          continue;
        }
        long sec = (qctx.frontend_recv_ns - bench_start_ns) / 1000000000L;
        CHECK(0 <= sec && sec < per_second_length);
        ++per_second_send[sec];
        switch (qctx.status) {
          case FakeFrontendDelegate::QueryStatus::kDropped:
            ++cnt_dropped;
            break;
          case FakeFrontendDelegate::QueryStatus::kTimeout:
            ++cnt_timeout;
            break;
          case FakeFrontendDelegate::QueryStatus::kSuccess:
            ++cnt_success;
            ++per_second_good[sec];
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
      throughput += total / l.bench_duration;

      if (fdump) {
        fprintf(fdump, "PERSECOND-SEND %zu", i);
        for (int x : per_second_send) fprintf(fdump, " %d", x);
        fprintf(fdump, "\n");

        fprintf(fdump, "PERSECOND-GOOD %zu", i);
        for (int x : per_second_good) fprintf(fdump, " %d", x);
        fprintf(fdump, "\n");
      }
    }

    int total_queries = sum_dropped + sum_timeout + sum_success + sum_noreply;
    double avg_badrate = 100.0 - sum_success * 100.0 / total_queries;
    snprintf(buf, sizeof(buf),
             "%-12s %8d %8d %8d %8d %8d (avg %.3f%%, worst %.3f%%)", "TOTAL",
             sum_noreply, sum_dropped, sum_timeout, sum_success, total_queries,
             avg_badrate, worst_badrate);
    LOG(INFO) << "  " << buf;
    LOG(INFO) << "  Throughput: " << throughput << " rps";

    if (fdump) {
      DumpBatchplan(fdump);
      CloseLogFile(options_.dump_schedule, fdump);
      LOG(INFO) << "BatchPlan dumped to " << options_.dump_schedule;
    }

    if (sum_noreply) {
      LOG(ERROR) << "Buggy scheduler. There are " << sum_noreply
                 << " queries having no reply.";
      return 1;
    }
    return 0;
  }

 private:
  void BuildWorkloads() {
    loadgen_contexts_.resize(options_.models.size());
    for (size_t i = 0; i < options_.models.size(); ++i) {
      InitLoadGen(i);
    }
  }

  void BuildMultiThreadRankScheduler() {
    scheduler_ = std::make_unique<MultiThreadRankScheduler>(
        rankmt_options_, main_executor_.get(), rank_executor_.get());
  }

  void BuildFakeServers() {
    uint32_t next_backend_id = 10001;
    for (int i = 0; i < options_.gpus; ++i) {
      bool save_archive = !options_.dump_schedule.empty();
      auto backend_id = next_backend_id++;
      auto backend = std::make_shared<FakeBackendDelegate>(
          main_executor_.get(), backend_id, &accessor_, save_archive);
      accessor_.AddBackend(NodeId(backend_id), backend);
      scheduler_->AddBackend(NodeId(backend_id), backend);
      backends_.push_back(backend);
    }

    for (size_t i = 0; i < options_.models.size(); ++i) {
      const auto& w = options_.models[i];
      auto& l = loadgen_contexts_[i];
      uint32_t frontend_id = 60001 + i;
      auto frontend = std::make_shared<FakeFrontendDelegate>(
          [this](size_t cnt_done, size_t workload_idx) {}, frontend_id,
          w.model_session, i, CalcReservedSize(i));
      accessor_.AddFrontend(NodeId(frontend_id), frontend);
      scheduler_->AddFrontend(NodeId(frontend_id), frontend);
      frontends_.push_back(frontend);
      l.frontend = frontend.get();

      if (options_.multithread) {
        model_executors_.push_back(
            std::make_shared<ario::EpollExecutor>(ario::PollerType::kSpinning));
      } else {
        model_executors_.push_back(main_executor_);
      }
      auto entrance = scheduler_->AddModelSession(model_executors_.back().get(),
                                                  w.model_session);
      request_entrances_.push_back(entrance);
      model_index_table_.push_back(entrance.model_index());
      CHECK_EQ(entrance.model_index() + 1, model_index_table_.size());
    }
  }

  void InitLoadGen(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    const auto& w = options_.models[workload_idx];
    l.seed = options_.seed + workload_idx * 31;
    l.trace.insert(l.trace.end(), w.warmup.begin(), w.warmup.end());
    l.trace.insert(l.trace.end(), w.bench.begin(), w.bench.end());
    l.trace.insert(l.trace.end(), w.cooldown.begin(), w.cooldown.end());
    l.warmup_duration = l.bench_duration = l.cooldown_duration = 0;
    for (const auto& ts : w.warmup) l.warmup_duration += ts.duration;
    for (const auto& ts : w.bench) l.bench_duration += ts.duration;
    for (const auto& ts : w.cooldown) l.cooldown_duration += ts.duration;

    double init_rps = l.trace.at(0).rps;
    std::mt19937 gen(l.seed);
    std::uniform_real_distribution<> dist;
    l.start_offset_ns = dist(gen) * 1e9 / init_rps;

    l.ts_idx = 0;
    l.next_time = start_at_ + std::chrono::nanoseconds(l.start_offset_ns);
    l.ts_end_at = l.next_time;

    l.last_global_id = 1000000000 * (workload_idx + 1);
    l.last_query_id = 0;
    l.model_session_id = ModelSessionToString(w.model_session);
  }

  size_t CalcReservedSize(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    double sz = 0;
    for (const auto& ts : l.trace) {
      sz += (1.0 + std::sqrt(ts.rps)) * ts.rps * ts.duration * 3;
    }
    return static_cast<size_t>(sz);
  }

  void PrepareNextRequest(size_t workload_idx) {
    auto& l = loadgen_contexts_[workload_idx];
    const auto& workload = options_.models[workload_idx];
    auto gap_ns = static_cast<long>(l.gap_gen->Next() * 1e9);
    l.next_time += std::chrono::nanoseconds(gap_ns);
    auto next_time_ns = l.next_time.time_since_epoch().count();
    auto query_id = ++l.last_query_id;
    auto global_id = ++l.last_global_id;
    CHECK_LT(query_id, l.frontend->reserved_size())
        << "Reserved size not big enough.";
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

  void PostNextRequest(size_t workload_idx) {
    auto& w = options_.models[workload_idx];
    auto& l = loadgen_contexts_[workload_idx];
    if (l.next_time >= l.ts_end_at) {
      if (l.ts_idx == l.trace.size()) {
        LOG(INFO) << "[" << l.model_session_id << "] Finished sending";
        return;
      }
      const auto& ts = l.trace[l.ts_idx];
      LOG(INFO) << "[" << l.model_session_id
                << "] Next TraceSegment idx=" << l.ts_idx << " rps=" << ts.rps
                << " duration=" << ts.duration;
      l.gap_gen = w.gap_builder.Build(l.seed + l.ts_idx, ts.rps);
      l.ts_end_at +=
          std::chrono::nanoseconds(static_cast<long>(ts.duration * 1e9));
      l.ts_idx += 1;
    }

    PrepareNextRequest(workload_idx);
    l.timer.SetTimeout(l.next_time);
    l.timer.AsyncWait([this, workload_idx](ario::ErrorCode ec) {
      if (ec != ario::ErrorCode::kOk) return;
      auto& entrance = request_entrances_[workload_idx];
      auto& l = loadgen_contexts_[workload_idx];

      entrance.EnqueueQuery(std::move(l.request));
      PostNextRequest(workload_idx);
    });
  }

  FILE* OpenLogFile(const std::string& path) {
    if (options_.dump_schedule == "stdout") {
      return stdout;
    }
    FILE* f = fopen(options_.dump_schedule.c_str(), "w");
    if (!f) {
      LOG(ERROR) << "Cannot open log file to write: " << options_.dump_schedule;
      return nullptr;
    }
    return f;
  }

  void CloseLogFile(const std::string& path, FILE* f) {
    if (options_.dump_schedule != "stdout") {
      fclose(f);
    }
  }

  void DumpBatchplan(FILE* f) {
    for (size_t backend_idx = 0; backend_idx < backends_.size();
         ++backend_idx) {
      int gpu_idx = backend_idx;
      const auto& archive = backends_[backend_idx]->batchplan_archive();
      for (const auto& p : archive) {
        int model_idx = p.model_index();
        int plan_id = p.plan_id();
        int batch_size = p.queries_size();
        const auto& l = loadgen_contexts_[model_idx];
        auto bench_start_ns = start_at_.time_since_epoch().count() +
                              static_cast<long>(l.warmup_duration * 1e9);
        double exec_at = (p.exec_time_ns() - bench_start_ns) / 1e9;
        double finish_at = (p.expected_finish_time_ns() - bench_start_ns) / 1e9;
        if (exec_at < 0 || finish_at > l.bench_duration) continue;
        fprintf(f, "BATCHPLAN %d %d %d %d %.9f %.9f\n", plan_id, gpu_idx,
                model_idx, batch_size, exec_at, finish_at);
      }
    }
  }

  struct LoadGenContext {
    ario::Timer timer;

    long seed;
    std::vector<TraceSegment> trace;
    double warmup_duration;
    double bench_duration;
    double cooldown_duration;
    long start_offset_ns;

    uint64_t last_global_id;
    uint64_t last_query_id;
    std::string model_session_id;
    FakeFrontendDelegate* frontend;
    DispatchRequest request;

    std::unique_ptr<GapGenerator> gap_gen;
    size_t ts_idx;
    TimePoint ts_end_at;
    TimePoint next_time;
  };

  Options options_;
  RankmtConfig rankmt_options_;
  std::shared_ptr<ario::EpollExecutor> main_executor_;
  std::shared_ptr<ario::EpollExecutor> rank_executor_;
  std::vector<std::shared_ptr<ario::EpollExecutor>> model_executors_;
  std::vector<LoadGenContext> loadgen_contexts_;
  std::unique_ptr<MultiThreadRankScheduler> scheduler_;
  std::vector<MultiThreadRankScheduler::RequestEntrance> request_entrances_;
  std::vector<ModelIndex> model_index_table_;
  FakeDispatcherAccessor accessor_;
  std::vector<std::shared_ptr<FakeBackendDelegate>> backends_;
  std::vector<std::shared_ptr<FakeFrontendDelegate>> frontends_;

  std::vector<std::thread> threads_;
  TimePoint start_at_;
};

std::vector<TraceSegment> BuildTraceFromYaml(const YAML::Node& yaml,
                                             double segment_duration) {
  CHECK(yaml.IsSequence());
  std::vector<TraceSegment> ts;
  ts.reserve(yaml.size());
  for (const auto& node : yaml) {
    if (node.IsMap()) {
      auto rps = node["rps"].as<double>();
      auto duration = node["duration"].as<double>();
      ts.push_back({rps, duration});
    } else {
      CHECK(node.IsScalar());
      auto rps = node.as<double>();
      ts.push_back({rps, segment_duration});
    }
  }
  return ts;
}

Options Options::FromYaml(const YAML::Node& config) {
  Options opt;

  // Random seed
  opt.seed = config["seed"].as<int64_t>(0xabcdabcd987LL);

  // Use multiple threads to run RankMT
  opt.multithread = config["multithread"].as<bool>(false);

  // Dump Schedule. Options: `false`, `stdout`, or path
  if (config["dump_schedule"].as<bool>(true)) {
    opt.dump_schedule = config["dump_schedule"].as<std::string>("");
  }

  // Number of GPUs
  opt.gpus = config["gpus"].as<int>(1);

  // Models
  CHECK(config["models"].IsSequence());
  for (const auto& model : config["models"]) {
    ModelOptions mo;
    mo.model_session.set_version(1);

    // Model framework
    mo.model_session.set_framework(
        model["framework"].as<std::string>("tensorflow"));

    // Model name
    mo.model_session.set_model_name(model["model"].as<std::string>());

    // Model latency SLO in milliseconds
    mo.model_session.set_latency_sla(model["slo"].as<int>());

    // Request interval gap
    mo.gap_builder = GapGeneratorBuilder::Parse(model["gap"].as<std::string>());

    // Segment duration in seconds
    double segment_duration = model["segment_duration"].as<double>(0.0);

    // Trace segments
    mo.warmup = BuildTraceFromYaml(model["warmup"], segment_duration);
    mo.bench = BuildTraceFromYaml(model["bench"], segment_duration);
    mo.cooldown = BuildTraceFromYaml(model["cooldown"], segment_duration);

    opt.models.push_back(std::move(mo));
  }

  return opt;
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  FLAGS_colorlogtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, false);

  auto config = YAML::Load(std::cin);
  CHECK(config.IsMap());
  auto options = Options::FromYaml(config);

  google::InstallFailureSignalHandler();
  DispatcherRunner bencher(std::move(options), RankmtConfig::FromFlags());
  return bencher.Run();
}
