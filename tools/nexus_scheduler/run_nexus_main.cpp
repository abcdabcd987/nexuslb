#include <gflags/gflags.h>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <atomic>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
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

#include "nexus/common/gapgen.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/util.h"
#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/backend_delegate.h"
#include "nexus_scheduler/fake_nexus_backend.h"
#include "nexus_scheduler/fake_nexus_frontend.h"
#include "nexus_scheduler/fake_object_accessor.h"
#include "nexus_scheduler/frontend_delegate.h"
#include "nexus_scheduler/query_collector.h"
#include "nexus_scheduler/scheduler.h"

using namespace nexus;
using namespace nexus::app;
using namespace nexus::backend;
using namespace nexus::scheduler;

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
  int frontends;
  int backends;
  std::string dump_schedule;
  std::vector<ModelOptions> models;
  YAML::Node static_schedule;

  static Options FromYaml(const YAML::Node& config);
};

class NexusRunner {
 public:
  explicit NexusRunner(Options options)
      : options_(std::move(options)), ema_report_timer_(io_context_) {
    LOG(INFO) << "Preparing the benchmark";
    accessor_.query_collector = &query_collector_;
    BuildScheduler();
    BuildFakeServers();
    start_at_ = Clock::now() + std::chrono::seconds(2);
    BuildClients();
  }

  int Run() {
    ReportTimerDelay();
    scheduler_->Start();
    for (auto& backend : backends_) {
      backend->Start();
    };
    for (auto& frontend : frontends_) {
      frontend->Start();
    }
    for (size_t i = 0; i < loadgen_contexts_.size(); ++i) {
      auto& l = loadgen_contexts_[i];
      l.timer.expires_at(l.next_time);
      l.timer.async_wait([this, i](boost::system::error_code ec) {
        if (ec) return;
        PostNextRequest(i);
      });
    }

    threads_.emplace_back([this] {
      while (!io_context_.stopped()) {
        io_context_.poll();
      }
    });

    long stop_offset_ns = 0;
    for (size_t i = 0; i < options_.models.size(); ++i) {
      auto& w = options_.models[i];
      auto& l = loadgen_contexts_[i];
      double duration = w.model_session.latency_sla() / 1e3 * 2;
      duration += l.trace.warmup_duration + l.trace.bench_duration +
                  l.trace.cooldown_duration;
      long ns = l.start_offset_ns + duration * 1e9;
      stop_offset_ns = std::max(stop_offset_ns, ns);
    }
    auto stop_at = start_at_ + std::chrono::nanoseconds(stop_offset_ns);

    std::mutex mutex;
    std::condition_variable cv;
    bool should_join = false;
    boost::asio::system_timer wait_finish(io_context_, stop_at);
    wait_finish.async_wait(
        [this, &mutex, &cv, &should_join](boost::system::error_code) {
          std::unique_lock lock(mutex);
          should_join = true;
          lock.unlock();
          cv.notify_all();
        });
    {
      std::unique_lock lock(mutex);
      cv.wait(lock, [&should_join] { return should_join; });
    }

    for (auto& backend : backends_) {
      backend->Stop();
    };
    for (auto& frontend : frontends_) {
      frontend->Stop();
    }
    scheduler_->Stop();
    io_context_.stop();
    ReportTimerDelay();

    for (auto iter = threads_.rbegin(); iter != threads_.rend(); ++iter) {
      iter->join();
    }
    LOG(INFO) << "All threads joined.";

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
      const auto& m = options_.models[i];
      auto trace = BuildTrace(m);
      int cnt_noreply = 0, cnt_dropped = 0, cnt_timeout = 0, cnt_success = 0;
      size_t n = query_collector_.last_query_id(i)->load();
      auto bench_start_ns =
          start_at_.time_since_epoch().count() + trace.warmup_duration * 1e9;
      auto bench_end_ns = bench_start_ns + trace.bench_duration * 1e9;
      size_t per_second_length =
          static_cast<size_t>(std::ceil(trace.bench_duration));
      std::vector<int> per_second_send(per_second_length),
          per_second_good(per_second_length);
      const auto& queries = query_collector_.queries(i);
      for (size_t j = 0; j < n; ++j) {
        const auto& qctx = queries[j];
        if (qctx.frontend_recv_ns < bench_start_ns ||
            qctx.frontend_recv_ns > bench_end_ns) {
          continue;
        }
        long sec = (qctx.frontend_recv_ns - bench_start_ns) / 1000000000L;
        CHECK(0 <= sec && sec < per_second_length);
        ++per_second_send[sec];
        switch (qctx.status) {
          case QueryCollector::QueryStatus::kDropped:
            ++cnt_dropped;
            break;
          case QueryCollector::QueryStatus::kTimeout:
            ++cnt_timeout;
            break;
          case QueryCollector::QueryStatus::kSuccess:
            ++cnt_success;
            ++per_second_good[sec];
            break;
          default:
            ++cnt_noreply;
            break;
        }
      }
      const auto& model_name = options_.models[i].model_session.model_name();
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
      throughput += total / trace.bench_duration;

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
  struct TraceContext {
    std::vector<TraceSegment> trace;
    double warmup_duration;
    double bench_duration;
    double cooldown_duration;
  };

  void BuildScheduler() {
    scheduler_ = std::make_unique<Scheduler>(&io_context_, &accessor_,
                                             Scheduler::Config::Default());
    accessor_.scheduler = scheduler_.get();
    if (options_.static_schedule) {
      scheduler_->LoadWorkload(options_.static_schedule);
    }
  }

  void BuildFakeServers() {
    std::vector<ModelSession> model_sessions;
    double total_rps = 0;
    for (size_t i = 0; i < options_.models.size(); ++i) {
      const auto& m = options_.models[i];
      model_sessions.push_back(m.model_session);
      long slo_ns = m.model_session.latency_sla() * 1000000L;

      auto trace = BuildTrace(m);
      query_collector_.AddModel(CalcReservedSize(trace), slo_ns);
      double avg_rps = 0, duration = 0;
      for (const auto& ts : trace.trace) {
        avg_rps += ts.duration * ts.rps;
        duration += ts.duration;
      }
      avg_rps /= duration;
      total_rps += total_rps;
    }
    ema_alpha_ = 1 - std::pow(0.5, 1 / total_rps);

    uint32_t next_backend_id = 10001;
    for (int i = 0; i < options_.backends; ++i) {
      auto backend_id = next_backend_id++;
      auto backend = std::make_shared<FakeNexusBackend>(
          &io_context_, &accessor_, backend_id, model_sessions);
      backends_.push_back(backend);
      accessor_.backends[backend_id] = backend.get();

      // Register backend at scheduler
      scheduler_->RegisterBackend(std::make_shared<BackendDelegate>(
          &accessor_, backend_id, "FakeGPU", "FakeUUID"));
    }

    for (int i = 0; i < options_.frontends; ++i) {
      uint32_t frontend_id = 60001 + i;
      auto frontend = std::make_shared<FakeNexusFrontend>(
          &io_context_, &accessor_, frontend_id);
      frontends_.push_back(frontend);
      accessor_.frontends[frontend_id] = frontend.get();

      // Register frontend at scheduler
      scheduler_->RegisterFrontend(
          std::make_shared<FrontendDelegate>(&accessor_, frontend_id));

      // Load model to the frontend
      for (const auto& w : options_.models) {
        LoadModelRequest req;
        req.set_node_id(frontend_id);
        req.mutable_model_session()->CopyFrom(w.model_session);
        NexusLoadModelReply reply;
        scheduler_->LoadModel(req, &reply);
        frontend->LoadModel(reply);
      }
    }
  }

  void BuildClients() {
    for (int i = 0; i < options_.frontends; ++i) {
      for (size_t j = 0; j < options_.models.size(); ++j) {
        loadgen_contexts_.push_back(LoadGenContext(&io_context_, i, j));
        InitLoadGen(loadgen_contexts_.size() - 1);
      }
    }
  }

  TraceContext BuildTrace(const ModelOptions& w) {
    TraceContext l;
    l.trace.insert(l.trace.end(), w.warmup.begin(), w.warmup.end());
    l.trace.insert(l.trace.end(), w.bench.begin(), w.bench.end());
    l.trace.insert(l.trace.end(), w.cooldown.begin(), w.cooldown.end());
    l.warmup_duration = l.bench_duration = l.cooldown_duration = 0;
    for (const auto& ts : w.warmup) l.warmup_duration += ts.duration;
    for (const auto& ts : w.bench) l.bench_duration += ts.duration;
    for (const auto& ts : w.cooldown) l.cooldown_duration += ts.duration;
    return l;
  }

  void InitLoadGen(size_t loadgen_idx) {
    auto& l = loadgen_contexts_[loadgen_idx];
    l.frontend = frontends_[l.client_idx % frontends_.size()].get();
    l.model_handler = l.frontend->GetModelHandler(l.model_idx);

    const auto& w = options_.models[l.model_idx];
    l.seed = options_.seed + loadgen_idx * 31;
    l.trace = BuildTrace(w);
    for (auto& ts : l.trace.trace) {
      ts.rps /= options_.frontends;
    }

    double init_rps = l.trace.trace.at(0).rps;
    std::mt19937 gen(l.seed);
    std::uniform_real_distribution<> dist;
    l.start_offset_ns = dist(gen) * 1e9 / init_rps;

    l.ts_idx = 0;
    l.next_time = start_at_ + std::chrono::nanoseconds(l.start_offset_ns);
    l.ts_end_at = l.next_time;

    l.last_query_id = query_collector_.last_query_id(l.model_idx);
    l.model_session_id = ModelSessionToString(w.model_session);
  }

  size_t CalcReservedSize(const TraceContext& l) {
    double sz = 0;
    for (const auto& ts : l.trace) {
      sz += (1.0 + std::sqrt(ts.rps)) * ts.rps * ts.duration * 3;
    }
    return static_cast<size_t>(sz);
  }

  void PrepareNextRequest(size_t loadgen_idx) {
    auto& l = loadgen_contexts_[loadgen_idx];
    const auto& workload = options_.models[l.model_idx];
    auto gap_ns = static_cast<long>(l.gap_gen->Next() * 1e9);
    l.next_time += std::chrono::nanoseconds(gap_ns);
    auto next_time_ns = l.next_time.time_since_epoch().count();
    auto query_id = l.last_query_id->fetch_add(1);
    auto* query = &l.query;
    query->set_query_id(query_id);
    query->set_frontend_id(l.frontend->node_id());
    auto* clock = query->mutable_clock();
    clock->set_frontend_recv_ns(next_time_ns);

    query_collector_.ReceivedQuery(l.model_idx, query_id, next_time_ns);
  }

  void PostNextRequest(size_t loadgen_idx) {
    auto& l = loadgen_contexts_[loadgen_idx];
    auto& w = options_.models[l.model_idx];
    if (l.next_time >= l.ts_end_at) {
      if (l.ts_idx == l.trace.trace.size()) {
        LOG(INFO) << "[" << l.model_session_id << "] Finished sending";
        return;
      }
      const auto& ts = l.trace.trace[l.ts_idx];
      LOG(INFO) << "[" << l.model_session_id
                << "] Next TraceSegment idx=" << l.ts_idx << " rps=" << ts.rps
                << " duration=" << ts.duration;
      l.gap_gen = w.gap_builder.Build(l.seed + l.ts_idx, ts.rps);
      l.ts_end_at +=
          std::chrono::nanoseconds(static_cast<long>(ts.duration * 1e9));
      l.ts_idx += 1;
    }

    PrepareNextRequest(loadgen_idx);
    l.timer.expires_at(l.next_time);
    l.timer.async_wait([this, loadgen_idx](boost::system::error_code ec) {
      if (ec) return;
      auto& l = loadgen_contexts_[loadgen_idx];
      auto timer_delay_us = (Clock::now() - l.timer.expires_at()).count() / 1e3;
      ema_timer_delay_us_ = (1 - ema_alpha_) * ema_timer_delay_us_.load() +
                            ema_alpha_ * timer_delay_us;
      ++cnt_timer_trigger_;
      if (timer_delay_us > 1000) {
        ++cnt_timer_delay_violation_;
        LOG(WARNING) << "CPU overloaded. timer_delay="
                     << static_cast<int>(timer_delay_us) << "us";
      }
      l.model_handler->counter()->Increase(1);
      auto backend_id = l.model_handler->GetBackend();
      auto backend = accessor_.backends.at(backend_id);
      backend->EnqueueQuery(l.model_idx, l.query);

      PostNextRequest(loadgen_idx);
    });
  }

  void ReportTimerDelay() {
    auto bad = cnt_timer_delay_violation_.load();
    auto all = cnt_timer_trigger_.load();
    LOG(INFO) << "LoadGen timer EMA delay="
              << static_cast<int>(ema_timer_delay_us_.load())
              << "us violation=" << bad << "/" << all << "="
              << (bad * 100.0 / all) << "%";
    ema_report_timer_.expires_after(std::chrono::seconds(1));
    ema_report_timer_.async_wait([this](boost::system::error_code ec) {
      if (ec) return;
      ReportTimerDelay();
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
    int next_plan_id = 1;
    for (size_t backend_idx = 0; backend_idx < backends_.size();
         ++backend_idx) {
      int gpu_idx = backend_idx;
      const auto& archive = backends_[backend_idx]->batchplan_archive();
      for (const auto& p : archive) {
        int model_idx = p.model_idx;
        int plan_id = next_plan_id++;
        int batch_size = p.batch_size;
        const auto& l = loadgen_contexts_[model_idx];
        auto bench_start_ns = start_at_.time_since_epoch().count() +
                              static_cast<long>(l.trace.warmup_duration * 1e9);
        double exec_at =
            (p.exec_at.time_since_epoch().count() - bench_start_ns) / 1e9;
        double finish_at =
            (p.finish_at.time_since_epoch().count() - bench_start_ns) / 1e9;
        if (exec_at < 0 || finish_at > l.trace.bench_duration) continue;
        fprintf(f, "BATCHPLAN %d %d %d %d %.9f %.9f\n", plan_id, gpu_idx,
                model_idx, batch_size, exec_at, finish_at);
      }
    }
  }

  struct LoadGenContext {
    boost::asio::system_timer timer;
    size_t client_idx;
    size_t model_idx;
    FakeNexusFrontend* frontend;
    std::shared_ptr<ModelHandler> model_handler;

    long seed;
    long start_offset_ns;
    TraceContext trace;

    std::atomic<uint64_t>* last_query_id;
    std::string model_session_id;
    QueryProto query;

    std::unique_ptr<GapGenerator> gap_gen;
    size_t ts_idx;
    TimePoint ts_end_at;
    TimePoint next_time;

    LoadGenContext(boost::asio::io_context* io_context, size_t client_idx,
                   size_t model_idx)
        : timer(*io_context), client_idx(client_idx), model_idx(model_idx) {}
  };

  Options options_;
  boost::asio::io_context io_context_;
  QueryCollector query_collector_;
  std::unique_ptr<Scheduler> scheduler_;
  std::vector<std::shared_ptr<FakeNexusBackend>> backends_;
  std::vector<std::shared_ptr<FakeNexusFrontend>> frontends_;
  FakeObjectAccessor accessor_;
  std::vector<LoadGenContext> loadgen_contexts_;

  std::vector<std::thread> threads_;
  TimePoint start_at_;
  boost::asio::system_timer ema_report_timer_;
  double ema_alpha_ = 0;
  std::atomic<double> ema_timer_delay_us_ = 0;
  std::atomic<int> cnt_timer_delay_violation_ = 0;
  std::atomic<int> cnt_timer_trigger_ = 0;
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

  // Number of frontends
  opt.frontends = config["frontends"].as<int>(1);

  // Number of GPUs
  opt.backends = config["backends"].as<int>(1);

  // Dump Schedule. Options: `false`, `stdout`, or path
  if (config["dump_schedule"].as<bool>(true)) {
    opt.dump_schedule = config["dump_schedule"].as<std::string>("");
  }

  // Static schedule
  opt.static_schedule = config["schedule"];

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
  NexusRunner bencher(std::move(options));
  return bencher.Run();
}
