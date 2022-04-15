#include <gflags/gflags.h>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
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
  int clients;
  std::string dump_schedule;
  std::vector<ModelOptions> models;

  static Options FromYaml(const YAML::Node& config);
};

class DispatcherRunner {
 public:
  explicit DispatcherRunner(Options options) : options_(std::move(options)) {
    main_executor_ = std::make_shared<boost::asio::io_context>();

    LOG(INFO) << "Preparing the benchmark";
    start_at_ = Clock::now() + std::chrono::seconds(2);
    BuildWorkloads();
    BuildScheduler();
    BuildFakeServers();
  }

  int Run() { return 0; }

 private:
  void BuildWorkloads() {
    loadgen_contexts_.resize(options_.models.size());
    for (size_t i = 0; i < options_.models.size(); ++i) {
      InitLoadGen(i);
    }
  }

  void BuildScheduler() { scheduler_ = std::make_unique<Scheduler>(); }

  void BuildFakeServers() {
    std::vector<ModelSession> model_sessions;
    for (const auto& m : options_.models) {
      model_sessions.push_back(m.model_session);
    }

    uint32_t next_backend_id = 10001;
    for (int i = 0; i < options_.backends; ++i) {
      bool save_archive = !options_.dump_schedule.empty();
      auto backend_id = next_backend_id++;
      auto backend = std::make_shared<FakeNexusBackend>(
          main_executor_.get(), &accessor_, backend_id, model_sessions);
      backends_.push_back(backend);

      // Register backend at scheduler
      scheduler_->RegisterBackend(
          std::make_shared<BackendDelegate>(backend_id, "FakeGPU", "FakeUUID"));
    }

    for (int i = 0; i < options_.frontends; ++i) {
      uint32_t frontend_id = 60001 + i;
      auto frontend =
          std::make_shared<FakeNexusFrontend>(frontend_id, &accessor_);
      frontends_.push_back(frontend);
      auto& l = loadgen_contexts_[i];
      l.frontend = frontend.get();

      // Register frontend at scheduler
      scheduler_->RegisterFrontend(
          std::make_shared<FrontendDelegate>(frontend_id));

      // Load model to the frontend
      for (size_t j = 0; j < options_.models.size(); ++j) {
        const auto& w = options_.models[j];
        LoadModelRequest req;
        req.set_node_id(frontend_id);
        req.mutable_model_session()->CopyFrom(w.model_session);
        NexusLoadModelReply reply;
        scheduler_->LoadModel(req, &reply);
        frontend->LoadModel(reply, CalcReservedSize(j));
      }
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
    CHECK_LT(query_id, l.frontend->reserved_size(l.model_idx))
        << "Reserved size not big enough.";
    l.request.set_query_id(query_id);
    auto* query = l.request.mutable_query_without_input();
    query->set_query_id(query_id);
    query->set_global_id(global_id);
    query->set_frontend_id(l.frontend->node_id());
    auto* clock = query->mutable_clock();
    clock->set_frontend_recv_ns(next_time_ns);
    clock->set_frontend_dispatch_ns(next_time_ns);
    clock->set_dispatcher_recv_ns(next_time_ns);

    l.frontend->ReceivedQuery(l.model_idx, query_id, next_time_ns);
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
    l.timer.expires_at(l.next_time);
    l.timer.async_wait([this, workload_idx](boost::system::error_code ec) {
      if (ec) return;
      auto& l = loadgen_contexts_[workload_idx];
      l.model_handler->counter()->Increase(1);
      auto next_backend = l.model_handler->GetBackend();

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

  void DumpBatchplan() {
    FILE* f = OpenLogFile(options_.dump_schedule);
    if (!f) return;
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
    CloseLogFile(options_.dump_schedule, f);
    LOG(INFO) << "BatchPlan dumped to " << options_.dump_schedule;
  }

  struct LoadGenContext {
    size_t model_idx;
    FakeNexusFrontend* frontend;
    std::shared_ptr<ModelHandler> model_handler;
    boost::asio::system_timer timer;

    long seed;
    std::vector<TraceSegment> trace;
    double warmup_duration;
    double bench_duration;
    double cooldown_duration;
    long start_offset_ns;

    uint64_t last_global_id;
    uint64_t last_query_id;
    std::string model_session_id;
    DispatchRequest request;

    std::unique_ptr<GapGenerator> gap_gen;
    size_t ts_idx;
    TimePoint ts_end_at;
    TimePoint next_time;

    LoadGenContext(boost::asio::io_context* io_context) : timer(*io_context) {}
  };

  Options options_;
  std::shared_ptr<boost::asio::io_context> main_executor_;
  std::vector<LoadGenContext> loadgen_contexts_;
  std::unique_ptr<Scheduler> scheduler_;
  std::vector<std::shared_ptr<FakeNexusBackend>> backends_;
  std::vector<std::shared_ptr<FakeNexusFrontend>> frontends_;
  FakeObjectAccessor accessor_;

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
