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
  int clients;
  std::vector<ModelOptions> models;

  static Options FromYaml(const YAML::Node& config);
};

class DispatcherRunner {
 public:
  explicit DispatcherRunner(Options options) : options_(std::move(options)) {
    main_executor_ = std::make_shared<boost::asio::io_context>();

    LOG(INFO) << "Preparing the benchmark";
    BuildScheduler();
    BuildFakeServers();
    start_at_ = Clock::now() + std::chrono::seconds(2);
    BuildClients();
  }

  int Run() { return 0; }

 private:
  void BuildScheduler() {
    scheduler_ = std::make_unique<Scheduler>(main_executor_.get(), &accessor_,
                                             Scheduler::Config::Default());
    accessor_.scheduler = scheduler_.get();
  }

  void BuildFakeServers() {
    std::vector<ModelSession> model_sessions;
    for (size_t i = 0; i < options_.models.size(); ++i) {
      const auto& m = options_.models[i];
      model_sessions.push_back(m.model_session);
      long slo_ns = m.model_session.latency_sla() * 1000000L;
      query_collector_.AddModel(CalcReservedSize(i), slo_ns);
    }

    uint32_t next_backend_id = 10001;
    for (int i = 0; i < options_.backends; ++i) {
      auto backend_id = next_backend_id++;
      auto backend = std::make_shared<FakeNexusBackend>(
          main_executor_.get(), &accessor_, backend_id, model_sessions);
      backends_.push_back(backend);
      accessor_.backends[backend_id] = backend.get();

      // Register backend at scheduler
      scheduler_->RegisterBackend(std::make_shared<BackendDelegate>(
          &accessor_, backend_id, "FakeGPU", "FakeUUID"));
    }

    for (int i = 0; i < options_.frontends; ++i) {
      uint32_t frontend_id = 60001 + i;
      auto frontend = std::make_shared<FakeNexusFrontend>(
          main_executor_.get(), &accessor_, frontend_id);
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
    for (int i = 0; i < options_.clients; ++i) {
      for (size_t j = 0; j < options_.models.size(); ++j) {
        loadgen_contexts_.push_back(LoadGenContext(main_executor_.get(), i, j));
        InitLoadGen(loadgen_contexts_.size() - 1);
      }
    }
  }

  void InitLoadGen(size_t loadgen_idx) {
    auto& l = loadgen_contexts_[loadgen_idx];
    l.frontend = frontends_[l.client_idx % frontends_.size()].get();
    l.model_handler = l.frontend->GetModelHandler(l.model_idx);

    const auto& w = options_.models[l.model_idx];
    l.seed = options_.seed + loadgen_idx * 31;
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

    l.last_query_id = query_collector_.last_query_id(l.model_idx);
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
    auto query_id = l.last_query_id->fetch_add(1);
    auto* query = &l.query;
    query->set_query_id(query_id);
    query->set_frontend_id(l.frontend->node_id());
    auto* clock = query->mutable_clock();
    clock->set_frontend_recv_ns(next_time_ns);

    query_collector_.ReceivedQuery(l.model_idx, query_id, next_time_ns);
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
      auto backend_id = l.model_handler->GetBackend();
      auto backend = accessor_.backends.at(backend_id);
      backend->EnqueueQuery(l.model_idx, l.query);

      PostNextRequest(workload_idx);
    });
  }

  struct LoadGenContext {
    boost::asio::system_timer timer;
    size_t client_idx;
    size_t model_idx;
    FakeNexusFrontend* frontend;
    std::shared_ptr<ModelHandler> model_handler;

    long seed;
    std::vector<TraceSegment> trace;
    double warmup_duration;
    double bench_duration;
    double cooldown_duration;
    long start_offset_ns;

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
  std::shared_ptr<boost::asio::io_context> main_executor_;
  QueryCollector query_collector_;
  std::unique_ptr<Scheduler> scheduler_;
  std::vector<std::shared_ptr<FakeNexusBackend>> backends_;
  std::vector<std::shared_ptr<FakeNexusFrontend>> frontends_;
  FakeObjectAccessor accessor_;
  std::vector<LoadGenContext> loadgen_contexts_;

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

  // Number of frontends
  opt.frontends = config["frontends"].as<int>(1);

  // Number of GPUs
  opt.backends = config["backends"].as<int>(1);

  // Number of clients
  opt.clients = config["clients"].as<int>(1);
  CHECK(opt.clients % opt.frontends == 0);

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
  DispatcherRunner bencher(std::move(options));
  return bencher.Run();
}
