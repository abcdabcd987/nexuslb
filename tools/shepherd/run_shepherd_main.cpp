#include <gflags/gflags.h>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <boost/asio/io_context.hpp>
#include <boost/asio/system_timer.hpp>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "nexus/common/gapgen.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/proto/nnquery.pb.h"
#include "shepherd/common.h"
#include "shepherd/fake_accessor.h"
#include "shepherd/fake_shepherd_backend.h"
#include "shepherd/fake_shepherd_frontend.h"
#include "shepherd/flex_scheduler.h"

using namespace nexus;
using namespace nexus::shepherd;

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
  std::string dump_schedule;
  int gpus;
  std::vector<ModelOptions> models;
  double bail_badrate;

  static Options FromYaml(const YAML::Node& config);
};

class ShepherdRunner {
 public:
  explicit ShepherdRunner(Options options, ShepherdConfig shepherd_cfg)
      : options_(std::move(options)), shepherd_cfg_(std::move(shepherd_cfg)) {
    LOG(INFO) << "Preparing the benchmark";
    start_at_ = Clock::now() + std::chrono::seconds(2);
    BuildScheduler();
    BuildWorkloads();
    BuildFakeServers();
  }

  int Run() {
    for (size_t i = 0; i < options_.models.size(); ++i) {
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
      duration += l.warmup_duration + l.bench_duration + l.cooldown_duration;
      long ns = l.start_offset_ns + duration * 1e9;
      stop_offset_ns = std::max(stop_offset_ns, ns);
    }
    auto stop_at = start_at_ + std::chrono::nanoseconds(stop_offset_ns);

    stop_signal_ = false;
    bailed_out_ = false;
    boost::asio::system_timer wait_finish(io_context_, stop_at);
    wait_finish.async_wait([this](boost::system::error_code ec) {
      std::unique_lock lock(stop_mutex_);
      stop_signal_ = true;
      lock.unlock();
      stop_cv_.notify_all();
    });
    {
      std::unique_lock lock(stop_mutex_);
      stop_cv_.wait(lock, [this] { return stop_signal_; });
    }

    scheduler_->Stop();
    for (auto& backend : backends_) {
      backend->Stop();
    }
    io_context_.stop();

    for (auto iter = threads_.rbegin(); iter != threads_.rend(); ++iter) {
      iter->join();
    }
    LOG(INFO) << "All threads joined.";
    for (auto& backend : backends_) {
      backend->DrainBatchPlans();
    }

    FILE* fdump = nullptr;
    if (!bailed_out_ && !options_.dump_schedule.empty()) {
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
          case FakeShepherdFrontend::QueryStatus::kDropped:
            ++cnt_dropped;
            break;
          case FakeShepherdFrontend::QueryStatus::kTimeout:
            ++cnt_timeout;
            break;
          case FakeShepherdFrontend::QueryStatus::kSuccess:
            ++cnt_success;
            ++per_second_good[sec];
            break;
          default:
            ++cnt_noreply;
            break;
        }
      }
      const auto& model_name = "model_" + std::to_string(l.model_id);
      int total = cnt_dropped + cnt_timeout + cnt_success + cnt_noreply;
      double badrate = total > 0 ? 100.0 - cnt_success * 100.0 / total : 100.0;
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
    double avg_badrate =
        total_queries > 0 ? 100.0 - sum_success * 100.0 / total_queries : 100.0;
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
    loadgen_contexts_.reserve(options_.models.size());
    for (size_t i = 0; i < options_.models.size(); ++i) {
      loadgen_contexts_.emplace_back(&io_context_, i, i);
    }
    for (size_t i = 0; i < options_.models.size(); ++i) {
      InitLoadGen(i);
    }
  }

  void BuildScheduler() {
    scheduler_ = std::make_unique<FlexScheduler>(&io_context_, shepherd_cfg_);
  }

  void BuildFakeServers() {
    int next_backend_id = 10001;
    for (int i = 0; i < options_.gpus; ++i) {
      bool save_archive = !options_.dump_schedule.empty();
      auto backend_id = next_backend_id++;
      auto backend = std::make_shared<FakeShepherdBackend>(
          &io_context_, &accessor_, backend_id, save_archive);
      backends_.push_back(backend);
      scheduler_->AddGpu(backend_id, &backend->stub());
    }

    for (size_t i = 0; i < options_.models.size(); ++i) {
      const auto& w = options_.models[i];
      auto& l = loadgen_contexts_[i];
      int model_id = i;
      int slo_ms = w.model_session.latency_sla();
      auto frontend = std::make_shared<FakeShepherdFrontend>(
          model_id, slo_ms, i, CalcReservedSize(i), l.global_id_offset);
      accessor_.AddFrontend(model_id, frontend);
      scheduler_->AddModel(
          model_id, slo_ms,
          ModelDatabase::Singleton().GetModelProfile(
              "FakeGPU", "FakeUUID", ModelSessionToProfileID(w.model_session)));
      frontends_.push_back(frontend);
      l.frontend = frontend.get();
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

    l.global_id_offset = 10000000 * (workload_idx + 1);
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
    CHECK_LT(query_id, l.frontend->reserved_size())
        << "Reserved size not big enough.";

    l.query.query_id = query_id + l.global_id_offset;
    l.query.model_id = l.model_id;
    l.query.arrival_at = l.next_time;

    l.frontend->ReceivedQuery(l.query.query_id, next_time_ns);
  }

  void PostNextRequest(size_t workload_idx) {
    if (stop_signal_) {
      return;
    }
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
      scheduler_->AddQuery(l.query, l.frontend);

      PostNextRequest(workload_idx);
    });

    // Bail out if bad rate is too high.
    auto cnt_total = l.frontend->cnt_total(), cnt_bad = l.frontend->cnt_bad();
    if (cnt_bad > cnt_total * options_.bail_badrate &&
        (l.next_time - start_at_).count() - l.start_offset_ns >
            static_cast<long>(l.warmup_duration * 1e9)) {
      LOG(INFO) << "[" << l.model_session_id
                << "] Bad rate too high, bailing out... bad rate: " << cnt_bad
                << " / " << cnt_total << " = " << (cnt_bad * 100.0 / cnt_total)
                << " %";
      for (auto& lctx : loadgen_contexts_) {
        lctx.timer.cancel();
      }
      bailed_out_ = true;
      stop_signal_ = true;
      stop_cv_.notify_all();
    }
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
    std::vector<std::vector<long>> queue_delay(options_.models.size());
    for (size_t backend_idx = 0; backend_idx < backends_.size();
         ++backend_idx) {
      int gpu_idx = backend_idx;
      const auto& archive = backends_[backend_idx]->batchplan_archive();
      for (const auto& p : archive) {
        int model_idx = p.model_id;
        int plan_id = p.batch_id;
        int batch_size = p.query_ids.size();
        const auto& l = loadgen_contexts_[model_idx];
        auto bench_start_ns = start_at_.time_since_epoch().count() +
                              static_cast<long>(l.warmup_duration * 1e9);
        double exec_at = (p.exec_time_ns() - bench_start_ns) / 1e9;
        double finish_at = (p.expected_finish_time_ns() - bench_start_ns) / 1e9;
        if (exec_at < 0 || finish_at > l.bench_duration) continue;
        fprintf(f, "BATCHPLAN %d %d %d %d %.9f %.9f\n", plan_id, gpu_idx,
                model_idx, batch_size, exec_at, finish_at);

        for (auto global_id : p.query_ids) {
          auto query_id = global_id - l.global_id_offset;
          auto& qctx = l.frontend->queries()[query_id];
          queue_delay[model_idx].push_back(p.exec_time_ns() -
                                           qctx.frontend_recv_ns);
        }
      }
    }
    for (size_t i = 0; i < queue_delay.size(); ++i) {
      fprintf(f, "QUEUE-DELAY %zu", i);
      for (auto x : queue_delay[i]) {
        fprintf(f, " %ld", x);
      }
      fprintf(f, "\n");
    }
  }

  struct LoadGenContext {
    boost::asio::system_timer timer;
    std::string model_session_id;
    size_t model_idx;
    FakeShepherdFrontend* frontend;

    long seed;
    std::vector<TraceSegment> trace;
    double warmup_duration;
    double bench_duration;
    double cooldown_duration;
    long start_offset_ns;

    int global_id_offset;
    int last_query_id;
    int model_id;

    std::unique_ptr<GapGenerator> gap_gen;
    size_t ts_idx;
    TimePoint ts_end_at;
    TimePoint next_time;
    Query query;

    LoadGenContext(boost::asio::io_context* io_context, size_t model_idx,
                   int model_id)
        : timer(*io_context), model_idx(model_idx), model_id(model_id) {}
  };

  Options options_;
  ShepherdConfig shepherd_cfg_;
  boost::asio::io_context io_context_;
  std::vector<LoadGenContext> loadgen_contexts_;
  std::unique_ptr<FlexScheduler> scheduler_;
  FakeObjectAccessor accessor_;
  std::vector<std::shared_ptr<FakeShepherdBackend>> backends_;
  std::vector<std::shared_ptr<FakeShepherdFrontend>> frontends_;

  std::vector<std::thread> threads_;
  TimePoint start_at_;
  bool stop_signal_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool bailed_out_;
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

  // Dump Schedule. Options: `false`, `stdout`, or path
  if (config["dump_schedule"].as<bool>(true)) {
    opt.dump_schedule = config["dump_schedule"].as<std::string>("");
  }

  // Number of GPUs
  opt.gpus = config["gpus"].as<int>(1);

  // Bail if bad rate is higher than this threshold. [0.0, 1.0)
  opt.bail_badrate = config["bail_badrate"].as<double>(1.0);

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
  ShepherdRunner bencher(std::move(options), ShepherdConfig::FromFlags());
  return bencher.Run();
}
