#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>

#include <boost/asio.hpp>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <opencv2/opencv.hpp>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "nexus/common/connection.h"
#include "nexus/common/gapgen.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/util.h"
#include "nexus/proto/nnquery.pb.h"

using namespace nexus;

struct TracePoint {
  double rps;
  double duration;
};

struct Options {
  std::string host;
  int port;
  double sttime;
  ModelSession model_session;
  int width;
  int height;
  std::string imgext;
  int64_t seed;
  std::string gap;
  std::vector<TracePoint> trace;
  std::function<std::unique_ptr<GapGenerator>(double rps)> construct_gapgen;

  static Options FromYaml(size_t cfgidx, const YAML::Node& config);
};

class WorkloadSender {
 public:
  explicit WorkloadSender(boost::asio::io_context* main_io_context,
                          boost::asio::io_context* output_io_context,
                          size_t sender_idx, Options options)
      : sender_idx_(sender_idx),
        options_(std::move(options)),
        model_sess_id_(ModelSessionToString(options_.model_session)),
        message_handler_(this),
        main_io_context_(*CHECK_NOTNULL(main_io_context)),
        output_io_context_(*CHECK_NOTNULL(output_io_context)),
        timer_(main_io_context_) {
    PrepareRequestTemplate();
    done_ = false;
    Connect();
    StartSend();
  }

  ~WorkloadSender() { Stop(); }

  bool IsDone() { return done_; }

 private:
  class MessageHandler : public ::nexus::MessageHandler {
   public:
    explicit MessageHandler(WorkloadSender* outer)
        : outer_(*CHECK_NOTNULL(outer)) {}

    void HandleMessage(std::shared_ptr<Connection> conn,
                       std::shared_ptr<Message> message) override {
      if (message->type() != kUserReply) {
        LOG(FATAL) << "Received unknown message type " << message->type();
      }
      boost::asio::defer(outer_.output_io_context_, [this, message] {
        ReplyProto proto;
        bool ok = proto.ParseFromArray(message->body(), message->body_length());
        if (!ok) {
          LOG(FATAL) << "Failed to parse ReplyProto. message->body_length()="
                     << message->body_length();
        }
        outer_.OutputReply(std::move(proto));
      });
    }

    void HandleError(std::shared_ptr<Connection> conn,
                     boost::system::error_code ec) override {
      LOG(ERROR) << "Connection error: " << ec.message();
      outer_.Stop();
    }

    void HandleConnected(std::shared_ptr<Connection> conn) override {}

   private:
    WorkloadSender& outer_;
  };

  void Stop() {
    if (conn_) {
      conn_->Stop();
      conn_.reset();
    }

    timer_.cancel();
  }

  void PrepareRequestTemplate() {
    cv::Mat mat(cv::Size(options_.width, options_.height), CV_8UC3);
    std::vector<uint8_t> buf;
    bool ok = cv::imencode(options_.imgext, mat, buf);
    if (!ok) {
      LOG(FATAL) << "Failed to encode image. imgext=\"" << options_.imgext
                 << "\".";
    }

    uint32_t user_id = reinterpret_cast<uint64_t>(this) & 0xFFFFFFFFU;
    request_proto_.set_user_id(user_id);
    request_proto_.set_req_id(0);
    request_proto_.mutable_model_session()->CopyFrom(options_.model_session);
    auto* input = request_proto_.mutable_input();
    input->set_data_type(DT_IMAGE);
    auto* image = input->mutable_image();
    image->set_data(buf.data(), buf.size());
    if (options_.imgext == ".jpg") {
      image->set_format(ImageProto_ImageFormat_JPEG);
    } else {
      LOG(FATAL) << "Unknown image format. imgext=\"" << options_.imgext
                 << "\".";
    }
    image->set_color(true);
  }

  void Connect() {
    boost::asio::ip::tcp::resolver resolver(main_io_context_);
    boost::asio::ip::tcp::socket socket(main_io_context_);
    auto endpoints =
        resolver.resolve(options_.host, std::to_string(options_.port));
    boost::asio::connect(socket, endpoints);
    conn_ = std::make_shared<Connection>(std::move(socket), &message_handler_);
    conn_->Start();
  }

  void StartSend() {
    TimePoint st_time;
    TimePoint now = Clock::now();
    if (options_.sttime) {
      st_time = TimePoint(
          std::chrono::nanoseconds(static_cast<long>(options_.sttime * 1e9)));
      LOG_IF(FATAL, st_time <= now)
          << "Start too late. Give up. diff=" << (now - st_time).count() / 1e6
          << "ms";
    } else {
      st_time = now;
    }

    tp_idx_ = 0;
    next_time_ = st_time;
    tp_end_time_ = st_time;

    timer_.expires_at(st_time);
    timer_.async_wait([this](const boost::system::error_code& ec) {
      if (ec) return;
      LOG(INFO) << "[" << model_sess_id_ << "] Start sending...";
      PostNextRequest();
    });
  }

  void PostNextRequest() {
    if (next_time_ >= tp_end_time_) {
      if (tp_idx_ == options_.trace.size()) {
        LOG(INFO) << "[" << model_sess_id_ << "] Finished sending";
        auto cleanup_time =
            tp_end_time_ + std::chrono::milliseconds(
                               options_.model_session.latency_sla() + 500);
        timer_.expires_at(cleanup_time);
        timer_.async_wait([this](const boost::system::error_code& ec) {
          if (ec) return;
          FinishSend();
        });
        return;
      }
      const auto& tp = options_.trace[tp_idx_];
      LOG(INFO) << "[" << model_sess_id_ << "] Next TracePoint idx=" << tp_idx_
                << " rps=" << tp.rps << " duration=" << tp.duration;
      gapgen_ = options_.construct_gapgen(tp.rps);
      tp_end_time_ +=
          std::chrono::nanoseconds(static_cast<long>(tp.duration * 1e9));
      tp_idx_ += 1;
    }

    double gap = gapgen_->Next();
    next_time_ += std::chrono::nanoseconds(static_cast<long>(gap * 1e9));
    uint32_t reqid = request_proto_.req_id() + 1;
    request_proto_.set_req_id(reqid);
    msg_to_send_ =
        std::make_shared<Message>(kUserRequest, request_proto_.ByteSizeLong());
    msg_to_send_->EncodeBody(request_proto_);

    timer_.expires_at(next_time_);
    timer_.async_wait([this](const boost::system::error_code& ec) {
      if (ec) return;
      DoSend();
    });
  }

  void DoSend() {
    auto now = Clock::now();
    CHECK(now >= next_time_);
    conn_->Write(msg_to_send_);
    msg_to_send_.reset();

    uint32_t reqid = request_proto_.req_id();
    long expire_ns = next_time_.time_since_epoch().count();
    long send_ns = now.time_since_epoch().count();
    boost::asio::defer(output_io_context_, [this, reqid, expire_ns, send_ns] {
      printf("SEND %lu %u %ld %ld\n", sender_idx_, reqid, expire_ns, send_ns);
      fflush(stdout);
    });

    PostNextRequest();
  }

  void FinishSend() {
    conn_->Stop();
    conn_.reset();
    done_ = true;
  }

  void OutputReply(ReplyProto reply) {
    CHECK_GE(reply.query_latency_size(), 0) << "Missing query_latency field";
    const auto& clock = reply.query_latency(0).clock();
    printf(
        "REPLY %lu %u %d %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld "
        "%ld %ld\n",
        sender_idx_, reply.req_id(), reply.status(), clock.frontend_recv_ns(),
        clock.frontend_dispatch_ns(), clock.dispatcher_recv_ns(),
        clock.dispatcher_sched_ns(), clock.dispatcher_dispatch_ns(),
        clock.backend_recv_ns(), clock.backend_fetch_image_ns(),
        clock.backend_got_image_ns(), clock.backend_prep_dequeue_ns(),
        clock.backend_preprocessed_ns(), clock.backend_memcpy_ns(),
        clock.backend_exec_ns(), clock.backend_finish_ns(),
        clock.backend_reply_ns(), clock.frontend_got_reply_ns());
    fflush(stdout);

    const auto& bpstats = reply.query_latency(0).batchplan_stats();
    if (!bpstats.batch_size()) {
      return;
    }
    printf("BATCH %lu %lu %u %u %ld %ld %ld %ld %ld %ld %ld %ld %ld %d\n",
           sender_idx_, bpstats.plan_id(), bpstats.batch_size(),
           bpstats.backend_id(), bpstats.deadline_ns(),
           bpstats.expected_exec_ns(), bpstats.expected_finish_ns(),
           bpstats.dispatcher_dispatch_ns(), bpstats.backend_recv_ns(),
           bpstats.prepared_ns(), bpstats.actual_exec_ns(),
           bpstats.input_synced_ns(), bpstats.actual_finish_ns(),
           bpstats.status());
    fflush(stdout);
  }

  size_t sender_idx_;
  Options options_;
  std::string model_sess_id_;
  MessageHandler message_handler_;
  boost::asio::io_context& main_io_context_;
  boost::asio::system_timer timer_;
  std::shared_ptr<Connection> conn_;
  boost::asio::io_context& output_io_context_;

  bool done_;
  RequestProto request_proto_;
  std::unique_ptr<GapGenerator> gapgen_;
  size_t tp_idx_;
  TimePoint tp_end_time_;
  TimePoint next_time_;

  std::shared_ptr<Message> msg_to_send_;
};

Options Options::FromYaml(size_t cfgidx, const YAML::Node& config) {
  Options opt;
  opt.model_session.set_version(1);

  // Frontend IP
  opt.host = config["host"].as<std::string>("127.0.0.1");

  // Frontend port
  opt.port = config["port"].as<int>(9001);

  // Start time in unix epoch. 0 to start now.
  opt.sttime = config["sttime"].as<double>(0.0);

  // Model framework
  opt.model_session.set_framework(
      config["framework"].as<std::string>("tensorflow"));

  // Model name
  LOG_IF(FATAL, !config["model"]) << "Must specify `model`. cfgidx=" << cfgidx;
  opt.model_session.set_model_name(config["model"].as<std::string>());

  // Model latency SLO in milliseconds
  LOG_IF(FATAL, !config["slo"]) << "Must specify `slo`. cfgidx=" << cfgidx;
  opt.model_session.set_latency_sla(config["slo"].as<int>());

  // Input image width
  opt.width = config["width"].as<int>(224);

  // Input image height
  opt.height = config["height"].as<int>(224);

  // Image encoding
  opt.imgext = config["imgext"].as<std::string>(".jpg");

  // Random seed
  opt.seed = config["seed"].as<int64_t>(0xabcdabcd987LL);

  // Request interval gap. Choices: ['const', 'exp']
  opt.gap = config["gap"].as<std::string>("const");

  // Gap factory
  {
    std::unordered_map<std::string, decltype(opt.construct_gapgen)> gap_factory;
    gap_factory.emplace("const", [](double rps) {
      return std::make_unique<ConstGapGenerator>(rps);
    });
    gap_factory.emplace("exp", [seed = opt.seed](double rps) {
      return std::make_unique<ExpGapGenerator>(seed, rps);
    });
    auto iter = gap_factory.find(opt.gap);
    if (iter == gap_factory.end()) {
      LOG(FATAL) << "Unknown choice for `gap`: " << opt.gap;
    }
    opt.construct_gapgen = iter->second;
  }

  // Trace
  const auto& trace = config["trace"];
  LOG_IF(FATAL, !trace.IsSequence())
      << "Must specify `trace` as an array. cfgidx=" << cfgidx;
  for (const auto& node : trace) {
    LOG_IF(FATAL, !node.IsMap()) << "Entries of `trace` must be a map, "
                                    "specifying `rps` and `duration`. cfgidx="
                                 << cfgidx;
    auto rps = node["rps"].as<double>();
    auto duration = node["duration"].as<double>();
    opt.trace.push_back({rps, duration});
  }

  return opt;
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  FLAGS_colorlogtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, false);
  google::InstallFailureSignalHandler();

  // Parse
  auto config = YAML::Load(std::cin);
  LOG_IF(FATAL, !config.IsSequence())
      << "Root of the YAML config must be an array.";
  std::vector<Options> options_list;
  for (size_t i = 0; i < config.size(); ++i) {
    options_list.push_back(Options::FromYaml(i, config[i]));
  }

  // Setup IO tasks
  boost::asio::io_context main_io_context;
  boost::asio::io_context output_io_context;
  std::vector<std::unique_ptr<WorkloadSender>> senders;
  for (size_t i = 0; i < options_list.size(); ++i) {
    senders.emplace_back(std::make_unique<WorkloadSender>(
        &main_io_context, &output_io_context, i, options_list[i]));
  }

  // Spawn output thread
  auto output_work = boost::asio::make_work_guard(output_io_context);
  std::thread output_thread([&output_io_context] {
    pthread_setname_np(pthread_self(), "OutputThread");
    output_io_context.run();
  });

  // Event loop
  for (;;) {
    main_io_context.poll();

    size_t cnt_done = 0;
    for (auto& sender : senders) {
      cnt_done += static_cast<size_t>(sender->IsDone());
    }
    if (cnt_done == senders.size()) {
      break;
    }
  }

  // Cleanup
  senders.clear();
  main_io_context.run();
  output_work.reset();
  output_io_context.run();
  output_thread.join();
}
