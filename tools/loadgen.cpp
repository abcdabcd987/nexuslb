#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

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
#include <memory>
#include <opencv2/opencv.hpp>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "nexus/common/connection.h"
#include "nexus/common/time_util.h"
#include "nexus/common/util.h"
#include "nexus/proto/nnquery.pb.h"

using namespace nexus;

class GapGenerator {
 public:
  GapGenerator(double rps) : rps_(rps) {}
  double rps() const { return rps_; }
  virtual std::string Name() const = 0;
  virtual double Next() = 0;

 private:
  double rps_;
};

class ConstGapGenerator : public GapGenerator {
 public:
  ConstGapGenerator(double rps) : GapGenerator(rps) {}

  double Next() override { return 1 / rps(); }

  std::string Name() const override { return "const"; }
};

class ExpGapGenerator : public GapGenerator {
 public:
  ExpGapGenerator(int64_t seed, double rps)
      : GapGenerator(rps), rand_engine_(seed) {}

  double Next() override {
    auto rand = uniform_(rand_engine_);
    return -std::log(1.0 - rand) / rps();
  }

  std::string Name() const override { return "exp"; }

 private:
  std::mt19937 rand_engine_;
  std::uniform_real_distribution<double> uniform_;
};

struct WorkloadPoint {
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
  std::vector<WorkloadPoint> workload;
  std::function<std::unique_ptr<GapGenerator>(double rps)> construct_gapgen;

  static Options FromGflags();
};

class WorkloadSender {
 public:
  explicit WorkloadSender(Options options)
      : options_(std::move(options)),
        message_handler_(this),
        timer_(io_context_),
        output_fd_(io_context_, STDOUT_FILENO) {
    output_fd_.non_blocking(true);
    PrepareRequestTemplate();
  }

  ~WorkloadSender() { Stop(); }

  void Run() {
    done_ = false;
    Connect();
    StartSend();
    while (!done_) {
      io_context_.poll();
    }

    // Cleanup
    io_context_.run();
  }

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
      ReplyProto proto;
      bool ok = proto.ParseFromArray(message->body(), message->body_length());
      if (!ok) {
        LOG(FATAL) << "Failed to parse ReplyProto. message->body_length()="
                   << message->body_length();
      }
      outer_.HandleReply(std::move(proto));
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
    boost::asio::ip::tcp::resolver resolver(io_context_);
    boost::asio::ip::tcp::socket socket(io_context_);
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
      LOG_IF(FATAL, st_time > now) << "Start too late. Give up.";
    } else {
      st_time = now;
    }

    wp_idx_ = 0;
    next_time_ = st_time;
    wp_end_time_ = st_time;

    timer_.expires_at(st_time);
    timer_.async_wait([this](const boost::system::error_code& ec) {
      if (ec) return;
      LOG(INFO) << "Start sending...";
      PostNextRequest();
    });
  }

  void PostNextRequest() {
    if (next_time_ >= wp_end_time_) {
      if (wp_idx_ == options_.workload.size()) {
        LOG(INFO) << "Finished sending";
        auto cleanup_time =
            wp_end_time_ + std::chrono::milliseconds(
                               options_.model_session.latency_sla() + 500);
        timer_.expires_at(cleanup_time);
        timer_.async_wait([this](const boost::system::error_code& ec) {
          if (ec) return;
          FinishSend();
        });
        return;
      }
      const auto& wp = options_.workload[wp_idx_];
      LOG(INFO) << "Next WorkloadPoint idx=" << wp_idx_ << " rps=" << wp.rps
                << " duration=" << wp.duration;
      gapgen_ = options_.construct_gapgen(wp.rps);
      next_time_ = wp_end_time_;
      wp_end_time_ +=
          std::chrono::nanoseconds(static_cast<long>(wp.duration * 1e9));
      wp_idx_ += 1;
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

    std::vector<char> buf;
    buf.resize(5 + 10 + 20 * 2);
    uint32_t reqid = request_proto_.req_id();
    long expire_ns = next_time_.time_since_epoch().count();
    long now_ns = now.time_since_epoch().count();
    int n = snprintf(buf.data(), buf.size(), "SEND %u %ld %ld\n", reqid,
                     expire_ns, now_ns);
    CHECK_LE(n, buf.size()) << "Buffer size too small";
    buf.resize(n);
    WriteOutput(std::move(buf));

    PostNextRequest();
  }

  void FinishSend() {
    conn_->Stop();
    conn_.reset();
    done_ = true;
  }

  void HandleReply(ReplyProto reply) {
    std::vector<char> buf;
    buf.resize(6 + 10 + 12 * 20 + 1);
    CHECK_GE(reply.query_latency_size(), 0) << "Missing query_latency field";
    const auto& clock = reply.query_latency(0).clock();
    int n = snprintf(
        buf.data(), buf.size(),
        "REPLY %u %d %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
        reply.req_id(), reply.status(), clock.frontend_recv_ns(),
        clock.frontend_dispatch_ns(), clock.dispatcher_recv_ns(),
        clock.dispatcher_sched_ns(), clock.dispatcher_dispatch_ns(),
        clock.backend_recv_ns(), clock.backend_fetch_image_ns(),
        clock.backend_got_image_ns(), clock.backend_exec_ns(),
        clock.backend_finish_ns(), clock.backend_reply_ns(),
        clock.frontend_got_reply_ns());
    CHECK_LE(n, buf.size()) << "Buffer size too small";
    buf.resize(n);
    WriteOutput(std::move(buf));
  }

  void WriteOutput(std::vector<char> buf) {
    output_queue_.emplace_back(std::move(buf));
    if (output_queue_.size() == 1) {
      AsyncWriteOutput();
    }
  }

  void AsyncWriteOutput() {
    boost::asio::async_write(
        output_fd_, boost::asio::buffer(output_queue_.front()),
        [this](const boost::system::error_code& ec,
               std::size_t bytes_transferred) {
          if (ec) {
            LOG(ERROR) << "Failed to write to output: " << ec
                       << " Bytes transferred: " << bytes_transferred;
          }
          output_queue_.pop_front();
          if (!output_queue_.empty()) {
            AsyncWriteOutput();
          }
        });
  }

  Options options_;
  MessageHandler message_handler_;
  boost::asio::io_context io_context_;
  boost::asio::system_timer timer_;
  std::shared_ptr<Connection> conn_;

  std::deque<std::vector<char>> output_queue_;
  boost::asio::posix::stream_descriptor output_fd_;

  bool done_;
  RequestProto request_proto_;
  std::unique_ptr<GapGenerator> gapgen_;
  size_t wp_idx_;
  TimePoint wp_end_time_;
  TimePoint next_time_;

  std::shared_ptr<Message> msg_to_send_;
};

DEFINE_string(host, "127.0.0.1", "Frontend IP");
DEFINE_int32(port, 9001, "Frontend port");
DEFINE_double(sttime, 0, "Start time in unix epoch. 0 to start now.");
DEFINE_string(framework, "tensorflow", "Model framework");
DEFINE_string(model, "", "Model name");
DEFINE_int32(slo, 0, "Model latency SLO in milliseconds");
DEFINE_int32(width, 224, "Input image width");
DEFINE_int32(height, 224, "Input image height");
DEFINE_string(imgext, ".jpg", "Image encoding");
DEFINE_int64(seed, 0xabcdabcd987LL, "Random seed");
DEFINE_string(gap, "const", "Request interval gap. Choices: ['const', 'exp']");
DEFINE_string(workload, "", "Format: '<RPS>x<DURATION>,<RPS>x<DURATION>,...'");

Options Options::FromGflags() {
  if (FLAGS_model.empty()) {
    fprintf(stderr, "Must specify `-model`\n");
    std::exit(1);
  }
  if (!FLAGS_slo) {
    fprintf(stderr, "Must specify `-slo`\n");
    std::exit(1);
  }
  if (FLAGS_workload.empty()) {
    fprintf(stderr, "Must specify `-workload`\n");
    std::exit(1);
  }

  Options opt;
  opt.host = FLAGS_host;
  opt.port = FLAGS_port;
  opt.sttime = FLAGS_sttime;
  opt.model_session.set_framework(FLAGS_framework);
  opt.model_session.set_model_name(FLAGS_model);
  opt.model_session.set_version(1);
  opt.model_session.set_latency_sla(FLAGS_slo);
  opt.width = FLAGS_width;
  opt.height = FLAGS_height;
  opt.imgext = FLAGS_imgext;
  opt.seed = FLAGS_seed;
  opt.gap = FLAGS_gap;

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
      fprintf(stderr, "Unknown choice for `-gap`: %s\n", opt.gap.c_str());
      std::exit(1);
    }
    opt.construct_gapgen = iter->second;
  }

  std::vector<std::string> wp_strings, tokens;
  SplitString(FLAGS_workload, ',', &wp_strings);
  for (const auto& wpstr : wp_strings) {
    SplitString(wpstr, 'x', &tokens);
    if (tokens.size() != 2) {
      fprintf(stderr, "Expecting 2 tokens but got %zu from string `%s`\n",
              tokens.size(), wpstr.c_str());
      std::exit(1);
    }
    double rps, duration;
    try {
      rps = std::stod(tokens[0]);
      duration = std::stod(tokens[1]);
    } catch (const std::exception& e) {
      fprintf(stderr, "Failed to parse two floating points from string `%s`\n",
              wpstr.c_str());
      std::exit(1);
    }
    opt.workload.push_back({rps, duration});
  }

  return opt;
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  FLAGS_colorlogtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, false);
  auto options = Options::FromGflags();
  google::InstallFailureSignalHandler();

  WorkloadSender(options).Run();
}
