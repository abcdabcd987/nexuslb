#include <algorithm>
#include <boost/asio.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#define CHECK(x)                                                      \
  do {                                                                \
    if (!(x)) {                                                       \
      fprintf(stderr, "%s:%d: !CHECK(%s)\n", __FILE__, __LINE__, #x); \
      abort();                                                        \
    }                                                                 \
  } while (0)

using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::time_point<Clock, std::chrono::nanoseconds>;

void PrintErrorImpl(boost::asio::ip::tcp::socket& socket,
                    boost::system::error_code ec, int lineno) {
  if (ec) {
    if (socket.is_open()) {
      fprintf(stderr, "L%d: Error peer=%s:%u code=%d message: %s\n", lineno,
              socket.remote_endpoint().address().to_string().c_str(),
              socket.remote_endpoint().port(), ec.value(),
              ec.message().c_str());
    } else {
      fprintf(stderr, "L%d: Error code=%d message: %s\n", lineno, ec.value(),
              ec.message().c_str());
    }
  }
}

#define CheckSocketError(socket, ec) PrintErrorImpl(socket, ec, __LINE__)

enum class MessageType : uint32_t {
  kFetchRequest = 1,
  kFetchResponse = 2,
};

struct MessageHeader {
  MessageType type;
  uint32_t body_length;
};

class Message {
 public:
  explicit Message(MessageHeader header) {
    data_ = std::malloc(sizeof(header) + header.body_length);
    std::memcpy(data_, &header, sizeof(header));
  }
  Message(MessageType type, uint32_t body_length) {
    data_ = std::malloc(sizeof(MessageHeader) + body_length);
    auto header = static_cast<MessageHeader*>(data_);
    header->type = type;
    header->body_length = body_length;
  }
  ~Message() { std::free(data_); }
  Message(const Message&) = delete;
  Message(Message&&) = delete;

  void* data() { return data_; }
  size_t size() const { return sizeof(MessageHeader) + header().body_length; }
  void* body() { return static_cast<char*>(data_) + sizeof(MessageHeader); }
  const MessageHeader& header() const {
    return *static_cast<MessageHeader*>(data_);
  }

  template <class T>
  T* as() {
    return static_cast<T*>(body());
  }

 private:
  void* data_;
};

struct FetchRequest {
  size_t read_length;
};

struct FetchResponse {
  size_t data_length;
  char data[1];

  static size_t GetSize(size_t data_length) {
    return offsetof(FetchResponse, data) + data_length;
  }
  size_t size() const { return GetSize(data_length); }
};

class Connection;

class MessageHandler {
 public:
  virtual void HandleMessage(std::shared_ptr<Connection> conn,
                             std::shared_ptr<Message> msg) = 0;
};

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  Connection(boost::asio::io_context& io_context, MessageHandler* handler)
      : socket_(io_context), handler_(handler) {}
  Connection(boost::asio::ip::tcp::socket socket, MessageHandler* handler)
      : socket_(std::move(socket)), handler_(handler) {}
  ~Connection() { Stop(); }
  boost::asio::ip::tcp::socket& socket() { return socket_; }

  void Start() {
    socket_.set_option(boost::asio::ip::tcp::no_delay(true));
    DoReadHeader();
  }

  void Stop() { socket_.close(); }

  void Write(std::shared_ptr<Message> msg) {
    write_queue_.push_back(std::move(msg));
    if (write_queue_.size() == 1) {
      DoWrite();
    }
  }

 private:
  void DoReadHeader() {
    auto self = shared_from_this();
    boost::asio::async_read(socket_,
                            boost::asio::buffer(&header_, sizeof(header_)),
                            [this, self](boost::system::error_code ec, size_t) {
                              CheckSocketError(socket_, ec);
                              if (!ec) {
                                DoReadBody(std::make_shared<Message>(header_));
                              }
                            });
  }

  void DoReadBody(std::shared_ptr<Message> msg) {
    auto self = shared_from_this();
    boost::asio::async_read(
        socket_, boost::asio::buffer(msg->body(), msg->header().body_length),
        [this, self, msg](boost::system::error_code ec, size_t) {
          CheckSocketError(socket_, ec);
          if (!ec) {
            handler_->HandleMessage(self, msg);
            DoReadHeader();
          }
        });
  }

  void DoWrite() {
    auto self = shared_from_this();
    auto msg = write_queue_.front();
    boost::asio::async_write(
        socket_, boost::asio::buffer(msg->data(), msg->size()),
        [this, self](boost::system::error_code ec, size_t) {
          CheckSocketError(socket_, ec);
          if (!ec) {
            write_queue_.pop_front();
            if (!write_queue_.empty()) {
              DoWrite();
            }
          }
        });
  }

  boost::asio::ip::tcp::socket socket_;
  MessageHandler* handler_;
  MessageHeader header_;
  std::deque<std::shared_ptr<Message>> write_queue_;
};

class TcpServer {
 public:
  TcpServer(boost::asio::io_context& io_context, uint16_t port)
      : io_context_(io_context),
        acceptor_(io_context_, boost::asio::ip::tcp::endpoint(
                                   boost::asio::ip::tcp::v4(), port)) {}

  void Start() { DoAccept(); }

 private:
  class RequestHandler : public MessageHandler {
   public:
    void HandleMessage(std::shared_ptr<Connection> conn,
                       std::shared_ptr<Message> msg) override {
      CHECK(msg->header().type == MessageType::kFetchRequest);
      auto* req = msg->as<FetchRequest>();
      auto resp_msg =
          std::make_shared<Message>(MessageType::kFetchResponse,
                                    FetchResponse::GetSize(req->read_length));
      auto* resp = resp_msg->as<FetchResponse>();
      resp->data_length = req->read_length;
      conn->Write(std::move(resp_msg));
    }
  };

  void DoAccept() {
    acceptor_.async_accept([this](const boost::system::error_code& ec,
                                  boost::asio::ip::tcp::socket socket) {
      CheckSocketError(socket, ec);
      if (ec) return;

      std::make_shared<Connection>(std::move(socket), &handler_)->Start();
      DoAccept();
    });
  }

  boost::asio::io_context& io_context_;
  boost::asio::ip::tcp::acceptor acceptor_;
  RequestHandler handler_;
};

class TcpClient {
 public:
  TcpClient(boost::asio::io_context& io_context)
      : io_context_(io_context), handler_(this) {
    conn_ = std::make_shared<Connection>(io_context_, &handler_);
  }

  void Stop() { conn_->Stop(); }

  void AsyncConnect(std::string host, uint16_t port,
                    std::function<void(boost::system::error_code)> callback) {
    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::address::from_string(host), port);
    conn_->socket().async_connect(endpoint, [this, cb = std::move(callback)](
                                                boost::system::error_code ec) {
      conn_->Start();
      cb(ec);
    });
  }

  void AsyncFetch(size_t length,
                  std::function<void(const FetchResponse&)> callback) {
    callbacks_.push_back(std::move(callback));
    auto msg = std::make_shared<Message>(MessageType::kFetchRequest,
                                         sizeof(FetchRequest));
    auto* req = msg->as<FetchRequest>();
    req->read_length = length;
    conn_->Write(msg);
  }

 private:
  class ResponseHandler : public MessageHandler {
   public:
    ResponseHandler(TcpClient* outer) : outer_(outer) {}
    void HandleMessage(std::shared_ptr<Connection> conn,
                       std::shared_ptr<Message> msg) override {
      CHECK(msg->header().type == MessageType::kFetchResponse);
      CHECK(!outer_->callbacks_.empty());
      auto* resp = msg->as<FetchResponse>();
      auto callback = std::move(outer_->callbacks_.front());
      outer_->callbacks_.pop_front();
      callback(*resp);
    }

   private:
    TcpClient* outer_;
  };

  boost::asio::io_context& io_context_;
  ResponseHandler handler_;
  std::shared_ptr<Connection> conn_;
  std::deque<std::function<void(const FetchResponse&)>> callbacks_;
};

class IncastBench {
 public:
  struct Options {
    std::vector<std::pair<std::string, uint16_t>> servers;
    size_t read_size;
    size_t concurrent_reads;
    size_t warmups;
    size_t bench;
    size_t cooldown;
  };
  IncastBench(Options options, boost::asio::io_context& io_context,
              bool& everyting_done)
      : options_(std::move(options)),
        io_context_(io_context),
        everyting_done_(everyting_done),
        test_elapse_ns_(options_.warmups + options_.bench + options_.cooldown) {
    for (size_t i = 0; i < options_.servers.size(); ++i) {
      clients_.push_back(std::make_unique<TcpClient>(io_context_));
    }
  }

  void Start() {
    for (size_t i = 0; i < options_.servers.size(); ++i) {
      const auto& server = options_.servers[i];
      clients_[i]->AsyncConnect(
          server.first, server.second,
          [this, server_idx = i](boost::system::error_code ec) {
            if (ec) {
              const auto& server = options_.servers[server_idx];
              fprintf(stderr, "Failed to connect to server %s:%u. Reason: %s\n",
                      server.first.c_str(), server.second,
                      ec.message().c_str());
              std::exit(1);
            }

            if (++cnt_connected_ == options_.servers.size()) {
              boost::asio::post(io_context_, [this] { StartBench(); });
            }
          });
    }
  }

 private:
  void StartBench() {
    start_time_ = Clock::now();
    cnt_done_test_ = 0;
    target_complete_ = options_.concurrent_reads * options_.servers.size();
    BenchMore();
  }

  void BenchMore() {
    if (cnt_done_test_ ==
        options_.warmups + options_.bench + options_.cooldown) {
      fprintf(stderr, "Benchmark done.\n");
      BenchmarkDone();
      return;
    } else if (cnt_done_test_ == options_.warmups + options_.bench) {
      fprintf(stderr, "Start to cooldown.\n");
      bench_finish_time_ = Clock::now();
    } else if (cnt_done_test_ == options_.warmups) {
      fprintf(stderr, "Start to benchmark.\n");
      bench_start_time_ = Clock::now();
    } else if (cnt_done_test_ == 0) {
      fprintf(stderr, "Start to warmup.\n");
    }

    cnt_complete_ = 0;
    test_start_time_ = Clock::now();
    for (auto& client : clients_) {
      for (size_t i = 0; i < options_.concurrent_reads; ++i) {
        client->AsyncFetch(
            options_.read_size, [this](const FetchResponse& resp) {
              if (++cnt_complete_ == target_complete_) {
                boost::asio::post(
                    io_context_, [this, now = Clock::now()] { TestDone(now); });
              }
            });
      }
    }
  }

  void TestDone(TimePoint test_done_time) {
    auto elapse_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         test_done_time - test_start_time_)
                         .count();
    test_elapse_ns_[cnt_done_test_] = elapse_ns;
    ++cnt_done_test_;
    ReportProgress(false);
    BenchMore();
  }

  void BenchmarkDone() {
    ReportProgress(true);
    for (auto& client : clients_) {
      client->Stop();
    }

    auto elapse_ns = (bench_finish_time_ - bench_start_time_).count();
    double elapse = elapse_ns / 1e9;
    auto bench_packets =
        options_.servers.size() * options_.bench * options_.concurrent_reads;
    auto avg_pps = bench_packets / elapse;
    auto avg_gbps = bench_packets * options_.read_size * 1.0 / elapse_ns * 8;
    printf("num_servers: %lu\n", options_.servers.size());
    printf("concurrent_reads per server: %lu\n", options_.concurrent_reads);
    printf("num_packets per server: %lu\n", options_.bench);
    printf("read_size: %lu\n", options_.read_size);
    printf("mode: TCP INCAST READ\n");
    printf("avg bandwidth: %.3f Gbps\n", avg_gbps);
    printf("avg rate: %.3f kpps\n", avg_pps / 1000);

    std::sort(test_elapse_ns_.begin(), test_elapse_ns_.end());
    auto pp = [this](double p) {
      size_t idx = std::floor(test_elapse_ns_.size() * p / 100.);
      double latency_us = test_elapse_ns_[idx] / 1e3;
      double throughput_gbps = target_complete_ * options_.read_size * 1.0 /
                               test_elapse_ns_[idx] * 8;
      printf("p%-5.2f: %4.0f us (%6.3f Gbps)\n", p, latency_us,
             throughput_gbps);
    };
    pp(50);
    pp(75);
    pp(90);
    pp(95);
    pp(99);
    pp(99.5);
    pp(99.9);
    pp(99.95);
    pp(99.99);

    everyting_done_ = true;
  }

  void ReportProgress(bool force) {
    auto now = Clock::now();
    auto last_second = std::chrono::duration_cast<std::chrono::seconds>(
                           last_report_time_ - start_time_)
                           .count();
    auto now_second =
        std::chrono::duration_cast<std::chrono::seconds>(now - start_time_)
            .count();
    if (now_second == last_second && !force) {
      return;
    }
    auto nanos = (now - start_time_).count();
    auto seconds = nanos / 1e9;
    auto total = options_.warmups + options_.bench + options_.cooldown;
    auto pct = cnt_done_test_ * 100 / total;
    auto rps = cnt_done_test_ * target_complete_ / seconds;
    fprintf(stderr, "[%3lu%%] Sent %lu/%lu requests in %.6fs. (avg %.3f rps)\n",
            pct, cnt_done_test_, total, seconds, rps);
    last_report_time_ = now;
  }

  Options options_;
  boost::asio::io_context& io_context_;
  bool& everyting_done_;
  std::vector<std::unique_ptr<TcpClient>> clients_;
  size_t cnt_connected_ = 0;

  std::vector<long> test_elapse_ns_;
  TimePoint start_time_;
  TimePoint last_report_time_;

  size_t cnt_done_test_;
  size_t target_complete_;
  size_t cnt_complete_;
  TimePoint test_start_time_;
  TimePoint bench_start_time_;
  TimePoint bench_finish_time_;
};

void DieUsage(const char* program) {
  printf("usage:\n");
  printf("  %s server block|spin <listen_port> \n", program);
  printf(
      "  %s benchincast block|spin "
      "<read_size> <concurrent_reads> <warmups> <tests> <cooldowns> "
      "<ip:port>...\n",
      program);
  std::exit(1);
}

void Run(int argc, char** argv, boost::asio::io_context& io_context,
         bool& done) {
  if (std::string("block") == argv[2]) {
    io_context.run();
  } else if (std::string("spin") == argv[2]) {
    while (!done) {
      io_context.poll();
    }
  } else {
    DieUsage(argv[0]);
  }
}

void die(std::string reason) {
  fprintf(stderr, "die: %s\n", reason.c_str());
  fflush(stderr);
  abort();
}

void ServerMain(int argc, char** argv) {
  if (argc != 4) DieUsage(argv[0]);
  uint16_t port = std::stoul(argv[3]);

  boost::asio::io_context io_context;
  bool done = false;
  TcpServer server(io_context, port);
  server.Start();
  Run(argc, argv, io_context, done);
}

void IncastMain(int argc, char** argv) {
  if (argc < 9) DieUsage(argv[0]);
  IncastBench::Options options;
  options.read_size = std::stoul(argv[3]);
  options.concurrent_reads = std::stoul(argv[4]);
  options.warmups = std::stoul(argv[5]);
  options.bench = std::stoul(argv[6]);
  options.cooldown = std::stoul(argv[7]);
  for (int i = 8; i < argc; ++i) {
    std::string s(argv[i]);
    auto pos = s.find(':');
    if (pos == std::string::npos) die("Failed to parse server address: " + s);
    auto ip = s.substr(0, pos);
    uint16_t port = std::stoul(s.substr(pos + 1));
    options.servers.push_back({ip, port});
  }

  boost::asio::io_context io_context;
  bool done = false;
  IncastBench bench(options, io_context, done);
  bench.Start();
  Run(argc, argv, io_context, done);
}

int main(int argc, char** argv) {
  if (argc < 3) DieUsage(argv[0]);
  if (std::string("server") == argv[1]) {
    ServerMain(argc, argv);
  } else if (std::string("benchincast") == argv[1]) {
    IncastMain(argc, argv);
  } else {
    DieUsage(argv[0]);
  }
}
