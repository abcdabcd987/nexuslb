#pragma once
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "ario/epoll.h"
#include "ario/memory.h"

namespace ario {

class TcpSocket {
 public:
  TcpSocket();
  TcpSocket(TcpSocket &&other);
  TcpSocket &operator=(TcpSocket &&other);
  ~TcpSocket();
  bool IsValid() const;
  void Connect(EpollExecutor &executor, const std::string &host, uint16_t port);
  void AsyncRead(MutableBuffer buffer,
                 std::function<void(int err, size_t len)> &&handler);
  void AsyncWrite(ConstBuffer buffer,
                  std::function<void(int err, size_t len)> &&handler);

  const std::string &peer_ip() const;
  uint16_t peer_port() const;

 private:
  friend class TcpAcceptor;
  class Impl;
  TcpSocket(std::unique_ptr<Impl> impl);
  void AddEpollWatch();

  std::unique_ptr<Impl> impl_;
};

class TcpAcceptor {
 public:
  explicit TcpAcceptor(EpollExecutor &executor);
  ~TcpAcceptor();
  TcpAcceptor(const TcpAcceptor &other) = delete;
  TcpAcceptor &operator=(const TcpAcceptor &other) = delete;
  TcpAcceptor(TcpAcceptor &&other) = delete;
  TcpAcceptor &operator=(TcpAcceptor &&other) = delete;

  void BindAndListen(uint16_t port);
  void AsyncAccept(std::function<void(int err, TcpSocket peer)> &&handler);
  void RunEventLoop();
  void StopEventLoop();

 private:
  class EpollHandler : public EpollEventHandler {
   public:
    explicit EpollHandler(TcpAcceptor &super);
    void HandleEpollEvent(uint32_t epoll_events) override;

   private:
    TcpAcceptor &super_;
  };

  void DoAccept();

  EpollExecutor &executor_;
  int listen_fd_ = -1;
  EpollHandler epoll_handler_;

  std::mutex mutex_;
  std::function<void(int err, TcpSocket peer)> on_accept_handler_;
};

}  // namespace ario
