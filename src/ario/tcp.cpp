#include "ario/tcp.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cstring>

#include "ario/epoll.h"
#include "ario/utils.h"

namespace ario {

enum class TcpAsyncStreamState {
  kDrained,
  kAvailable,
};

template <typename T>
struct TcpAsyncOpContext {
  Span<T> buffer;
  std::function<void(int err, size_t len)> handler;
  size_t bytes_completed = 0;

  TcpAsyncOpContext(Span<T> buffer,
                    std::function<void(int err, size_t len)> &&handler)
      : buffer(buffer), handler(handler) {}
};

class TcpSocket::Impl {
 public:
  ~Impl();
  Impl(const Impl &other) = delete;
  Impl &operator=(const Impl &other) = delete;
  Impl(Impl &&other) = delete;
  Impl &operator=(Impl &&other) = delete;

 private:
  friend class TcpSocket;
  friend class TcpAcceptor;
  class EpollHandler : public EpollEventHandler {
   public:
    explicit EpollHandler(Impl &super);
    void HandleEpollEvent(uint32_t epoll_events) override;

   private:
    Impl &super_;
  };

  Impl(EpollExecutor &executor, int fd, std::string peer_ip,
       uint16_t peer_port);
  bool DoRead();
  bool DoWrite();
  void OnReadAvailable();
  void OnWriteAvailable();
  void Shutdown();

  EpollExecutor &executor_;
  EpollHandler epoll_handler_;
  std::string peer_ip_;
  uint16_t peer_port_;

  std::mutex mutex_;
  int fd_;
  TcpAsyncStreamState read_state_ = TcpAsyncStreamState::kDrained;
  TcpAsyncStreamState write_state_ = TcpAsyncStreamState::kAvailable;
  std::optional<TcpAsyncOpContext<void>> read_context_;
  std::optional<TcpAsyncOpContext<const void>> write_context_;
};

TcpSocket::Impl::Impl(EpollExecutor &executor, int fd, std::string peer_ip,
                      uint16_t peer_port)
    : executor_(executor),
      epoll_handler_(*this),
      peer_ip_(std::move(peer_ip)),
      peer_port_(peer_port),
      fd_(fd) {}

TcpSocket::Impl::~Impl() { Shutdown(); }

bool TcpSocket::Impl::DoRead() {
  auto &ctx = *read_context_;
  auto *buf = static_cast<uint8_t *>(ctx.buffer.data()) + ctx.bytes_completed;
  size_t nleft = ctx.buffer.size() - ctx.bytes_completed;
  while (nleft > 0) {
    auto nbytes = read(fd_, buf, nleft);
    if (nbytes < 0) {
      read_state_ = TcpAsyncStreamState::kDrained;
      if (errno != EAGAIN) {
        perror("read");
        return true;
      }
      return false;
    } else if (nbytes == 0) {
      Shutdown();
      return true;
    }
    nleft -= nbytes;
    buf += nbytes;
    ctx.bytes_completed += nbytes;
  }
  errno = 0;
  return true;
}

bool TcpSocket::Impl::DoWrite() {
  auto &ctx = *write_context_;
  auto *buf =
      static_cast<const uint8_t *>(ctx.buffer.data()) + ctx.bytes_completed;
  size_t nleft = ctx.buffer.size() - ctx.bytes_completed;
  while (nleft > 0) {
    auto nbytes = write(fd_, buf, nleft);
    if (nbytes < 0) {
      write_state_ = TcpAsyncStreamState::kDrained;
      if (errno != EAGAIN) {
        perror("write");
        return true;
      }
      return false;
    } else if (nbytes == 0) {
      Shutdown();
      return true;
    }
    nleft -= nbytes;
    buf += nbytes;
    ctx.bytes_completed += nbytes;
  }
  errno = 0;
  return true;
}

void TcpSocket::Impl::OnReadAvailable() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (fd_ == -1) {
    return;
  }
  read_state_ = TcpAsyncStreamState::kAvailable;
  if (!read_context_.has_value()) {
    return;
  }
  bool done = DoRead();
  if (done) {
    auto ctx = std::move(*read_context_);
    read_context_.reset();
    lock.unlock();
    ctx.handler(errno, ctx.bytes_completed);
  }
}

void TcpSocket::Impl::OnWriteAvailable() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (fd_ == -1) {
    return;
  }
  write_state_ = TcpAsyncStreamState::kAvailable;
  if (!write_context_.has_value()) {
    return;
  }
  bool done = DoWrite();
  if (done) {
    auto ctx = std::move(*write_context_);
    write_context_.reset();
    lock.unlock();
    ctx.handler(errno, ctx.bytes_completed);
  }
}

void TcpSocket::Impl::Shutdown() {
  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
}

TcpSocket::Impl::EpollHandler::EpollHandler(TcpSocket::Impl &super)
    : super_(super) {}

void TcpSocket::Impl::EpollHandler::HandleEpollEvent(uint32_t epoll_events) {
  if (epoll_events & EPOLLIN) {
    super_.OnReadAvailable();
  }
  if (epoll_events & EPOLLOUT) {
    super_.OnWriteAvailable();
  }
  if (epoll_events & ~(EPOLLIN | EPOLLOUT)) {
    fprintf(stderr,
            "TcpSocket::Impl::EpollHandler: Unhandled epoll event: 0x%08x\n",
            epoll_events);
  }
}

TcpSocket::TcpSocket() : impl_() {}

TcpSocket::TcpSocket(TcpSocket &&other) = default;

TcpSocket &TcpSocket::operator=(TcpSocket &&other) = default;

TcpSocket::~TcpSocket() = default;

TcpSocket::TcpSocket(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

const std::string &TcpSocket::peer_ip() const { return impl_->peer_ip_; }

uint16_t TcpSocket::peer_port() const { return impl_->peer_port_; }

bool TcpSocket::IsValid() const { return impl_.get() != nullptr; }

void TcpSocket::AsyncRead(MutableBuffer buffer,
                          std::function<void(int err, size_t len)> &&handler) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  if (impl_->read_context_.has_value())
    die("Another AsyncRead is in progress.");
  impl_->read_context_.emplace(buffer, std::move(handler));
  if (impl_->read_state_ == TcpAsyncStreamState::kAvailable) {
    impl_->executor_.Post(
        [impl = impl_.get()](ErrorCode) { impl->OnReadAvailable(); },
        ErrorCode::kOk);
  }
}

void TcpSocket::AsyncWrite(ConstBuffer buffer,
                           std::function<void(int err, size_t len)> &&handler) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  if (impl_->write_context_.has_value())
    die("Another AsyncWrite is in progress.");
  impl_->write_context_.emplace(buffer, std::move(handler));
  if (impl_->write_state_ == TcpAsyncStreamState::kAvailable) {
    impl_->executor_.Post(
        [impl = impl_.get()](ErrorCode) { impl->OnWriteAvailable(); },
        ErrorCode::kOk);
  }
}

void TcpSocket::Connect(EpollExecutor &executor, const std::string &host,
                        uint16_t port) {
  if (impl_) die("Already connected.");

  addrinfo hints;
  addrinfo *result;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = 0;
  hints.ai_protocol = 0;
  auto str_port = std::to_string(port);
  int ret = getaddrinfo(host.c_str(), str_port.c_str(), &hints, &result);
  if (ret < 0) die_perror("getaddrinfo");

  int fd = -1;
  for (auto *rp = result; rp != nullptr; rp = rp->ai_next) {
    fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd == -1) continue;
    if (connect(fd, rp->ai_addr, rp->ai_addrlen) != -1) break;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(result);
  if (fd == -1) {
    fprintf(stderr, "Could not connect to %s:%d\n", host.c_str(), port);
    abort();
  }

  SetNonBlocking(fd);
  impl_.reset(new TcpSocket::Impl(executor, fd, host, port));
  AddEpollWatch();
}

void TcpSocket::AddEpollWatch() {
  EpollExecutorAddEpollWatch(impl_->executor_, impl_->fd_,
                             impl_->epoll_handler_);
}

TcpAcceptor::EpollHandler::EpollHandler(TcpAcceptor &super) : super_(super) {}

void TcpAcceptor::EpollHandler::HandleEpollEvent(uint32_t epoll_events) {
  super_.DoAccept();
}

TcpAcceptor::TcpAcceptor(EpollExecutor &executor)
    : executor_(executor), epoll_handler_(*this) {}

TcpAcceptor::~TcpAcceptor() {
  if (listen_fd_ != -1) {
    close(listen_fd_);
  }
}

void TcpAcceptor::BindAndListen(uint16_t port) {
  if (listen_fd_ != -1) die("TcpAcceptor::BindAndListen: already listening");
  listen_fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listen_fd_ < 0) die_perror("socket");

  int flag = 1;
  int ret =
      setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
  if (ret < 0) die_perror("setsockopt SO_REUSEADDR");

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port);
  ret = bind(listen_fd_, (struct sockaddr *)&server_addr, sizeof(server_addr));
  if (ret < 0) die_perror("bind");

  constexpr int kBacklog = 10;
  ret = listen(listen_fd_, kBacklog);
  if (ret < 0) die_perror("listen");

  SetNonBlocking(listen_fd_);

  EpollExecutorAddEpollWatch(executor_, listen_fd_, epoll_handler_);
}

void TcpAcceptor::AsyncAccept(
    std::function<void(int err, TcpSocket peer)> &&handler) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (on_accept_handler_) die("AsyncAccept already in progress.");
    on_accept_handler_ = std::move(handler);
  }
  DoAccept();
}

void TcpAcceptor::DoAccept() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!on_accept_handler_) {
    return;
  }
  struct sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(client_addr);
  int client_fd =
      accept(listen_fd_, (struct sockaddr *)&client_addr, &client_addr_len);
  if (client_fd < 0) {
    if (errno != EAGAIN) {
      perror("accept");
      auto handler = std::move(on_accept_handler_);
      lock.unlock();
      handler(errno, {});
    }
    return;
  }
  auto handler = std::move(on_accept_handler_);
  lock.unlock();
  char str_addr[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &client_addr.sin_addr, str_addr, sizeof(str_addr));
  fprintf(stderr, "New TCP connection from: %s:%d\n", str_addr,
          ntohs(client_addr.sin_port));

  SetNonBlocking(client_fd);
  auto impl = std::unique_ptr<TcpSocket::Impl>(new TcpSocket::Impl(
      executor_, client_fd, str_addr, client_addr.sin_port));
  TcpSocket peer(std::move(impl));
  peer.AddEpollWatch();
  handler(0, std::move(peer));
}

}  // namespace ario
