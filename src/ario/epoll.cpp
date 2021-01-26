#include "ario/epoll.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cstring>

#include "ario/utils.h"

namespace ario {

Interrupter::Interrupter() : event_fd_(eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) {
  if (event_fd_ < 0) die_perror("eventfd");
}

Interrupter::~Interrupter() { close(event_fd_); }

int Interrupter::fd() const { return event_fd_; }

void Interrupter::Interrupt() {
  uint64_t counter = 1;
  int ret = write(event_fd_, &counter, sizeof(counter));
  (void)ret;
}

void Interrupter::Reset() {
  uint64_t counter = 0;
  int ret = read(event_fd_, &counter, sizeof(counter));
  (void)ret;
}

EpollExecutor::EpollExecutor() : epoll_fd_(epoll_create1(0)) {
  if (epoll_fd_ < 0) die_perror("epoll_create1");

  epoll_event event;
  event.events = EPOLLIN | EPOLLERR | EPOLLET;
  event.data.ptr = &interrupter_;
  int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, interrupter_.fd(), &event);
  if (ret < 0) die_perror("EPOLL_CTL_ADD interrupter");
}

EpollExecutor::~EpollExecutor() { close(epoll_fd_); }

void EpollExecutor::RunEventLoop() {
  constexpr size_t kMaxEvents = 64;
  struct epoll_event events[kMaxEvents];
  memset(&events, 0, sizeof(events));

  while (!stop_event_loop_) {
    int n = epoll_wait(epoll_fd_, events, kMaxEvents, -1);
    for (int i = 0; i < n; ++i) {
      auto *ptr = events[i].data.ptr;
      if (ptr == &interrupter_) {
        interrupter_.Reset();
      } else {
        auto *handler = static_cast<EpollEventHandler *>(events[i].data.ptr);
        handler->HandleEpollEvent(events[i].events);
      }
    }

    for (std::optional<std::function<void()>> func;;) {
      func = PopPostQueue();
      if (func.has_value()) {
        (*func)();
      } else {
        break;
      }
    }
  }
}

void EpollExecutor::StopEventLoop() {
  stop_event_loop_ = true;
  interrupter_.Interrupt();
}

void EpollExecutor::Post(std::function<void()> &&func) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    post_queue_.emplace(std::move(func));
  }
  interrupter_.Interrupt();
}

std::optional<std::function<void()>> EpollExecutor::PopPostQueue() {
  std::optional<std::function<void()>> ret;
  std::lock_guard<std::mutex> lock(mutex_);
  if (!post_queue_.empty()) {
    ret.emplace(std::move(post_queue_.front()));
    post_queue_.pop();
  }
  return ret;
}

void EpollExecutorAddEpollWatch(EpollExecutor &executor, int fd,
                                EpollEventHandler &handler) {
  struct epoll_event event;
  event.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLPRI | EPOLLET;
  event.data.ptr = &handler;
  int ret = epoll_ctl(executor.epoll_fd_, EPOLL_CTL_ADD, fd, &event);
  if (ret < 0) die_perror("EpollExecutorAddEpollWatch");
}

}  // namespace ario
