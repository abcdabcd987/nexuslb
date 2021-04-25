#include "ario/epoll.h"

#include <immintrin.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>

#include "ario/error.h"
#include "ario/utils.h"

namespace ario {

thread_local EpollExecutor *EpollExecutor::this_thread_executor_ = nullptr;

EpollExecutor::EpollExecutor(PollerType poller_type)
    : poller_type_(poller_type), epoll_fd_(epoll_create1(0)) {
  if (epoll_fd_ < 0) die_perror("epoll_create1");

  if (poller_type == PollerType::kBlocking) {
    epoll_event event;
    event.events = EPOLLIN | EPOLLERR | EPOLLET;
    event.data.ptr = &interrupter_;
    int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, interrupter_.fd(), &event);
    if (ret < 0) die_perror("EPOLL_CTL_ADD interrupter");

    event.events = EPOLLIN | EPOLLET;
    event.data.ptr = &timerfd_;
    ret = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timerfd_.fd(), &event);
    if (ret < 0) die_perror("EPOLL_CTL_ADD timer");
  }
}

EpollExecutor::~EpollExecutor() { close(epoll_fd_); }

void EpollExecutor::RunEventLoop() {
  this_thread_executor_ = this;
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    ++cnt_workers_;
  }

  if (poller_type_ == PollerType::kBlocking) {
    LoopBlocking();
  } else {
    LoopSpinning();
  }

  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    --cnt_workers_;
  }
  this_thread_executor_ = nullptr;
  stop_cv_.notify_all();
}

void EpollExecutor::LoopBlocking() {
  constexpr size_t kMaxEvents = 64;
  struct epoll_event events[kMaxEvents];
  memset(&events, 0, sizeof(events));

  while (!stop_event_loop_) {
    int n = epoll_wait(epoll_fd_, events, kMaxEvents, -1);
    for (int i = 0; i < n; ++i) {
      auto *ptr = events[i].data.ptr;
      if (ptr == &interrupter_) {
        interrupter_.Reset();
      } else if (ptr == &timerfd_) {
        std::lock_guard<std::mutex> lock(mutex_);
        timerfd_.PopReadyTimerItems(callback_queue_);
        auto earliest = timerfd_.EarliestTimeout();
        if (earliest.has_value()) {
          timerfd_.SetTimerFd(earliest.value());
        }
      } else {
        auto *handler = static_cast<EpollEventHandler *>(events[i].data.ptr);
        handler->HandleEpollEvent(events[i].events);
      }
    }

    for (std::unique_ptr<CallbackQueue::CallbackBind> bind;;) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (callback_queue_.IsEmpty()) {
          break;
        }
        bind = std::move(callback_queue_.PopFront());
      }
      bind->callback(bind->error);
    }
  }
}

void EpollExecutor::LoopSpinning() {
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    if (cnt_workers_ != 1) {
      fprintf(stderr,
              "PollerTyper::kSpinning doesn't support multithreaded because of "
              "performance concerns for now.\n");
      std::abort();
    }
  }

  constexpr size_t kMaxEvents = 64;
  struct epoll_event events[kMaxEvents];
  memset(&events, 0, sizeof(events));

  while (!stop_event_loop_) {
    // File descriptors
    int n = epoll_wait(epoll_fd_, events, kMaxEvents, 0);
    for (int i = 0; i < n; ++i) {
      auto *ptr = events[i].data.ptr;
      auto *handler = static_cast<EpollEventHandler *>(ptr);
      handler->HandleEpollEvent(events[i].events);
    }

    // Custom poller (RDMA)
    for (auto *poller : event_pollers_) {
      poller->Poll();
    }

    // Timer
    timerfd_.PopReadyTimerItems(callback_queue_);

    // Posted events
    while (!callback_queue_.IsEmpty()) {
      auto bind = callback_queue_.PopFront();
      bind->callback(bind->error);
    }

    _mm_pause();
  }
}

void EpollExecutor::StopEventLoop() {
  // TODO: stop more elegantly
  stop_event_loop_ = true;

  std::unique_lock<std::mutex> lock(stop_mutex_);
  stop_cv_.wait(lock, [this] {
    if (cnt_workers_ && poller_type_ == PollerType::kBlocking) {
      interrupter_.Interrupt();
    }
    return cnt_workers_ == 0 ||
           (cnt_workers_ == 1 && this_thread_executor_ == this);
  });
}

void EpollExecutor::Post(std::function<void(ErrorCode)> &&func,
                         ErrorCode error) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_queue_.PushBack(std::move(func), error);
  }
  if (poller_type_ == PollerType::kBlocking) {
    interrupter_.Interrupt();
  }
}

void EpollExecutor::ScheduleTimer(TimerData &data, TimePoint timeout,
                                  std::function<void(ErrorCode)> &&callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  bool earliest = timerfd_.EnqueueTimer(data, timeout, std::move(callback));
  if (earliest && poller_type_ == PollerType::kBlocking) {
    timerfd_.SetTimerFd(timeout);
  }
}

size_t EpollExecutor::CancelTimer(TimerData &data) {
  size_t cnt_cancelled;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    cnt_cancelled = timerfd_.CancelTimer(data, callback_queue_);
  }
  if (cnt_cancelled && poller_type_ == PollerType::kBlocking) {
    interrupter_.Interrupt();
  }
  return cnt_cancelled;
}

void EpollExecutor::MoveTimer(TimerData &dst, TimerData &src) {
  size_t cnt_cancelled;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    cnt_cancelled = timerfd_.CancelTimer(dst, callback_queue_);
    timerfd_.MoveTimer(dst, src);
  }
  if (cnt_cancelled && poller_type_ == PollerType::kBlocking) {
    interrupter_.Interrupt();
  }
}

void EpollExecutor::WatchFD(int fd, EpollEventHandler &handler) {
  struct epoll_event event;
  event.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLPRI | EPOLLET;
  event.data.ptr = &handler;
  int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event);
  if (ret < 0) die_perror("EpollExecutor::WatchFD");
}

void EpollExecutor::AddPoller(EventPoller &poller) {
  std::lock_guard<std::mutex> lock(mutex_);
  event_pollers_.push_back(&poller);
}

}  // namespace ario
