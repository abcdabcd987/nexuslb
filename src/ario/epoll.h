#pragma once
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>

#include "ario/callback_queue.h"
#include "ario/chrono.h"
#include "ario/error.h"
#include "ario/interrupter.h"
#include "ario/small_function.h"
#include "ario/timerfd.h"

namespace ario {

class Timer;

class EpollEventHandler {
 public:
  virtual void HandleEpollEvent(uint32_t epoll_events) = 0;
};

class EventPoller {
 public:
  virtual void Poll() = 0;
};

enum class PollerType {
  kBlocking,
  kSpinning,
};

// Design and implementation copied from Boost.Asio. See:
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/epoll_reactor.hpp
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/impl/epoll_reactor.hpp
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/impl/epoll_reactor.ipp
class EpollExecutor {
 public:
  explicit EpollExecutor(PollerType poller_type);
  ~EpollExecutor();
  EpollExecutor(const EpollExecutor &other) = delete;
  EpollExecutor &operator=(const EpollExecutor &other) = delete;
  EpollExecutor(EpollExecutor &&other) = delete;
  EpollExecutor &operator=(EpollExecutor &&other) = delete;
  static EpollExecutor *ThisThreadExecutor() { return this_thread_executor_; }
  PollerType poller_type() const { return poller_type_; }

  void WatchFD(int fd, EpollEventHandler &handler);
  void AddPoller(EventPoller &poller);

  void RunEventLoop();
  void StopEventLoop();
  void PostBigCallback(std::function<void(ErrorCode)> &&func, ErrorCode error);
  void PostOk(SmallFunction<void(ErrorCode)> &&func) {
    PostBigCallback(func.Release(), ErrorCode::kOk);
  }

  void ScheduleTimer(TimerData &data, TimePoint timeout,
                     std::function<void(ErrorCode)> &&callback);
  size_t CancelTimer(TimerData &data);
  void MoveTimer(TimerData &dst, TimerData &src);

 private:
  void LoopBlocking();
  void LoopSpinning();

  static thread_local EpollExecutor *this_thread_executor_;

  const PollerType poller_type_;
  int epoll_fd_;
  std::atomic<bool> stop_event_loop_{false};
  Interrupter interrupter_;

  std::mutex stop_mutex_;
  size_t cnt_workers_ /* GUARDED_BY(stop_mutex_) */ = 0;
  std::condition_variable stop_cv_;

  std::mutex mutex_;
  TimerFD timerfd_ /* GUARDED_BY(mutex_) */;
  CallbackQueue callback_queue_ /* GUARDED_BY(mutex_) */;

  std::mutex event_pollers_write_mutex_;
  std::atomic<size_t> event_pollers_size_{0};
  std::vector<EventPoller *> event_pollers_;
};

}  // namespace ario
