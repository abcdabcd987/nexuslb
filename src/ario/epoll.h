#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>

#include "ario/callback_queue.h"
#include "ario/chrono.h"
#include "ario/error.h"
#include "ario/timerfd.h"

namespace ario {

class Timer;

// Design and implementation copied from Boost.Asio. See:
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/eventfd_select_interrupter.hpp
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/impl/eventfd_select_interrupter.ipp
class Interrupter {
 public:
  Interrupter();
  ~Interrupter();
  Interrupter(const Interrupter &other) = delete;
  Interrupter &operator=(const Interrupter &other) = delete;
  Interrupter(Interrupter &&other) = delete;
  Interrupter &operator=(Interrupter &&other) = delete;

  int fd() const;
  void Interrupt();
  void Reset();

 private:
  const int event_fd_;
};

class EpollEventHandler {
 public:
  virtual void HandleEpollEvent(uint32_t epoll_events) = 0;
};

// Design and implementation copied from Boost.Asio. See:
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/epoll_reactor.hpp
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/impl/epoll_reactor.hpp
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/impl/epoll_reactor.ipp
class EpollExecutor {
 public:
  EpollExecutor();
  ~EpollExecutor();
  EpollExecutor(const EpollExecutor &other) = delete;
  EpollExecutor &operator=(const EpollExecutor &other) = delete;
  EpollExecutor(EpollExecutor &&other) = delete;
  EpollExecutor &operator=(EpollExecutor &&other) = delete;

  void RunEventLoop();
  void StopEventLoop();
  void Post(std::function<void(ErrorCode)> &&func, ErrorCode error);
  void ScheduleTimer(TimerData &data, TimePoint timeout,
                     std::function<void(ErrorCode)> &&callback);
  size_t CancelTimer(TimerData &data);
  void MoveTimer(TimerData &dst, TimerData &src);

 private:
  friend void EpollExecutorAddEpollWatch(EpollExecutor &executor, int fd,
                                         EpollEventHandler &handler);

  int epoll_fd_;
  std::atomic<bool> stop_event_loop_{false};
  Interrupter interrupter_;

  std::mutex mutex_;
  TimerFD timerfd_ /* GUARDED_BY(mutex_) */;
  CallbackQueue callback_queue_ /* GUARDED_BY(mutex_) */;
};

}  // namespace ario
