#pragma once
#include <cstdint>
#include <functional>
#include <limits>
#include <list>

#include "ario/chrono.h"
#include "ario/error.h"
#include "ario/small_function.h"

namespace ario {

class EpollExecutor;

// Design and implementation copied from Boost.Asio. See:
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/timer_queue.hpp#L47
class TimerData {
 public:
  static constexpr size_t kInvalidHeapIndex =
      std::numeric_limits<size_t>::max();

  TimerData();
  TimerData(const TimerData& other) = delete;
  TimerData& operator=(const TimerData& other) = delete;
  TimerData(TimerData&& other) = delete;
  TimerData& operator=(TimerData&& other) = delete;

  std::list<std::function<void(ErrorCode)>> callbacks;
  size_t heap_index;
};

// Design and implementation copied from Boost.Asio. See:
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/deadline_timer_service.hpp
class Timer {
 public:
  Timer();
  explicit Timer(EpollExecutor& executor);
  Timer(EpollExecutor& executor, TimePoint timeout);
  Timer(EpollExecutor& executor, TimePoint timeout,
        SmallFunction<void(ErrorCode)>&& callback);
  ~Timer();
  Timer(const Timer& other) = delete;
  Timer& operator=(const Timer& other) = delete;
  Timer(Timer&& other);
  Timer& operator=(Timer&& other);

  TimePoint timeout() const { return timeout_; }

  size_t CancelAll();
  size_t SetTimeout(TimePoint timeout);
  void AsyncWaitBigCallback(std::function<void(ErrorCode)>&& callback);

  void AsyncWait(SmallFunction<void(ErrorCode)>&& callback) {
    AsyncWaitBigCallback(callback.Release());
  }

 private:
  EpollExecutor* executor_;
  TimePoint timeout_;
  TimerData data_;
};

}  // namespace ario
