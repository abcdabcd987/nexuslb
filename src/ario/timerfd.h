#pragma once
#include <chrono>
#include <functional>
#include <optional>
#include <queue>
#include <vector>

#include "ario/chrono.h"

namespace ario {

class TimerData;

// Design and implementation copied from Boost.Asio. See:
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/timer_queue.hpp
class TimerFD {
 public:
  explicit TimerFD();
  ~TimerFD();
  TimerFD(const TimerFD& other) = delete;
  TimerFD(TimerFD&& other) = delete;
  TimerFD& operator=(const TimerFD& other) = delete;
  TimerFD& operator=(TimerFD&& other) = delete;

  int fd() const { return timer_fd_; }

  bool EnqueueTimer(TimerData& data, TimePoint timeout,
                    std::function<void()>&& callback);
  size_t CancelTimer(TimerData& data, std::queue<std::function<void()>>& out);
  void MoveTimer(TimerData& dst, TimerData& src);
  std::optional<TimePoint> EarliestTimeout() const;
  void SetTimerFd(TimePoint timeout);
  size_t PopReadyTimerItems(std::queue<std::function<void()>>& out);

 private:
  struct HeapElement {
    TimePoint timeout;
    TimerData* data;
  };

  size_t PopCallbacks(TimerData& data, std::queue<std::function<void()>>& out);
  void HeapSwap(size_t lhs, size_t rhs);
  void HeapUp(size_t index);
  void HeapDown(size_t index);
  void HeapRemove(size_t index);

  int timer_fd_ = -1;
  std::vector<HeapElement> heap_;
};

}  // namespace ario
