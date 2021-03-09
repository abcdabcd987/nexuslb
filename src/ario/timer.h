#pragma once
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <vector>

namespace ario {

struct TimerEvent;

class Timer {
 public:
  using Clock = std::chrono::system_clock;
  using TimePoint = std::chrono::time_point<Clock, std::chrono::nanoseconds>;

  explicit Timer();
  ~Timer();
  Timer(const Timer& other) = delete;
  Timer(Timer&& other) = delete;
  Timer& operator=(const Timer& other) = delete;
  Timer& operator=(Timer&& other) = delete;

  int fd() const { return timer_fd_; }

  void AddEvent(TimePoint timeout, std::function<void()> callback);
  std::optional<TimePoint> EarliestTimeout() const;
  void SetTimerFd(TimePoint timeout);
  void PopReadyTimerItems(std::queue<std::function<void()>>& out);

 private:
  friend bool HeapOrderByTimeoutASC(const TimerEvent& lhs,
                                    const TimerEvent& rhs);

  int timer_fd_ = -1;
  std::vector<TimerEvent> heap_;
};

struct TimerEvent {
  Timer::TimePoint timeout;
  std::function<void()> callback;
};

}  // namespace ario
