#include "ario/timer.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <vector>

#include "ario/epoll.h"
#include "ario/utils.h"

namespace ario {

bool HeapOrderByTimeoutASC(const TimerEvent& lhs, const TimerEvent& rhs) {
  return lhs.timeout > rhs.timeout;
}

Timer::Timer() {
  timer_fd_ = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK);
  if (timer_fd_ < 0) die_perror("timerfd_create");
}

Timer::~Timer() { close(timer_fd_); }

void Timer::AddEvent(TimePoint timeout, std::function<void()> callback) {
  heap_.push_back({timeout, std::move(callback)});
  std::push_heap(heap_.begin(), heap_.end(), HeapOrderByTimeoutASC);
}

std::optional<Timer::TimePoint> Timer::EarliestTimeout() const {
  if (!heap_.empty()) {
    return heap_.front().timeout;
  }
  return std::nullopt;
}

void Timer::SetTimerFd(TimePoint timeout) {
  auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(timeout);
  auto nanos =
      timeout - std::chrono::time_point_cast<std::chrono::nanoseconds>(seconds);
  itimerspec ts;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 0;
  ts.it_value.tv_sec = seconds.time_since_epoch().count();
  ts.it_value.tv_nsec = nanos.count();

  int ret = timerfd_settime(timer_fd_, TFD_TIMER_ABSTIME, &ts, nullptr);
  if (ret < 0) die_perror("timerfd_settime");
}

void Timer::PopReadyTimerItems(std::queue<std::function<void()>>& out) {
  TimePoint now = Clock::now();
  while (!heap_.empty()) {
    auto& item = heap_.front();
    if (heap_.front().timeout > now) {
      break;
    }
    out.emplace(std::move(heap_.front().callback));
    std::pop_heap(heap_.begin(), heap_.end(), HeapOrderByTimeoutASC);
    heap_.pop_back();
  }
}

}  // namespace ario
