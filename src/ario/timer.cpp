#include "ario/timer.h"

#include <utility>

#include "ario/epoll.h"

namespace ario {

TimerData::TimerData() : callbacks(), heap_index(kInvalidHeapIndex) {}

Timer::Timer() : executor_(nullptr), timeout_(), data_() {}

Timer::Timer(EpollExecutor& executor)
    : executor_(&executor), timeout_(), data_() {}

Timer::Timer(EpollExecutor& executor, TimePoint timeout)
    : executor_(&executor), timeout_(timeout), data_() {}

Timer::Timer(EpollExecutor& executor, TimePoint timeout,
             SmallFunction<void(ErrorCode)>&& callback)
    : executor_(&executor), timeout_(timeout), data_() {
  AsyncWait(std::move(callback));
}

Timer::~Timer() { CancelAll(); }

Timer::Timer(Timer&& other)
    : executor_(std::exchange(other.executor_, nullptr)),
      timeout_(std::exchange(other.timeout_, {})),
      data_() {
  executor_->MoveTimer(data_, other.data_);
}

Timer& Timer::operator=(Timer&& other) {
  if (this != &other) {
    CancelAll();
    executor_ = std::exchange(other.executor_, nullptr);
    timeout_ = std::exchange(other.timeout_, {});
    executor_->MoveTimer(data_, other.data_);
  }
  return *this;
}

size_t Timer::CancelAll() {
  if (executor_) {
    return executor_->CancelTimer(data_);
  }
  return 0;
}

size_t Timer::SetTimeout(TimePoint timeout) {
  size_t cnt_cancelled = CancelAll();
  timeout_ = timeout;
  return cnt_cancelled;
}

void Timer::AsyncWaitBigCallback(std::function<void(ErrorCode)>&& callback) {
  if (!executor_) {
    throw std::invalid_argument("Timer::AsyncWait: !executor_");
  }
  executor_->ScheduleTimer(data_, timeout_, std::move(callback));
}

}  // namespace ario
