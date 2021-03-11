#include "ario/timerfd.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <vector>

#include "ario/timer.h"
#include "ario/utils.h"

namespace ario {

TimerFD::TimerFD() {
  timer_fd_ = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK);
  if (timer_fd_ < 0) die_perror("timerfd_create");
}

TimerFD::~TimerFD() { close(timer_fd_); }

bool TimerFD::EnqueueTimer(TimerData& data, TimePoint timeout,
                           std::function<void()>&& callback) {
  if (data.heap_index == TimerData::kInvalidHeapIndex) {
    data.heap_index = heap_.size();
    heap_.push_back({timeout, &data});
    HeapUp(data.heap_index);
  }
  data.callbacks.emplace_back(std::move(callback));
  return data.heap_index == 0 && data.callbacks.size() == 1;
}

size_t TimerFD::CancelTimer(TimerData& data,
                            std::queue<std::function<void()>>& out) {
  size_t cnt_cancelled = 0;
  if (data.heap_index != TimerData::kInvalidHeapIndex) {
    cnt_cancelled = PopCallbacks(data, out);
    HeapRemove(data.heap_index);
  }
  return cnt_cancelled;
}

void TimerFD::MoveTimer(TimerData& dst, TimerData& src) {
  dst.callbacks.splice(dst.callbacks.end(), src.callbacks);
  dst.heap_index = src.heap_index;
  src.heap_index = TimerData::kInvalidHeapIndex;
  if (dst.heap_index != TimerData::kInvalidHeapIndex) {
    heap_[dst.heap_index].data = &dst;
  }
}

std::optional<TimePoint> TimerFD::EarliestTimeout() const {
  if (!heap_.empty()) {
    return heap_.front().timeout;
  }
  return std::nullopt;
}

void TimerFD::SetTimerFd(TimePoint timeout) {
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

size_t TimerFD::PopReadyTimerItems(std::queue<std::function<void()>>& out) {
  TimePoint now = Clock::now();
  size_t cnt_ready = 0;
  while (!heap_.empty()) {
    auto& element = heap_[0];
    if (element.timeout > now) {
      break;
    }

    cnt_ready += PopCallbacks(*element.data, out);
    HeapRemove(0);
  }
  return cnt_ready;
}

size_t TimerFD::PopCallbacks(TimerData& data,
                             std::queue<std::function<void()>>& out) {
  auto cnt = data.callbacks.size();
  for (auto& cb : data.callbacks) {
    out.emplace(std::move(cb));
  }
  data.callbacks.clear();
  return cnt;
}

void TimerFD::HeapSwap(size_t lhs, size_t rhs) {
  std::swap(heap_[lhs], heap_[rhs]);
  heap_[lhs].data->heap_index = lhs;
  heap_[rhs].data->heap_index = rhs;
}

void TimerFD::HeapUp(size_t index) {
  while (index > 0) {
    size_t parent = (index - 1) / 2;
    if (heap_[index].timeout >= heap_[parent].timeout) {
      break;
    }
    HeapSwap(index, parent);
    index = parent;
  }
}

void TimerFD::HeapDown(size_t index) {
  size_t left = index * 2 + 1;
  while (left < heap_.size()) {
    size_t child = (left + 1 == heap_.size() ||
                    heap_[left].timeout < heap_[left + 1].timeout)
                       ? left
                       : left + 1;
    if (heap_[index].timeout < heap_[child].timeout) {
      break;
    }
    HeapSwap(index, child);
    index = child;
    left = index * 2 + 1;
  }
}

void TimerFD::HeapRemove(size_t index) {
  if (index == heap_.size() - 1) {
    heap_.back().data->heap_index = TimerData::kInvalidHeapIndex;
    heap_.pop_back();
  } else {
    HeapSwap(index, heap_.size() - 1);
    heap_.back().data->heap_index = TimerData::kInvalidHeapIndex;
    heap_.pop_back();
    if (index > 0 && heap_[index].timeout < heap_[(index - 1) / 2].timeout) {
      HeapUp(index);
    } else {
      HeapDown(index);
    }
  }
}

}  // namespace ario
