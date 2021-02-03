#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>

namespace ario {

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
  void Post(std::function<void()> &&func);

 private:
  friend void EpollExecutorAddEpollWatch(EpollExecutor &executor, int fd,
                                         EpollEventHandler &handler);
  std::optional<std::function<void()>> PopPostQueue() /* REQUIRES(mutex_) */;

  const int epoll_fd_;
  std::atomic<bool> stop_event_loop_{false};
  Interrupter interrupter_;

  std::mutex mutex_;
  std::queue<std::function<void()>> post_queue_ /* GUARDED_BY(mutex_) */;
};

}  // namespace ario