#ifndef NEXUS_COMMON_SPINLOCK_H_
#define NEXUS_COMMON_SPINLOCK_H_

#include <atomic>

namespace nexus {

class Spinlock {
 public:
  void Acquire() {
    while (flag_.test_and_set(std::memory_order_acquire))
      ;  // spin
  }

  void Release() { flag_.clear(std::memory_order_release); }

 private:
  std::atomic_flag flag_ = ATOMIC_FLAG_INIT;
};

class SpinlockGuard {
 public:
  SpinlockGuard(Spinlock& lock) : lock_(lock) { lock.Acquire(); }

  ~SpinlockGuard() { lock_.Release(); }

 private:
  Spinlock& lock_;
};

}  // namespace nexus

#endif  // NEXUS_COMMON_SPINLOCK_H_
