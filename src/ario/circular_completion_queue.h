#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <utility>
#include <vector>

namespace ario {

template <typename T, size_t N>
class CircularCompletionQueue {
 public:
  static_assert(N && (N & (N - 1)) == 0, "N should be power of 2");

  CircularCompletionQueue() : pending_(N, false) {
    buf_ = static_cast<T*>(std::calloc(N, sizeof(T)));
  }

  ~CircularCompletionQueue() { std::free(buf_); }

  CircularCompletionQueue(const CircularCompletionQueue&) = delete;
  CircularCompletionQueue& operator=(const CircularCompletionQueue&) = delete;
  CircularCompletionQueue(CircularCompletionQueue&&) = delete;
  CircularCompletionQueue& operator=(CircularCompletionQueue&&) = delete;

  size_t Capacity() const { return N; }
  size_t Size() const { return end_ - beg_; }
  bool IsEmpty() const { return beg_ == end_; }
  bool IsFull() const { return tail_ == head_ && beg_ != end_; }

  uint64_t Enqueue(T&& value) {
    if (IsFull()) {
      throw std::length_error("Enqueue");
    }
    new (&buf_[tail_]) T(std::move(value));
    pending_[tail_] = true;
    tail_ = (tail_ + 1) % N;
    return end_++;
  }

  T Dequeue(uint64_t id) {
    if (id < beg_ || id >= end_) {
      throw std::out_of_range("Dequeue: !(beg <= id && id < end_)");
    }
    size_t idx = (head_ + (id - beg_)) % N;
    if (!pending_[idx]) {
      throw std::out_of_range("Dequeue: !pending[idx]");
    }
    T value = std::move(buf_[idx]);
    buf_[idx].~T();
    pending_[idx] = false;

    while (beg_ != end_ && !pending_[head_]) {
      ++beg_;
      head_ = (head_ + 1) % N;
    }
    return value;
  }

 private:
  T* buf_;
  std::vector<bool> pending_;
  size_t head_ = 0;
  size_t tail_ = 0;
  uint64_t beg_ = 1;
  uint64_t end_ = 1;
};

}  // namespace ario
