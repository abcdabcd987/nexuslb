#include "ario/callback_queue.h"

namespace ario {

bool CallbackQueue::IsEmpty() const { return queue_.empty(); }

void CallbackQueue::PushBack(std::function<void(ErrorCode)>&& callback,
                             ErrorCode error) {
  queue_.push_back({std::move(callback), error});
}

CallbackQueue::CallbackBind CallbackQueue::PopFront() {
  auto bind = std::move(queue_.front());
  queue_.pop_front();
  return bind;
}

void CallbackQueue::PopAll(std::list<CallbackBind>& out) {
  out.splice(out.end(), queue_);
}

}  // namespace ario
