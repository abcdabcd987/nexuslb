#include "ario/callback_queue.h"

namespace ario {

bool CallbackQueue::IsEmpty() const { return queue_.empty(); }

void CallbackQueue::PushBack(std::function<void(ErrorCode)>&& callback,
                             ErrorCode error) {
  auto* bind = new CallbackBind{std::move(callback), error};
  queue_.emplace_back(bind);
}

std::unique_ptr<CallbackQueue::CallbackBind> CallbackQueue::PopFront() {
  auto bind = std::move(queue_.front());
  queue_.pop_front();
  return bind;
}

}  // namespace ario
