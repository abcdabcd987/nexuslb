#pragma once

#include <functional>
#include <list>
#include <memory>

#include "ario/error.h"

namespace ario {

class CallbackQueue {
 public:
  struct CallbackBind {
    std::function<void(ErrorCode)> callback;
    ErrorCode error;
  };

  bool IsEmpty() const;
  void PushBack(std::function<void(ErrorCode)>&& callback, ErrorCode error);
  CallbackQueue::CallbackBind PopFront();
  void PopAll(std::list<CallbackBind>& out);

 private:
  std::list<CallbackBind> queue_;
};

}  // namespace ario
