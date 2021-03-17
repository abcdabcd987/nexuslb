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
  void PushBack(std::function<void(ErrorCode)> &&callback, ErrorCode error);
  std::unique_ptr<CallbackBind> PopFront();

 private:
  std::list<std::unique_ptr<CallbackBind>> queue_;
};

}  // namespace ario
