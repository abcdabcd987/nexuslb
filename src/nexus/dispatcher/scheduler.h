#ifndef NEXUS_DISPATCHER_SCHEDULER_H_
#define NEXUS_DISPATCHER_SCHEDULER_H_

#include <memory>

#include "nexus/common/typedef.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class Dispatcher;

class Scheduler {
 public:
  class Builder {
   public:
    virtual ~Builder() = default;

   private:
    friend class Dispatcher;
    virtual std::unique_ptr<Scheduler> Build(
        std::unique_ptr<DispatcherAccessor> dispatcher) = 0;
  };

  virtual ~Scheduler() = default;
  virtual void RunAsWorker() = 0;
  virtual void Stop() = 0;
  virtual void AddModelSession(ModelSession model_session) = 0;
  virtual void AddBackend(NodeId backend_id) = 0;
  virtual CtrlStatus EnqueueQuery(DispatchRequest&& request) = 0;

 protected:
  explicit Scheduler(std::unique_ptr<DispatcherAccessor> dispatcher)
      : dispatcher_(std::move(dispatcher)) {}
  std::unique_ptr<DispatcherAccessor> dispatcher_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
