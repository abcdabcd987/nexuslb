#include "nexus/dispatcher/rankmt/scheduler.h"

#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ario/epoll.h"
#include "ario/error.h"
#include "ario/timer.h"
#include "nexus/common/functional.h"
#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/batch_policy.h"
#include "nexus/dispatcher/dispatcher.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

MultiThreadRankScheduler::Builder::Builder(
    ario::EpollExecutor* scheduler_executor,
    ario::EpollExecutor* rank_thread_executor)
    : scheduler_executor_(CHECK_NOTNULL(scheduler_executor)),
      rank_thread_executor_(CHECK_NOTNULL(rank_thread_executor)) {}

std::unique_ptr<MultiThreadRankScheduler>
MultiThreadRankScheduler::Builder::Build() {
  return std::make_unique<MultiThreadRankScheduler>(scheduler_executor_,
                                                    rank_thread_executor_);
}

MultiThreadRankScheduler::MultiThreadRankScheduler(
    ario::EpollExecutor* scheduler_executor,
    ario::EpollExecutor* rank_thread_executor)
    : executor_(*CHECK_NOTNULL(scheduler_executor)),
      rank_thread_(rank_thread_executor) {}

MultiThreadRankScheduler::RequestEntrance::RequestEntrance(
    ModelThread* model_thread)
    : model_thread_(model_thread) {}

CtrlStatus MultiThreadRankScheduler::RequestEntrance::EnqueueQuery(
    DispatchRequest&& request) {
  CHECK(model_thread_ != nullptr);
  return model_thread_->EnqueueQuery(std::move(request));
}

MultiThreadRankScheduler::~MultiThreadRankScheduler() {
  // TODO
}

void MultiThreadRankScheduler::Stop() {
  // TODO
  LOG(INFO) << "MultiThreadRankScheduler::Stop";
  std::mutex mutex;
  size_t cnt = 0;
  std::condition_variable cv;
  for (auto& model_thread : model_threads_) {
    model_thread->Stop(mutex, cnt, cv);
  }
  rank_thread_.Stop(mutex, cnt, cv);
  {
    size_t target = model_threads_.size() + 1;
    std::unique_lock lock(mutex);
    cv.wait(lock, [target, &cnt] { return cnt == target; });
  }
  model_threads_.clear();
}

MultiThreadRankScheduler::RequestEntrance
MultiThreadRankScheduler::AddModelSession(
    ario::EpollExecutor* model_thread_executor, ModelSession model_session) {
  CHECK_NE(model_thread_executor, nullptr);
  if (backends_.empty()) {
    LOG(FATAL) << "Add backend before adding model sessions.";
  }

  auto model_session_id = ModelSessionToString(model_session);
  if (model_index_table_.count(model_session_id)) {
    LOG(FATAL) << "Model session already exists. model_session="
               << model_session_id
               << " model_index=" << model_index_table_[model_session_id];
  }

  CHECK_EQ(model_threads_.size(), model_index_table_.size());
  ModelIndex model_index(model_index_table_.size());
  model_index_table_[model_session_id] = model_index;
  model_threads_.emplace_back(std::make_unique<ModelThread>(
      model_thread_executor, model_session, model_index, &rank_thread_,
      frontends_, backends_));
  auto* model_thread = model_threads_.back().get();
  rank_thread_.PostAddModelThread(model_index, model_thread);
  return RequestEntrance(model_thread);
}

void MultiThreadRankScheduler::AddBackend(
    NodeId backend_id, std::shared_ptr<BackendDelegate> delegate) {
  // Update RankThread and ModelThread
  backends_[backend_id] = delegate;
  rank_thread_.PostAddBackend(backend_id, delegate);
  for (auto& model_thread : model_threads_) {
    if (!model_thread) {
      continue;
    }
    model_thread->PostAddBackend(backend_id, delegate);
  }
}

void MultiThreadRankScheduler::AddFrontend(
    NodeId frontend_id, std::shared_ptr<FrontendDelegate> delegate) {
  frontends_[frontend_id] = delegate;
  for (auto& model_thread : model_threads_) {
    if (!model_thread) {
      continue;
    }
    model_thread->PostAddFrontend(frontend_id, delegate);
  }
}

void MultiThreadRankScheduler::RemoveBackend(NodeId backend_id) {
  backends_.erase(backend_id);
  rank_thread_.PostRemoveBackend(backend_id);
  for (auto& model_thread : model_threads_) {
    if (!model_thread) {
      continue;
    }
    model_thread->PostRemoveBackend(backend_id);
  }
}

void MultiThreadRankScheduler::RemoveFrontend(NodeId frontend_id) {
  frontends_.erase(frontend_id);
  for (auto& model_thread : model_threads_) {
    if (!model_thread) {
      continue;
    }
    model_thread->PostRemoveFrontend(frontend_id);
  }
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
