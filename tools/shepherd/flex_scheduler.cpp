#include "shepherd/flex_scheduler.h"

#include <glog/logging.h>

#include <boost/container/small_vector.hpp>
#include <boost/iterator/iterator_categories.hpp>
#include <boost/range.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/any_range.hpp>
#include <boost/range/join.hpp>
#include <chrono>
#include <optional>
#include <unordered_set>

#include "nexus/common/time_util.h"
#include "shepherd/common.h"

namespace nexus::shepherd {

FlexScheduler::FlexScheduler(boost::asio::io_context* io_context,
                             ShepherdConfig cfg)
    : io_context_(*CHECK_NOTNULL(io_context)),
      dctrl_(cfg.ctrl_latency),
      ddata_(cfg.data_latency),
      preempt_lambda_(cfg.preempt_lambda) {}

void FlexScheduler::Stop() {
  for (auto& [_, gctx] : gpus_) {
    gctx->free_timer.cancel();
  }
  for (auto& [_, qctx] : queries_) {
    qctx->frontend_stub.MarkQueryDropped(qctx->query.query_id);
  }
}

void FlexScheduler::AddModel(int model_id, int slo_ms,
                             const ModelProfile* profile) {
  CHECK(models_.count(model_id) == 0);
  models_.emplace(model_id, std::make_shared<ModelContext>(ModelContext{
                                slo_ms, *CHECK_NOTNULL(profile), {}}));
}

void FlexScheduler::AddGpu(int gpu_id, BackendStub* backend_stub) {
  CHECK(gpus_.count(gpu_id) == 0);
  gpus_.emplace(gpu_id,
                std::make_shared<GpuContext>(GpuContext{
                    gpu_id, *CHECK_NOTNULL(backend_stub), TimePoint::min(),
                    boost::asio::system_timer(io_context_), std::nullopt}));
}

FlexScheduler::BatchGenResult FlexScheduler::BatchGen(
    TimePoint sched_at, int gpu_id,
    boost::container::small_vector<int, 2> model_ids) {
  using namespace std::chrono;
  std::optional<BatchPlan> best;
  std::vector<std::shared_ptr<QueryContext>> timeouts;
  auto gctx = gpus_.at(gpu_id);

  // NOTE: OnGpuCompletion loops over all models. New request only need consider
  // the new model and the preempted model.
  boost::any_range<int, boost::forward_traversal_tag> models;
  if (model_ids.empty()) {
    models = boost::adaptors::keys(models_);
  } else {
    models = model_ids;
  }

  for (auto model_id : models) {
    auto& mctx = models_.at(model_id);

    // Line 4: Dequeue requests passing their deadlines from Q(m)
    auto fwd_elapse_bs1 = nanoseconds(
        static_cast<long>(mctx->profile.GetForwardLatency(1) * 1e3));
    while (!mctx->queue.empty()) {
      auto qctx = *mctx->queue.rbegin();
      auto finish_at = sched_at + dctrl_ + ddata_ + fwd_elapse_bs1;
      if (qctx->deadline >= finish_at) {
        break;
      } else {
        timeouts.push_back(qctx);
        mctx->queue.erase(qctx);
      }
    }

    // Line 5: Candidate request set S ← Q(m)
    // Line 6: if Bc(n) uses model m then
    // Line 7:     S ← Q(m) + Bc(n)
    //
    // NOTE: Optimization. We don't need to have a sepearate copy of S here.
    //   S == itertool.chain(gctx->current_batch->query_ids, mctx->queue)
    // Because requests in the current batch should have earlier deadlines than
    // the requests in the queue.
    int* current_batch_beg = nullptr;
    size_t current_batch_size = 0;
    if (gctx->current_batch && gctx->current_batch->model_id == model_id) {
      current_batch_beg = gctx->current_batch->query_ids.data();
      current_batch_size = gctx->current_batch->query_ids.size();
    }
    auto reqs = boost::range::join(
        boost::make_iterator_range_n(current_batch_beg, current_batch_size) |
            boost::adaptors::transformed(
                [&](int query_id) { return queries_.at(query_id); }),
        mctx->queue);

    // Line  8: Bg(n,m) ← empty
    // Line  9: for request r in S with ascending deadline do
    // Line 10:   if r can meet SLO with batch size |Bg(n,m))| then
    // Line 11:     Add r to Bg(n,m)
    // Line 12:   else
    // Line 13:     Break
    //
    // NOTE: I think Line 10 is a typo. Should be: |Bg(n,m)| + 1
    BatchPlan candidate;
    candidate.model_id = model_id;
    for (auto qctx : reqs) {
      auto exec_elapse = nanoseconds(static_cast<long>(
          mctx->profile.GetForwardLatency(candidate.query_ids.size() + 1)));
      auto finish_at = sched_at + dctrl_ + ddata_ + exec_elapse;
      if (qctx->deadline >= finish_at) {
        candidate.query_ids.push_back(qctx->query.query_id);
      } else {
        break;
      }
    }

    // Line 14: Bg(n) ← Bg(n,m) with largest batch size among all models
    if (!best.has_value() ||
        candidate.query_ids.size() > best->query_ids.size()) {
      candidate.exec_at =
          sched_at + dctrl_ + ddata_ * candidate.query_ids.size();
      double l = mctx->profile.GetForwardLatency(candidate.query_ids.size());
      candidate.finish_at =
          candidate.exec_at + nanoseconds(static_cast<long>(l * 1e3));
      best = std::move(candidate);
    }
  }

  return {best, std::move(timeouts)};
}

void FlexScheduler::OnGpuCompletion(int gpu_id) {
  // Remove finished batch
  auto gctx = gpus_.at(gpu_id);
  CHECK(gctx->current_batch.has_value());
  for (auto query_id : gctx->current_batch->query_ids) {
    CHECK(queries_.erase(query_id) > 0);
  }
  gctx->current_batch.reset();

  // Line 5: Bg,n ←BATCHGEN(n) # Largest feasible batch across all Qm
  // Line 6: Execute Bg,n and dequeue requests in Bg,n from model queue
  auto sched_at = Clock::now();
  auto ret = BatchGen(sched_at, gpu_id, {});
  if (ret.batch.has_value()) {
    AssignBatchPlan(ret.batch.value(), gpu_id, Preemption::kNo);
  }
  SendDroppedQueries(ret.timeouts);

  // Line 7: for each GPU n do
  // Line 8:   Bg,n ←BATCHGEN(n) # Update candidate batch
  //
  // NOTE: I don't understand why update other gpus? And where is other GPU's
  // candidate used?
  //
  // NOTE: Communicated with Hong on 2023-11-20. He said that the pseudocode
  // might be wrong. Just treat Line 7~8 as redundant. And let BatchGen process
  // all models instead of up to two models.
}

void FlexScheduler::AddQuery(Query query, FrontendStub* frontend_stub) {
  // Line 10: Enqueue r to corresponding queue
  auto mctx = models_.at(query.model_id);
  auto deadline = query.arrival_at + std::chrono::milliseconds(mctx->slo_ms);
  auto qctx = std::make_shared<QueryContext>(
      QueryContext{query, deadline, *CHECK_NOTNULL(frontend_stub)});
  mctx->queue.insert(qctx);
  queries_.emplace(query.query_id, qctx);
  auto sched_at = Clock::now();

  // NOTE: Added a outer loop to support the "go to Line 11".
  std::unordered_set<int> changed_models;
  changed_models.insert(query.model_id);
  while (!changed_models.empty()) {
    auto model_id = *changed_models.begin();
    changed_models.erase(model_id);

    // Line 11: for each GPU n do
    for (auto& [gpu_id, gctx] : gpus_) {
      // Line 12:   Bc,n ← The batch currently being executed on GPU n
      boost::container::small_vector<int, 2> model_ids;
      model_ids.push_back(model_id);
      int old_bs;
      if (gctx->current_batch.has_value()) {
        old_bs = gctx->current_batch->query_ids.size();
        int old_model_id = gctx->current_batch->model_id;
        if (old_model_id != model_id) {
          model_ids.push_back(old_model_id);
        }
      } else {
        old_bs = 0;
      }

      // Line 13:   Bg,n ←BATCHGEN(n)
      auto ret = BatchGen(sched_at, gpu_id, model_ids);
      auto new_bs = ret.batch.has_value() ? ret.batch->query_ids.size() : 0;

      if (old_bs == 0) {
        // Line 14:   if Bc = empty then
        // Line 15:     Execute Bg,n and dequeue requests in Bg,n
        if (new_bs > 0) {
          AssignBatchPlan(ret.batch.value(), gpu_id, Preemption::kNo);
        }
      } else if (new_bs >= preempt_lambda_ * old_bs && new_bs > 0) {
        // Line 16:   else if |Bg,n| ≥ λ×|Bc,n| then # Preemption rule
        // Line 17:     Preempt Bc,n
        // Line 18:     Execute Bg,n and dequeue requests in Bg,n
        // Line 19:     Treat requests in Bc,n as new arrivals (go to Line 11)
        AssignBatchPlan(ret.batch.value(), gpu_id, Preemption::kYes);
        if (ret.batch->model_id != model_id) {
          changed_models.insert(ret.batch->model_id);
        }
      }

      SendDroppedQueries(ret.timeouts);
    }
  }
}

void FlexScheduler::AssignBatchPlan(const BatchPlan& batch, int gpu_id,
                                    Preemption preempt) {
  auto gctx = gpus_.at(gpu_id);
  if (preempt == Preemption::kYes) {
    CHECK(gctx->current_batch.has_value());
    auto old_batch = gctx->current_batch.value();
    // Add preempted queries back to the queue
    auto old_mctx = models_.at(old_batch.model_id);
    for (auto query_id : old_batch.query_ids) {
      auto qctx = queries_.at(query_id);
      old_mctx->queue.insert(qctx);
    }
    gctx->current_batch.reset();
  }

  CHECK(!gctx->current_batch.has_value());
  auto mctx = models_.at(batch.model_id);
  for (auto query_id : batch.query_ids) {
    auto qctx = queries_.at(query_id);
    CHECK(mctx->queue.erase(qctx) > 0);
  }

  // Setup timer
  gctx->current_batch = batch;
  gctx->free_at = batch.finish_at;
  gctx->free_timer.expires_at(gctx->free_at - dctrl_);
  gctx->free_timer.async_wait([&] { OnGpuCompletion(gpu_id); });

  // Send batch to backend
  gctx->backend_stub.RunBatch(batch, preempt);
}

void FlexScheduler::SendDroppedQueries(
    const std::vector<std::shared_ptr<QueryContext>>& dropped) {
  // Since everything is fake, we don't bother to group queries by frontend.
  for (auto qctx : dropped) {
    qctx->frontend_stub.MarkQueryDropped(qctx->query.query_id);
  }
}

}  // namespace nexus::shepherd
