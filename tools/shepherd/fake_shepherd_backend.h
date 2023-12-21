#pragma once
#include <boost/asio/io_context.hpp>
#include <boost/asio/system_timer.hpp>
#include <deque>
#include <vector>

#include "shepherd/common.h"
#include "shepherd/fake_accessor.h"

namespace nexus::shepherd {

class FakeShepherdBackend {
 public:
  class Stub : public BackendStub {
   public:
    explicit Stub(FakeShepherdBackend* super) : super_(super) {}
    void RunBatch(BatchPlan plan, std::optional<int> preempt_batch_id) override;

   private:
    FakeShepherdBackend* super_;
  };

  FakeShepherdBackend(boost::asio::io_context* io_context,
                      FakeObjectAccessor* accessor, int gpu_id,
                      bool save_archive);
  int gpu_id() const { return gpu_id_; }
  const std::deque<BatchPlan>& batchplan_archive() const {
    return batchplan_archive_;
  }
  Stub& stub() { return stub_; }

  void DrainBatchPlans();
  void Stop();

 private:
  void RunBatchInternal(BatchPlan plan, std::optional<int> preempt_batch_id);
  void OnBatchFinish(const BatchPlan& plan);
  void OnTimer();
  void SetupTimer();
  void SaveBatchPlan(BatchPlan plan);

  boost::asio::io_context& io_context_;
  FakeObjectAccessor& accessor_;
  int gpu_id_;
  bool save_archive_;
  boost::asio::system_timer timer_;
  Stub stub_;
  std::vector<BatchPlan> batchplans_;
  std::deque<BatchPlan> batchplan_archive_;
};

}  // namespace nexus::shepherd
