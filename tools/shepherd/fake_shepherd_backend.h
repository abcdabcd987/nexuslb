#pragma once
#include <boost/asio/io_context.hpp>
#include <boost/asio/system_timer.hpp>
#include <boost/system/error_code.hpp>
#include <deque>
#include <vector>

#include "shepherd/common.h"
#include "shepherd/fake_accessor.h"

namespace nexus::shepherd {

class FakeShepherdBackend : public BackendStub {
 public:
  FakeShepherdBackend(boost::asio::io_context* io_context,
                      FakeObjectAccessor* accessor, int gpu_id,
                      bool save_archive);
  int gpu_id() const { return gpu_id_; }
  const std::deque<BatchPlan>& batchplan_archive() const {
    return batchplan_archive_;
  }

  void RunBatch(BatchPlan plan, Preemption preempt) override;
  void DrainBatchPlans();
  void Stop();

 private:
  void OnBatchFinish(const BatchPlan& plan);
  void OnTimer(boost::system::error_code ec);
  void SetupTimer();
  void SaveBatchPlan(BatchPlan plan);

  boost::asio::io_context& io_context_;
  FakeObjectAccessor& accessor_;
  int gpu_id_;
  bool save_archive_;
  boost::asio::system_timer timer_;
  std::mutex mutex_;
  std::vector<BatchPlan> batchplans_;
  std::deque<BatchPlan> batchplan_archive_;
};

}  // namespace nexus::shepherd
