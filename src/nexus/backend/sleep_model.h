#ifndef NEXUS_BACKEND_SLEEP_MODEL_H_
#define NEXUS_BACKEND_SLEEP_MODEL_H_

#include <vector>

#include "nexus/backend/model_ins.h"
#include "nexus/common/sleep_profile.h"

namespace nexus {
namespace backend {

class SleepModel : public ModelInstance {
 public:
  SleepModel(SleepProfile profile, const ModelInstanceConfig& config,
             ModelIndex model_index);

  Shape InputShape() override;
  std::unordered_map<std::string, Shape> OutputShapes() override;
  ArrayPtr CreateInputGpuArray() override;
  void WarmupInputArray(std::shared_ptr<Array> input_array) override;
  std::unordered_map<std::string, ArrayPtr> GetOutputGpuArrays() override;
  void Preprocess(std::shared_ptr<Task> task) override;
  void Forward(std::shared_ptr<BatchTask> batch_task) override;
  void Postprocess(std::shared_ptr<Task> task) override;
  uint64_t GetPeakBytesInUse() override;
  uint64_t GetBytesInUse() override;

 private:
  SleepProfile profile_;

  int image_height_;
  int image_width_;
  std::string input_layer_;
  Shape input_shape_;
  size_t input_size_;
  DataType input_data_type_;
  std::vector<std::string> output_layers_;
  std::unordered_map<std::string, Shape> output_shapes_;
  std::unordered_map<std::string, size_t> output_sizes_;
};

}  // namespace backend
}  // namespace nexus

#endif
