#ifndef NEXUS_BACKEND_TENSORFLOW_MODEL_H_
#define NEXUS_BACKEND_TENSORFLOW_MODEL_H_

#include "nexus/backend/model_ins.h"
#include "nexus/backend/tensorflow_wrapper.h"

namespace nexus {
namespace backend {

class TFShareModel;

class TensorflowModel : public ModelInstance {
 public:
  TensorflowModel(int gpu_id, const ModelInstanceConfig& config,
                  ModelIndex model_index);

  ~TensorflowModel();

  void WarmupInputArray(std::shared_ptr<Array> input_array) override;

  Shape InputShape() final;

  std::unordered_map<std::string, Shape> OutputShapes() final;

  ArrayPtr CreateInputGpuArray() final;

  std::unordered_map<std::string, ArrayPtr> GetOutputGpuArrays() final;

  void Preprocess(std::shared_ptr<Task> task) final;

  void Forward(std::shared_ptr<BatchTask> batch_task) final;

  void Postprocess(std::shared_ptr<Task> task) final;

  uint64_t GetPeakBytesInUse() override;

  uint64_t GetBytesInUse() override;

 private:
  tf::Tensor NewInputTensor();

  std::vector<tf::Tensor> WarmupInputTensor(tf::Tensor in_tensor);

  void MarshalDetectionResult(const QueryProto& query,
                              std::shared_ptr<Output> output, int im_height,
                              int im_width, QueryResultProto* result);

  int image_height_;
  int image_width_;
  std::string input_layer_;
  Shape input_shape_;
  size_t input_size_;
  DataType input_data_type_;
  std::vector<std::string> output_layers_;
  std::unordered_map<std::string, Shape> output_shapes_;
  std::unordered_map<std::string, size_t> output_sizes_;
  std::vector<float> input_mean_;
  std::vector<float> input_std_;
  std::unordered_map<int, std::string> classnames_;
  std::vector<tf::Tensor> input_tensors_;
  bool first_input_array_;

  std::unique_ptr<tf::Session> session_;
};

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_TENSORFLOW_MODEL_H_
