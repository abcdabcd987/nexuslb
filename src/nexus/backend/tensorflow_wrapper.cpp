#include "nexus/backend/tensorflow_wrapper.h"

#include <memory>
#include <string>

#include "tensorflow/core/common_runtime/gpu/gpu_process_state.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/public/session.h"

namespace nexus {
namespace backend {
namespace tf {

class TensorProxy {
  static_assert(sizeof(tensorflow::Tensor) >= sizeof(Tensor::impl_),
                "Tensor::impl_ too small.");

 public:
  static Tensor CopyFromTensorFlow(const tensorflow::Tensor& tf_tensor) {
    Tensor tensor;
    new (tensor.impl_.data()) tensorflow::Tensor(tf_tensor);
    return tensor;
  }

  static tensorflow::Tensor& AsTensorFlow(Tensor& tensor) {
    auto* tf_tensor =
        reinterpret_cast<tensorflow::Tensor*>(tensor.impl_.data());
    return *tf_tensor;
  }

  static const tensorflow::Tensor& AsTensorFlow(const Tensor& tensor) {
    const auto* tf_tensor =
        reinterpret_cast<const tensorflow::Tensor*>(tensor.impl_.data());
    return *tf_tensor;
  }
};

Tensor::Tensor() { new (impl_.data()) tensorflow::Tensor(); }

Tensor::~Tensor() { TensorProxy::AsTensorFlow(*this).~Tensor(); }

Tensor::Tensor(const Tensor& other) {
  new (impl_.data()) tensorflow::Tensor(TensorProxy::AsTensorFlow(other));
}

Tensor& Tensor::operator=(const Tensor& other) {
  TensorProxy::AsTensorFlow(*this).~Tensor();
  new (impl_.data()) tensorflow::Tensor(TensorProxy::AsTensorFlow(other));
  return *this;
}

Tensor::Tensor(Tensor&& other) {
  new (impl_.data())
      tensorflow::Tensor(std::move(TensorProxy::AsTensorFlow(other)));
}

Tensor& Tensor::operator=(Tensor&& other) {
  new (impl_.data())
      tensorflow::Tensor(std::move(TensorProxy::AsTensorFlow(other)));
  return *this;
}

std::vector<size_t> Tensor::shape() const {
  const auto& tf_tensor = TensorProxy::AsTensorFlow(*this);
  std::vector<size_t> shape;
  const auto& tf_shape = tf_tensor.shape();
  shape.reserve(tf_shape.dims());
  for (auto dim : tf_shape) {
    shape.push_back(dim.size);
  }
  return shape;
}

void* Tensor::data() const {
  const auto& tf_tensor = TensorProxy::AsTensorFlow(*this);
  return tf_tensor.data();
}

int64_t Tensor::NumElements() const {
  const auto& tf_tensor = TensorProxy::AsTensorFlow(*this);
  return tf_tensor.NumElements();
}

Tensor Tensor::Slice(int64_t dim0_start, int64_t dim0_limit) const {
  const auto& tf_tensor = TensorProxy::AsTensorFlow(*this);
  auto tf_slice = tf_tensor.Slice(dim0_start, dim0_limit);
  return TensorProxy::CopyFromTensorFlow(tf_slice);
}

class Session::Impl {
 public:
  Impl() = default;
  tensorflow::SessionOptions gpu_option;
  tensorflow::SessionOptions cpu_option;
  std::unique_ptr<tensorflow::Session> session;
  tensorflow::Allocator* tf_allocator;
};

Session::Session(const std::string& visible_device_list,
                 double per_process_gpu_memory_fraction,
                 const std::string& pb_path)
    : impl_(std::make_unique<Impl>()) {
  // Init session options
#ifdef USE_GPU
  auto& tf_option = impl_->gpu_option;
  auto gpu_opt = impl_->gpu_option.config.mutable_gpu_options();
  gpu_opt->set_visible_device_list(visible_device_list);
  gpu_opt->set_allocator_type("BFC");
  if (per_process_gpu_memory_fraction) {
    gpu_opt->set_per_process_gpu_memory_fraction(
        per_process_gpu_memory_fraction);
  } else {
    // Need to set allow_growth, otherwise TensorFlow can't initialize:
    //   failed to create cublas handle: CUBLAS_STATUS_NOT_INITIALIZED
    LOG(INFO) << "set_allow_growth(true)";
    gpu_opt->set_allow_growth(true);
  }
#else
  auto& tf_option = cpu_option_;
  (*cpu_option_.config.mutable_device_count())["GPU"] = 0;
#endif

  // Init session and load model
  impl_->session.reset(tensorflow::NewSession(tf_option));
  tensorflow::GraphDef graph_def;
  tensorflow::Status status;
  status = tensorflow::ReadBinaryProto(tf_option.env, pb_path, &graph_def);
  if (!status.ok()) {
    LOG(FATAL) << "Failed to load model " << pb_path << " : "
               << status.ToString();
  }
  status = impl_->session->Create(graph_def);
  if (!status.ok()) {
    LOG(FATAL) << "Failed to add graph to session: " << status.ToString();
  }

  // Get the GPU allocator for creating input buffer
#ifdef USE_GPU
  impl_->tf_allocator =
      tensorflow::GPUProcessState::singleton()->GetGPUAllocator(
          impl_->gpu_option.config.gpu_options(), tensorflow::TfGpuId(0), 0);
#else
  tf_allocator_ =
      tf::ProcessState::singleton()->GetCPUAllocator(tf::port::kNUMANoAffinity);
#endif
}

Session::~Session() {
  auto status = impl_->session->Close();
  if (!status.ok()) {
    LOG(FATAL) << "Failed to close Tensorflow Session: " << status.ToString();
  }
}

Tensor Session::NewTensor(DataType dtype, const std::vector<size_t>& shape) {
  tensorflow::DataType tf_dtype;
  switch (dtype) {
    case DataType::DT_UINT8:
      tf_dtype = tensorflow::DT_UINT8;
      break;
    case DataType::DT_FLOAT:
      tf_dtype = tensorflow::DT_FLOAT;
      break;
    default:
      LOG(FATAL) << "Unknown dtype " << static_cast<int>(dtype);
  }
  tensorflow::TensorShape tf_shape;
  for (auto dim : shape) {
    tf_shape.AddDim(dim);
  }
  tensorflow::Tensor tf_tensor(impl_->tf_allocator, tf_dtype, tf_shape);
  return TensorProxy::CopyFromTensorFlow(tf_tensor);
}

uint64_t Session::GetPeakBytesInUse() {
  auto stats = impl_->tf_allocator->GetStats();
  CHECK(stats.has_value());
  return stats->peak_bytes_in_use;
}

uint64_t Session::GetBytesInUse() {
  auto stats = impl_->tf_allocator->GetStats();
  CHECK(stats.has_value());
  return stats->bytes_in_use;
}

std::vector<Tensor> Session::Run(
    const std::vector<std::pair<std::string, Tensor>>& inputs,
    const std::vector<std::string>& output_tensor_names) {
  std::vector<std::pair<std::string, tensorflow::Tensor>> tf_inputs;
  tf_inputs.reserve(inputs.size());
  for (const auto& pair : inputs) {
    tf_inputs.emplace_back(pair.first, TensorProxy::AsTensorFlow(pair.second));
  }

  std::vector<tensorflow::Tensor> tf_outputs;
  auto status =
      impl_->session->Run(tf_inputs, output_tensor_names, {}, &tf_outputs);
  if (!status.ok()) {
    LOG(FATAL) << "Failed to run TensorFlow Session: " << status.ToString();
  }

  std::vector<Tensor> outputs;
  outputs.reserve(tf_outputs.size());
  for (auto tensor : tf_outputs) {
    outputs.emplace_back(TensorProxy::CopyFromTensorFlow(tensor));
  }

  return outputs;
}

}  // namespace tf
}  // namespace backend
}  // namespace nexus
