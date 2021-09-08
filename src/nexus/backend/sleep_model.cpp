#include "nexus/backend/sleep_model.h"

#include <glog/logging.h>
#include <immintrin.h>

#include <chrono>
#include <stdexcept>
#include <thread>

#include "nexus/backend/model_ins.h"
#include "nexus/common/time_util.h"

namespace nexus {
namespace backend {

namespace {

template <class Rep, class Period>
inline void BlockSleepFor(const std::chrono::duration<Rep, Period>& duration) {
  std::this_thread::sleep_for(duration);
}

template <class Rep, class Period>
inline void SpinSleepFor(const std::chrono::duration<Rep, Period>& duration) {
  auto until = Clock::now() + duration;
  while (Clock::now() < until) {
    _mm_pause();
  }
}

template <class Rep, class Period>
inline void SleepFor(const std::chrono::duration<Rep, Period>& duration) {
  SpinSleepFor(duration);
}

}  // namespace

SleepModel::SleepModel(SleepProfile profile, const ModelInstanceConfig& config,
                       ModelIndex model_index)
    : ModelInstance(-1, config, model_index), profile_(std::move(profile)) {
  LOG(INFO) << "Construct SleepModel."
            << "slope_us=" << profile.slope_us()
            << ", intercept_us=" << profile.intercept_us()
            << ", preprocess_us=" << profile.preprocess_us()
            << ", postprocess_us=" << profile.postprocess_us();
  if (model_session_.image_height() > 0) {
    image_height_ = model_session_.image_height();
    image_width_ = model_session_.image_width();
  } else {
    image_height_ = model_info_["image_height"].as<int>();
    image_width_ = model_info_["image_width"].as<int>();
  }
  max_batch_ = 500;

  input_shape_.set_dims(
      {static_cast<int>(max_batch_), image_height_, image_width_, 3});
  input_size_ = input_shape_.NumElements(1);
  input_layer_ = model_info_["input_layer"].as<std::string>();
  input_data_type_ = DT_FLOAT;

  if (model_info_["output_layer"].IsSequence()) {
    for (size_t i = 0; i < model_info_["output_layer"].size(); ++i) {
      output_layers_.push_back(
          model_info_["output_layer"][i].as<std::string>());
    }
  } else {
    output_layers_.push_back(model_info_["output_layer"].as<std::string>());
  }

  for (const auto& name : output_layers_) {
    // Dummy values
    int n = 10;
    output_sizes_[name] = n;
    output_shapes_[name] = Shape{1, n};
  }
}

Shape SleepModel::InputShape() { return input_shape_; }

std::unordered_map<std::string, Shape> SleepModel::OutputShapes() {
  return output_shapes_;
}

ArrayPtr SleepModel::CreateInputGpuArray() {
  auto num_elements = max_batch_ * image_height_ * image_width_ * 3;
  auto nbytes = num_elements * type_size(input_data_type_);
  auto buf = std::make_shared<Buffer>(nbytes, cpu_device_);
  auto arr =
      std::make_shared<Array>(input_data_type_, num_elements, cpu_device_);
  return arr;
}

void SleepModel::WarmupInputArray(std::shared_ptr<Array> input_array) {
  char* data = input_array->Data<char>();
  size_t size =
      type_size(input_array->data_type()) * input_array->num_elements();
  for (size_t i = 0; i < size; ++i) {
    data[i] = i & 0xFF;
  }
}

std::unordered_map<std::string, ArrayPtr> SleepModel::GetOutputGpuArrays() {
  return {};
}

void SleepModel::Preprocess(std::shared_ptr<Task> task) {
  auto prepare_image = [&, this] {
    auto in_arr = std::make_shared<Array>(DT_FLOAT, input_size_, cpu_device_);
    task->AppendInput(in_arr);
  };

  const auto& query = task->query;
  const auto& input_data = query.input();
  switch (input_data.data_type()) {
    case DT_IMAGE: {
      task->attrs["im_height"] = image_height_;
      task->attrs["im_width"] = image_width_;
      if (query.window_size() > 0) {
        for (int i = 0; i < query.window_size(); ++i) {
          const auto& rect = query.window(i);
          prepare_image();
        }
      } else {
        prepare_image();
      }
      break;
    }
    default:
      task->result.set_status(INPUT_TYPE_INCORRECT);
      task->result.set_error_message("Input type incorrect: " +
                                     DataType_Name(input_data.data_type()));
      break;
  }

  SleepFor(std::chrono::microseconds(profile_.preprocess_us()));
}

void SleepModel::Forward(std::shared_ptr<BatchTask> batch_task) {
  size_t batch_size = batch_task->batch_size();
  SleepFor(std::chrono::microseconds(profile_.forward_us(batch_size)));
  std::unordered_map<std::string, Slice> slices;
  for (uint i = 0; i < output_layers_.size(); ++i) {
    const auto& name = output_layers_[i];
    slices.emplace(name, Slice(batch_size, output_sizes_.at(name)));
  }
  batch_task->SliceOutputBatch(slices);
}

void SleepModel::Postprocess(std::shared_ptr<Task> task) {
  const QueryProto& query = task->query;
  QueryResultProto* result = &task->result;
  result->set_status(CTRL_OK);
  for (auto output : task->outputs) {
    auto out_arr = output->arrays.at(output_layers_[0]);
    size_t count = out_arr->num_elements();
    size_t output_size = output_sizes_.at(output_layers_[0]);
    auto* record = result->add_output();
  }

  SleepFor(std::chrono::microseconds(profile_.postprocess_us()));
}

uint64_t SleepModel::GetPeakBytesInUse() {
  throw std::runtime_error("SleepModel::GetPeakBytesInUse NotSupported");
}

uint64_t SleepModel::GetBytesInUse() {
  throw std::runtime_error("SleepModel::GetBytesInUse NotSupported");
}

}  // namespace backend
}  // namespace nexus
