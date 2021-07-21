#include "nexus/backend/tensorflow_model.h"

#include <glog/logging.h>

#include <boost/filesystem.hpp>
#include <cstdint>
#include <memory>
#include <opencv2/opencv.hpp>
#include <random>

#include "nexus/backend/slice.h"
#include "nexus/backend/utils.h"
#include "nexus/common/image.h"

namespace fs = boost::filesystem;

namespace {

template <typename T>
void FillJunk(T* data, size_t len) {
  std::mt19937 gen(reinterpret_cast<uint64_t>(data));
  std::uniform_int_distribution<uint8_t> dist;
  for (size_t i = 0; i < len; ++i) {
    data[i] = static_cast<T>(dist(gen));
  }
}

}  // namespace

namespace nexus {
namespace backend {

TensorflowModel::TensorflowModel(int gpu_id, const ModelInstanceConfig& config,
                                 ModelIndex model_index)
    : ModelInstance(gpu_id, config, model_index), first_input_array_(true) {
  CHECK(model_info_["model_file"]) << "Missing model_file in the model info";

  double per_process_gpu_memory_fraction;
  // Init session options
#ifdef USE_GPU
  auto visible_device_list = std::to_string(gpu_id);
  LOG(INFO) << "model memory usage: " << config.memory_usage() << " B";
  if (config.memory_usage() > 0) {
    double memory_usage = config.memory_usage();
    per_process_gpu_memory_fraction = memory_usage / gpu_device_->TotalMemory();
  } else {
    per_process_gpu_memory_fraction = 0.0;
  }
#else
  per_process_gpu_memory_fraction = 0.0;
  std::string visible_device_list = "";
#endif

  // Init session and load model
  fs::path model_dir = fs::path(model_info_["model_dir"].as<std::string>());
  fs::path model_file = model_dir / model_info_["model_file"].as<std::string>();
  CHECK(fs::exists(model_file))
      << "model file " << model_file << " doesn't exist";
  const auto& pb_path = model_file.string();
  session_ = std::make_unique<tf::Session>(
      visible_device_list, per_process_gpu_memory_fraction, pb_path);

  // Get the input and output shape
  if (model_session_.image_height() > 0) {
    image_height_ = model_session_.image_height();
    image_width_ = model_session_.image_width();
  } else {
    image_height_ = model_info_["image_height"].as<int>();
    image_width_ = model_info_["image_width"].as<int>();
  }
  // Tensorflow uses NHWC by default. More details see
  // https://www.tensorflow.org/versions/master/performance/performance_guide
  input_shape_.set_dims(
      {static_cast<int>(max_batch_), image_height_, image_width_, 3});
  input_size_ = input_shape_.NumElements(1);
  input_layer_ = model_info_["input_layer"].as<std::string>();

  if (model_info_["output_layer"].IsSequence()) {
    for (size_t i = 0; i < model_info_["output_layer"].size(); ++i) {
      output_layers_.push_back(
          model_info_["output_layer"][i].as<std::string>());
    }
  } else {
    output_layers_.push_back(model_info_["output_layer"].as<std::string>());
  }
  LOG(INFO) << "Model " << model_session_id_ << ", input: " << input_layer_
            << ", shape: " << input_shape_ << " (" << input_size_ << ")";

  if (model_name() == "ssd_mobilenet" || model_name() == "ssd_mobilenet_0.75") {
    input_data_type_ = DT_UINT8;
  } else {
    input_data_type_ = DT_FLOAT;
  }

  // Dry run the model to get the outpue size
  auto in_tensor = NewInputTensor().Slice(0, 1);
  auto out_tensors = WarmupInputTensor(in_tensor);
  for (uint i = 0; i < out_tensors.size(); ++i) {
    std::vector<int> dims;
    for (auto dim : out_tensors[i].shape()) {
      dims.push_back(dim);
    }
    if (dims.size() == 1) {
      dims.push_back(1);
    }
    Shape shape(dims);
    size_t out_size = shape.NumElements(1);
    output_shapes_.emplace(output_layers_[i], shape);
    output_sizes_.emplace(output_layers_[i], out_size);
    LOG(INFO) << "Output " << output_layers_[i] << ", shape: " << shape << " ("
              << out_size << ")";
  }

  // Load preprocessing configs
  if (model_info_["input_mean"]) {
    CHECK_EQ(model_info_["input_mean"].size(), 3) << "input_mean must have "
                                                  << "3 values";
    for (uint i = 0; i < model_info_["input_mean"].size(); ++i) {
      input_mean_.push_back(model_info_["input_mean"][i].as<float>());
    }
  }
  if (model_info_["input_std"]) {
    CHECK_EQ(model_info_["input_std"].size(), 3) << "input_std must have "
                                                 << "3 values";
    for (uint i = 0; i < model_info_["input_std"].size(); ++i) {
      input_std_.push_back(model_info_["input_std"][i].as<float>());
    }
  }

  // Load class names
  if (model_info_["class_names"]) {
    fs::path cns_path =
        model_dir / model_info_["class_names"].as<std::string>();
    LoadClassnames(cns_path.string(), &classnames_);
  }
}

void TensorflowModel::WarmupInputArray(std::shared_ptr<Array> input_array) {
  const auto& in_tensor = input_tensors_[input_array->tag()];

  std::vector<char> buf;
  size_t size =
      input_array->num_elements() * type_size(input_array->data_type());
  buf.resize(size);
  switch (input_array->data_type()) {
    case DataType::DT_UINT8:
      FillJunk(static_cast<char*>(buf.data()), input_array->num_elements());
      break;
    case DataType::DT_FLOAT:
      FillJunk(reinterpret_cast<float*>(buf.data()),
               input_array->num_elements());
      break;
    default:
      LOG(FATAL) << "Unknown data type: "
                 << DataType_Name(input_array->data_type());
  }
  Memcpy(input_array->Data<char>(), input_array->device(), buf.data(),
         cpu_device_, size);

  (void)WarmupInputTensor(in_tensor);
}

std::vector<tf::Tensor> TensorflowModel::WarmupInputTensor(
    tf::Tensor in_tensor) {
  std::vector<std::pair<std::string, tf::Tensor>> inputs;
  inputs.emplace_back(input_layer_, in_tensor);
  return session_->Run(inputs, output_layers_);
}

TensorflowModel::~TensorflowModel() {}

Shape TensorflowModel::InputShape() { return input_shape_; }

std::unordered_map<std::string, Shape> TensorflowModel::OutputShapes() {
  return output_shapes_;
}

ArrayPtr TensorflowModel::CreateInputGpuArray() {
  tf::Tensor tensor;
  if (first_input_array_) {
    tensor = input_tensors_[0];
    first_input_array_ = false;
  } else {
    tensor = NewInputTensor();
  }
  void* gpu_data = tensor.data();
  size_t nbytes = tensor.NumElements() * type_size(input_data_type_);
#ifdef USE_GPU
  auto& device = gpu_device_;
#else
  auto& device = cpu_device_;
#endif
  auto buf = std::make_shared<Buffer>(gpu_data, nbytes, device);
  auto arr =
      std::make_shared<Array>(input_data_type_, tensor.NumElements(), buf);
  arr->set_tag(input_tensors_.size() - 1);
  return arr;
}

std::unordered_map<std::string, ArrayPtr>
TensorflowModel::GetOutputGpuArrays() {
  // Because TF always returns output in CPU memory, doesn't support in-place
  // output in GPU memory
  return {};
}

void TensorflowModel::Preprocess(std::shared_ptr<Task> task) {
  // Tensorflow uses NHWC by default. More details see
  // https://www.tensorflow.org/versions/master/performance/performance_guide

  auto prepare_image_default = [&](cv::Mat& image) {
    // Convert to image in float
    cv::Mat fimg;
    image.convertTo(fimg, CV_32FC3);
    // create a cv::Mat using buffer allocated in the in_arr
    auto in_arr = std::make_shared<Array>(DT_FLOAT, input_size_, cpu_device_);
    cv::Mat resized(image_height_, image_width_, CV_32FC3,
                    in_arr->Data<void>());
    cv::resize(fimg, resized, cv::Size(image_width_, image_height_));
    task->AppendInput(in_arr);
  };

  auto prepare_image_ssd = [&](cv::Mat& image) {
    auto in_arr = std::make_shared<Array>(DT_UINT8, input_size_, cpu_device_);
    // create a cv::Mat using buffer allocated in the in_arr
    cv::Mat resized(image_width_, image_height_, CV_8UC3, in_arr->Data<void>());
    cv::resize(image, resized, cv::Size(image_width_, image_height_));
    task->AppendInput(in_arr);
  };

  std::function<void(cv::Mat&)> prepare_image;
  if (model_name() == "ssd_mobilenet" || model_name() == "ssd_mobilenet_0.75") {
    prepare_image = prepare_image_ssd;
  } else {
    prepare_image = prepare_image_default;
  }

  const auto& query = task->query;
  const auto& input_data = query.input();
  switch (input_data.data_type()) {
    case DT_IMAGE: {
      cv::Mat img = DecodeImage(input_data.image(), CO_RGB);
      task->attrs["im_height"] = img.rows;
      task->attrs["im_width"] = img.cols;
      if (query.window_size() > 0) {
        for (int i = 0; i < query.window_size(); ++i) {
          const auto& rect = query.window(i);
          cv::Mat crop_img =
              img(cv::Rect(rect.left(), rect.top(), rect.right() - rect.left(),
                           rect.bottom() - rect.top()));
          prepare_image(crop_img);
        }
      } else {
        prepare_image(img);
      }
      break;
    }
    default:
      task->result.set_status(INPUT_TYPE_INCORRECT);
      task->result.set_error_message("Input type incorrect: " +
                                     DataType_Name(input_data.data_type()));
      break;
  }
}

void TensorflowModel::Forward(std::shared_ptr<BatchTask> batch_task) {
  size_t batch_size = batch_task->batch_size();
  auto in_tensor =
      input_tensors_[batch_task->GetInputArray()->tag()].Slice(0, batch_size);
  auto out_tensors = session_->Run({{input_layer_, in_tensor}}, output_layers_);
  std::unordered_map<std::string, Slice> slices;
  for (uint i = 0; i < output_layers_.size(); ++i) {
    const auto& name = output_layers_[i];
    const void* tensor_data = out_tensors[i].data();
    size_t nfloats = out_tensors[i].NumElements();
    auto out_arr = batch_task->GetOutputArray(name);
    float* out_data = out_arr->Data<float>();
    Memcpy(out_data, cpu_device_, tensor_data, cpu_device_,
           nfloats * sizeof(float));
    slices.emplace(name, Slice(batch_size, output_sizes_.at(name)));
  }
  batch_task->SliceOutputBatch(slices);
}

void TensorflowModel::Postprocess(std::shared_ptr<Task> task) {
  const QueryProto& query = task->query;
  QueryResultProto* result = &task->result;
  result->set_status(CTRL_OK);
  for (auto output : task->outputs) {
    if (type() == "classification") {
      auto out_arr = output->arrays.at(output_layers_[0]);
      float* out_data = out_arr->Data<float>();
      size_t count = out_arr->num_elements();
      size_t output_size = output_sizes_.at(output_layers_[0]);
      if (classnames_.empty()) {
        PostprocessClassification(query, out_data, output_size, result);
      } else {
        PostprocessClassification(query, out_data, output_size, result,
                                  &classnames_);
      }
    } else if (type() == "detection") {
      int im_height = task->attrs["im_height"].as<int>();
      int im_width = task->attrs["im_width"].as<int>();
      MarshalDetectionResult(query, output, im_height, im_width, result);
    } else {
      std::ostringstream oss;
      oss << "Unsupported model type " << type() << " for " << framework();
      result->set_status(MODEL_TYPE_NOT_SUPPORT);
      result->set_error_message(oss.str());
      break;
    }
  }
}

uint64_t TensorflowModel::GetPeakBytesInUse() {
  return session_->GetPeakBytesInUse();
}

uint64_t TensorflowModel::GetBytesInUse() { return session_->GetBytesInUse(); }

tf::Tensor TensorflowModel::NewInputTensor() {
  std::vector<size_t> shape;
  for (auto dim : input_shape_.dims()) {
    shape.push_back(dim);
  }
  tf::DataType dtype;
  switch (input_data_type_) {
    case DT_UINT8:
      dtype = tf::DataType::DT_UINT8;
      break;
    case DT_FLOAT:
      dtype = tf::DataType::DT_FLOAT;
      break;
    default:
      LOG(FATAL) << "Unknown dtype " << static_cast<int>(input_data_type_);
  }
  input_tensors_.emplace_back(session_->NewTensor(dtype, shape));
  return input_tensors_.back();
}

void TensorflowModel::MarshalDetectionResult(const QueryProto& query,
                                             std::shared_ptr<Output> output,
                                             int im_height, int im_width,
                                             QueryResultProto* result) {
  int num_boxes =
      static_cast<int>(output->arrays.at("num_detections")->Data<float>()[0]);
  float* boxes = output->arrays.at("detection_boxes")->Data<float>();
  float* scores = output->arrays.at("detection_scores")->Data<float>();
  float* classes = output->arrays.at("detection_classes")->Data<float>();

  std::vector<std::string> output_fields(query.output_field().begin(),
                                         query.output_field().end());
  if (output_fields.size() == 0) {
    output_fields.push_back("rect");
    output_fields.push_back("class_name");
  }
  for (int i = 0; i < num_boxes; ++i) {
    auto record = result->add_output();
    if (FLAGS_hack_reply_omit_output) continue;
    int class_id = static_cast<int>(classes[i]);
    for (auto field : output_fields) {
      if (field == "rect") {
        auto value = record->add_named_value();
        value->set_name("rect");
        value->set_data_type(DT_RECT);
        auto rect = value->mutable_rect();
        rect->set_top(int(im_height * boxes[i * 4]));
        rect->set_left(int(im_width * boxes[i * 4 + 1]));
        rect->set_bottom(int(im_height * boxes[i * 4 + 2]));
        rect->set_right(int(im_width * boxes[i * 4 + 3]));
      } else if (field == "score") {
        auto value = record->add_named_value();
        value->set_name("score");
        value->set_data_type(DT_FLOAT);
        value->set_f(scores[i]);
      } else if (field == "class_id") {
        auto value = record->add_named_value();
        value->set_name("class_id");
        value->set_data_type(DT_INT32);
        value->set_i(class_id);
      } else if (field == "class_name") {
        auto value = record->add_named_value();
        value->set_name("class_name");
        value->set_data_type(DT_STRING);
        auto iter = classnames_.find(class_id);
        if (iter == classnames_.end()) {
          LOG(ERROR) << "Cannot find class name for class id " << class_id;
        } else {
          value->set_s(iter->second);
        }
      }
    }
  }
}

}  // namespace backend
}  // namespace nexus
