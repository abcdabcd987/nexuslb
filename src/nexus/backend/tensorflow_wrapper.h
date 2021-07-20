#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace nexus {
namespace backend {
namespace tf {

enum class DataType {
  DT_INVALID = 0,
  DT_FLOAT = 1,
  DT_UINT8 = 4,
};

class Tensor {
 public:
  Tensor();
  ~Tensor();
  Tensor(const Tensor& other);
  Tensor& operator=(const Tensor& other);
  Tensor(Tensor&& other);
  Tensor& operator=(Tensor&& other);

  std::vector<size_t> shape() const;
  void* data() const;
  int64_t NumElements() const;
  Tensor Slice(int64_t dim0_start, int64_t dim0_limit) const;

 private:
  friend class TensorProxy;
  using Impl = std::array<uint8_t, 32>;
  Impl impl_;
};

class Session {
 public:
  Session(const std::string& visible_device_list,
          double per_process_gpu_memory_fraction, const std::string& pb_path);
  ~Session();

  Tensor NewTensor(DataType dtype, const std::vector<size_t>& shape);
  uint64_t GetPeakBytesInUse();
  uint64_t GetBytesInUse();
  std::vector<Tensor> Run(
      const std::vector<std::pair<std::string, Tensor>>& inputs,
      const std::vector<std::string>& output_tensor_names);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tf
}  // namespace backend
}  // namespace nexus
