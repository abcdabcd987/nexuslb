#include "nexus/common/device.h"

#include <glog/logging.h>

#include <fstream>
#include <string>

namespace nexus {

DEFINE_bool(generic_profile, false,
            "Use the generic profile for all GPUs of the same model instead of "
            "using profiles for each GPU card. (Applicable to Backend only)");

CPUDevice::CPUDevice() : Device(kCPU) {
  std::ifstream fin("/proc/cpuinfo");
  std::string line;
  cpu_model_ = "GenericCPU";
  while (std::getline(fin, line)) {
    if (line.rfind("model name", 0) == 0) {
      auto pos = line.find(": ");
      if (pos != std::string::npos) {
        cpu_model_ = line.substr(pos + 2);
        break;
      }
    }
  }
}

void *CPUDevice::Allocate(size_t nbytes) { return malloc(nbytes); }

void CPUDevice::Free(void *buf) { free(buf); }

#ifdef USE_GPU

GPUDevice::GPUDevice(int gpu_id) : Device(kGPU), gpu_id_(gpu_id) {
  std::stringstream ss;
  ss << "gpu:" << gpu_id;
  name_ = ss.str();
  cudaDeviceProp prop;
  NEXUS_CUDA_CHECK(cudaSetDevice(gpu_id_));
  NEXUS_CUDA_CHECK(cudaGetDeviceProperties(&prop, gpu_id_));
  device_name_.assign(prop.name, strlen(prop.name));
  std::replace(device_name_.begin(), device_name_.end(), ' ', '_');
  total_memory_ = prop.totalGlobalMem;

  if (FLAGS_generic_profile) {
    uuid_ = "generic";
  } else {
    auto *u = reinterpret_cast<unsigned char *>(&prop.uuid);
    char uuid_hex[37] = {};
    sprintf(
        uuid_hex,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10],
        u[11], u[12], u[13], u[14], u[15]);
    uuid_ = uuid_hex;
  }

  NEXUS_CUDA_CHECK(cudaStreamCreate(&h2d_stream_));

  LOG(INFO) << "GPU " << gpu_id << " " << device_name_ << "(" << uuid_ << ")"
            << ": total memory " << total_memory_ / 1024. / 1024. / 1024.
            << "GB";
}

GPUDevice::~GPUDevice() { NEXUS_CUDA_CHECK(cudaStreamDestroy(h2d_stream_)); }

void GPUDevice::SyncHostToDevice() {
  NEXUS_CUDA_CHECK(cudaStreamSynchronize(h2d_stream_));
}

void GPUDevice::AsyncMemcpyHostToDevice(void *dst, const void *src,
                                        size_t nbytes) {
  NEXUS_CUDA_CHECK(
      cudaMemcpyAsync(dst, src, nbytes, cudaMemcpyHostToDevice, h2d_stream_));
}

void *GPUDevice::Allocate(size_t nbytes) {
  void *buf;
  NEXUS_CUDA_CHECK(cudaSetDevice(gpu_id_));
  cudaError_t err = cudaMalloc(&buf, nbytes);
  if (err != cudaSuccess) {
    throw cudaGetErrorString(err);
  }
  return buf;
}

size_t GPUDevice::FreeMemory() const {
  size_t free_mem, total_mem;
  NEXUS_CUDA_CHECK(cudaSetDevice(gpu_id_));
  NEXUS_CUDA_CHECK(cudaMemGetInfo(&free_mem, &total_mem));
  return free_mem;
}

void GPUDevice::Free(void *buf) { NEXUS_CUDA_CHECK(cudaFree(buf)); }

GPUDevice *DeviceManager::GetGPUDevice(int gpu_id) const {
  CHECK_LT(gpu_id, gpu_devices_.size())
      << "GPU id " << gpu_id << " exceeds number of GPU devices ("
      << gpu_devices_.size() << ")";
  return gpu_devices_[gpu_id];
}
#endif

DeviceManager::DeviceManager() {
  cpu_device_ = new CPUDevice();
  int gpu_count;
#ifdef USE_GPU
  NEXUS_CUDA_CHECK(cudaGetDeviceCount(&gpu_count));
  for (int i = 0; i < gpu_count; ++i) {
    gpu_devices_.push_back(new GPUDevice(i));
  }
#endif
}
}  // namespace nexus
