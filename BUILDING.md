# Building NexusLB on Ubuntu 20.04

## Install system-wide packages

```bash
# Build system and utilities
sudo apt-get install -y build-essential git curl wget

# NexusLB dependency
# Note: OpencCV  >= 4   (Ubuntu 18.04 ships with 3)
#       yaml-cpp >= 0.6 (Ubuntu 18.04 ships with 0.5)
sudo apt-get install -y \
    libboost-filesystem-dev libboost-system-dev \
    libgoogle-glog-dev libgflags-dev libgtest-dev \
    libprotobuf-dev protobuf-compiler \
    libopencv-dev \
    libyaml-cpp-dev \
    cmake

# NexusLB python dependency
sudo apt-get install -y python3 python3-dev python3-pip python3-numpy python3-yaml
```

## Install NVIDIA driver

```bash
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:graphics-drivers/ppa
sudo apt-get update
sudo apt-get install -y nvidia-headless-495 nvidia-utils-495
```

## Install Mellanox OFED driver

```bash
wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-2.2.4.0/MLNX_OFED_LINUX-4.9-2.2.4.0-ubuntu20.04-x86_64.tgz
tar xf MLNX_OFED_LINUX-4.9-2.2.4.0-ubuntu20.04-x86_64.tgz
cd MLNX_OFED_LINUX-4.9-2.2.4.0-ubuntu20.04-x86_64
sudo ./mlnxofedinstall --without-fw-update --all
```

## Install CUDA 11.2

```bash
wget https://developer.download.nvidia.com/compute/cuda/11.2.0/local_installers/cuda_11.2.0_460.27.04_linux.run
sudo sh cuda_11.2.0_460.27.04_linux.run --silent --toolkit
sudo unlink /usr/local/cuda
```

## Install cuDNN 8.1

```bash
wget https://developer.download.nvidia.com/compute/redist/cudnn/v8.1.0/cudnn-11.2-linux-x64-v8.1.0.77.tgz
tar xf cudnn-11.2-linux-x64-v8.1.0.77.tgz
sudo mv cuda/include/cudnn.h /usr/local/cuda-11.2/include
sudo mv cuda/lib64/libcudnn* /usr/local/cuda-11.2/lib64
sudo chmod a+r /usr/local/cuda-11.2/include/cudnn.h /usr/local/cuda-11.2/lib64/libcudnn*
sudo ldconfig
```

## Clone NexusLB

```bash
git clone https://github.com/abcdabcd987/nexuslb.git
```

## Download TensorFlow Wrapper

```bash
cd nexuslb
mkdir -p tensorflow/lib
cd tensorflow/lib
wget https://github.com/abcdabcd987/nexuslb/releases/download/libtensorflow_wrapper.so.2.10.0/libtensorflow_wrapper.so.2.10.0-cuda11.2-cudnn8
ln -sf libtensorflow_wrapper.so.2.10.0-cuda11.2-cudnn8 libtensorflow_wrapper.so
cd ../..
```

Alternatively, you can build TensorFlow Wrapper from scratch:

```bash
cd tensorflow
make gpu
```

## Build NexusLB

```bash
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebugInfo -DCUDA_PATH=/usr/local/cuda-11.2 -DUSE_TENSORFLOW=ON -DUSE_GPU=ON
make -j$(nproc)
cd ..
python3 -m pip install --user --editable ./python
```
