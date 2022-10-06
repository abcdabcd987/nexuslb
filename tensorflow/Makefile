# Default value from: https://www.tensorflow.org/install/source#gpu
TF_CUDA_VERSION ?= 11.2
TF_CUDNN_VERSION ?= 8
TF_CUDA_COMPUTE_CAPABILITIES ?= sm_35,sm_50,sm_60,sm_70,sm_75,compute_80



TF_VERSION != grep -Po '(?<="tensorflow-).*(?=")' WORKSPACE
CPU_SO := libtensorflow_wrapper.so.$(TF_VERSION)-cpu
GPU_SO := libtensorflow_wrapper.so.$(TF_VERSION)-cuda$(TF_CUDA_VERSION)-cudnn$(TF_CUDNN_VERSION)

DOCKER_IMAGE := gcr.io/tensorflow-testing/nosla-cuda11.2-cudnn8.1-ubuntu20.04-manylinux2014-multipython
DOCKER_NAME  := tensorflow_wrapper_builder
DOCKER_START := docker run --rm -d --name $(DOCKER_NAME) -v $(shell pwd):/build --init $(DOCKER_IMAGE) sleep inf
DOCKER_EXEC  := docker exec -t -w /build $(DOCKER_NAME)
DOCKER_CLEAN := $(DOCKER_EXEC) rm -rf bazel-bin bazel-out bazel-testlogs bazel-tensorflow bazel-build
DOCKER_STOP  := docker stop $(DOCKER_NAME)
BAZEL_BUILD   = bazel build --color=yes --curses=yes \
		--action_env=PYTHON_BIN_PATH=/usr/local/bin/python3.8 \
		--crosstool_top=@ubuntu20.04-gcc9_manylinux2014-cuda11.2-cudnn8.1-tensorrt7.2_config_cuda//crosstool:toolchain
BAZEL_TARGET = //:libtensorflow_wrapper.so

.NOTPARALLEL:
.DEFAULT_GOAL: prompt
.PHONY: prompt cpu gpu clean
prompt:
	@echo Please \`make gpu\` or \`make cpu\`

gpu: lib/$(GPU_SO)
	ln -sf $(GPU_SO) lib/libtensorflow_wrapper.so
lib/$(GPU_SO):
	$(DOCKER_START)
	$(DOCKER_EXEC) $(BAZEL_BUILD) \
		--config=cuda \
		--action_env TF_CUDA_VERSION=$(TF_CUDA_VERSION) \
		--action_env TF_CUDNN_VERSION=$(TF_CUDNN_VERSION) \
		--action_env TF_CUDA_COMPUTE_CAPABILITIES=$(TF_CUDA_COMPUTE_CAPABILITIES) \
		$(BAZEL_TARGET)
	docker cp $(DOCKER_NAME):/build/bazel-bin/libtensorflow_wrapper.so lib/$(GPU_SO)
	$(DOCKER_CLEAN)
	$(DOCKER_STOP)


cpu: lib/$(CPU_SO)
	ln -sf $(CPU_SO) lib/libtensorflow_wrapper.so
lib/$(CPU_SO):
	$(DOCKER_START)
	$(DOCKER_EXEC) $(BAZEL_BUILD) $(BAZEL_TARGET)
	docker cp $(DOCKER_NAME):/build/bazel-bin/libtensorflow_wrapper.so lib/$(CPU_SO)
	$(DOCKER_CLEAN)
	$(DOCKER_STOP)

clean:
	rm -f bazel-bin/libtensorflow_wrapper.so
	rm -f lib/libtensorflow_wrapper.so
	rm -f lib/$(GPU_SO)
	rm -f lib/$(CPU_SO)
	rm -f bazel-out bazel-out bazel-testlogs bazel-tensorflow
