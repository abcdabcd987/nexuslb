cmake_minimum_required(VERSION 3.12)
project(nexus LANGUAGES CXX)
list(INSERT CMAKE_MODULE_PATH 0 ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(NEXUS_DEP_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build-dep-install)
set(LIBTF_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libtensorflow_cc)
list(INSERT CMAKE_PREFIX_PATH 0 ${NEXUS_DEP_DIR}/boost)
list(INSERT CMAKE_PREFIX_PATH 0 ${NEXUS_DEP_DIR}/gflags)
list(INSERT CMAKE_PREFIX_PATH 0 ${NEXUS_DEP_DIR}/glog)
list(INSERT CMAKE_PREFIX_PATH 0 ${NEXUS_DEP_DIR}/gtest)
list(INSERT CMAKE_PREFIX_PATH 0 ${NEXUS_DEP_DIR}/opencv)
list(INSERT CMAKE_PREFIX_PATH 0 ${NEXUS_DEP_DIR}/yaml-cpp)
list(INSERT CMAKE_PREFIX_PATH 0 ${LIBTF_DIR}/protobuf)

# We don't support caffe2/caffe/darknet any more
option(USE_GPU        "Use GPU"        ON )
option(USE_TENSORFLOW "Use TensorFlow" ON )
set(USE_DARKNET OFF)
set(USE_CAFFE2 OFF)
set(USE_CAFFE OFF)
if(USE_CAFFE2 AND USE_CAFFE)
    message(FATAL_ERROR "`USE_CAFFE2` and `USE_CAFFE` cannot be set at the same time.")
endif()
set(CUDA_PATH "/usr/local/cuda" CACHE STRING "Path to the Cuda toolkit")

set(BOOST_ROOT /usr/local/boost)
find_package(Boost 1.69.0 REQUIRED COMPONENTS system filesystem)
find_package(Protobuf REQUIRED)
find_package(glog REQUIRED)
find_package(gflags REQUIRED COMPONENTS shared)
find_package(GTest REQUIRED)
find_package(yaml-cpp 0.6.2 REQUIRED)
find_package(OpenCV REQUIRED)
include(ProcessorCount)
ProcessorCount(NPROC)

find_package(Threads REQUIRED)



###### ario ######
add_library(ario
        src/ario/callback_queue.cpp
        src/ario/epoll.cpp
        src/ario/interrupter.cpp
        src/ario/memory.cpp
        src/ario/rdma.cpp
        src/ario/tcp.cpp
        src/ario/timer.cpp
        src/ario/timerfd.cpp
        src/ario/utils.cpp)
target_include_directories(ario PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(ario PUBLIC -std=c++17)
target_link_libraries(ario PRIVATE Threads::Threads)
target_link_libraries(ario PRIVATE ibverbs)
set_target_properties(ario PROPERTIES POSITION_INDEPENDENT_CODE ON)


###### protobuf ######
set(GENERATED_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GENERATED_SRC_DIR}/src/nexus/proto)
protobuf_generate_cpp(PROTO_NNQUERY_CC PROTO_NNQUERY_H
        ${GENERATED_SRC_DIR}/src/nexus/proto
        src/nexus/proto/nnquery.proto)
protobuf_generate_cpp(PROTO_CONTROL_CC PROTO_CONTROL_H
        ${GENERATED_SRC_DIR}/src/nexus/proto
        src/nexus/proto/control.proto)
set(GENERATED_PY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/python/nexus/proto)
add_custom_command(
    OUTPUT ${GENERATED_PY_DIR}/nnquery_pb2.py
    COMMAND mkdir -p ${GENERATED_PY_DIR}
    COMMAND touch ${GENERATED_PY_DIR}/__init__.py
    COMMAND protobuf::protoc
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/src/nexus/proto
        --python_out=${GENERATED_PY_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/src/nexus/proto/nnquery.proto
    DEPENDS
        protobuf::protoc
        ${CMAKE_CURRENT_SOURCE_DIR}/src/nexus/proto/nnquery.proto)
add_custom_target(python ALL DEPENDS ${GENERATED_PY_DIR}/nnquery_pb2.py)



###### common source files, headers, and linking options ######
add_library(common OBJECT
        ${PROTO_NNQUERY_CC}
        ${PROTO_NNQUERY_H}
        ${PROTO_CONTROL_CC}
        ${PROTO_CONTROL_H}
        src/nexus/common/backend_pool.cpp
        src/nexus/common/buffer.cpp
        src/nexus/common/connection.cpp
        src/nexus/common/data_type.cpp
        src/nexus/common/device.cpp
        src/nexus/common/image.cpp
        src/nexus/common/message.cpp
        src/nexus/common/metric.cpp
        src/nexus/common/model_db.cpp
        src/nexus/common/rdma_sender.cpp
        src/nexus/common/rps_meter.cpp
        src/nexus/common/server_base.cpp
        src/nexus/common/sleep_profile.cpp
        src/nexus/common/time_util.cpp
        src/nexus/common/util.cpp)
target_include_directories(common PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party
        ${GENERATED_SRC_DIR}/src)
target_compile_options(common PUBLIC -std=c++17)
target_link_libraries(common PUBLIC ario)
target_link_libraries(common PUBLIC
        yaml-cpp gflags glog::glog protobuf::libprotobuf
        ${OpenCV_LIBS} Boost::filesystem Boost::system)
set_target_properties(common PROPERTIES POSITION_INDEPENDENT_CODE ON)



###### scheduler ######
# add_executable(scheduler
#         src/nexus/scheduler/backend_delegate.cpp
#         src/nexus/scheduler/complex_query.cpp
#         src/nexus/scheduler/frontend_delegate.cpp
#         src/nexus/scheduler/sch_info.cpp
#         src/nexus/scheduler/scheduler.cpp
#         src/nexus/scheduler/scheduler_main.cpp)
# target_include_directories(scheduler PRIVATE
#         ${CMAKE_CURRENT_SOURCE_DIR}/src
#         ${GENERATED_SRC_DIR}/src)
# target_link_libraries(scheduler PRIVATE common)



###### dispatcher ######
add_library(dispatcher_obj
    src/nexus/dispatcher/backend_delegate.cpp
    src/nexus/dispatcher/backend_delegate_impl.cpp
    src/nexus/dispatcher/batch_policy.cpp
    src/nexus/dispatcher/batch_size_estimator.cpp
    src/nexus/dispatcher/dispatcher.cpp
    src/nexus/dispatcher/frontend_delegate_impl.cpp
    src/nexus/dispatcher/model_worker.cpp
    src/nexus/dispatcher/query_context.cpp
    src/nexus/dispatcher/rankmt/common.cpp
    src/nexus/dispatcher/rankmt/model_thread.cpp
    src/nexus/dispatcher/rankmt/rank_thread.cpp
    src/nexus/dispatcher/rankmt/scheduler.cpp
    src/nexus/dispatcher/session_context.cpp
)
target_link_libraries(dispatcher_obj PUBLIC common)

add_executable(dispatcher
        src/nexus/dispatcher/dispatcher_main.cpp)
target_link_libraries(dispatcher PRIVATE dispatcher_obj)



###### frontend libnexus.so ######
add_library(nexus SHARED
        src/nexus/app/app_base.cpp
        src/nexus/app/frontend.cpp
        src/nexus/app/model_handler.cpp
        src/nexus/app/request_context.cpp
        src/nexus/app/worker.cpp)
target_include_directories(nexus PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${GENERATED_SRC_DIR}/src)
target_link_libraries(nexus PUBLIC common)



###### backend ######
add_library(backend_obj
        src/nexus/backend/backend_server.cpp
        src/nexus/backend/batch_plan_context.cpp
        src/nexus/backend/batch_task.cpp
        src/nexus/backend/gpu_executor.cpp
        src/nexus/backend/model_exec.cpp
        src/nexus/backend/model_ins.cpp
        src/nexus/backend/sleep_model.cpp
        src/nexus/backend/slice.cpp
        src/nexus/backend/task.cpp
        src/nexus/backend/utils.cpp
        src/nexus/backend/worker.cpp)
target_link_libraries(backend_obj PUBLIC common)

add_executable(backend
        src/nexus/backend/backend_main.cpp)
target_link_libraries(backend PRIVATE backend_obj)

## backend: TensorFlow support ##
if(USE_TENSORFLOW)
    add_library(tensorflow_cc INTERFACE)
    target_include_directories(tensorflow_cc INTERFACE ${LIBTF_DIR}/tensorflow/include)
    target_include_directories(tensorflow_cc INTERFACE ${LIBTF_DIR}/protobuf/include)
    target_link_libraries(tensorflow_cc INTERFACE ${LIBTF_DIR}/tensorflow/lib/libtensorflow_cc.so)
    target_link_libraries(tensorflow_cc INTERFACE ${LIBTF_DIR}/tensorflow/lib/libtensorflow_framework.so)

    add_library(tensorflow_wrapper src/nexus/backend/tensorflow_wrapper.cpp)
    target_include_directories(tensorflow_wrapper PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(tensorflow_wrapper PUBLIC tensorflow_cc)

    # TensorFlow is currently built with C++14. Some TensorFlow APIs expose
    # pre-adopted std types with absl. To avoid weird ABI compatibility issues,
    # compile the wrapper with the same C++ standard as TensorFlow.
    target_compile_options(tensorflow_wrapper PRIVATE -std=c++14)

    target_compile_definitions(backend_obj PUBLIC USE_TENSORFLOW)
    target_sources(backend_obj PRIVATE
            src/nexus/backend/tensorflow_model.cpp)
    target_link_libraries(backend_obj PUBLIC tensorflow_wrapper)
endif()

## backend: Caffe2 support ##
if(USE_CAFFE2)
    target_compile_definitions(backend_obj PRIVATE USE_CAFFE2)
    target_sources(backend_obj PRIVATE src/nexus/backend/caffe2_model.cpp)
    set(CAFFE2_BUILD_ROOT ${CMAKE_CURRENT_BINARY_DIR}/caffe2)
    set(CAFFE2_SO
            ${CAFFE2_BUILD_ROOT}/install/lib/libcaffe2.so
            ${CAFFE2_BUILD_ROOT}/install/lib/libcaffe2_gpu.so)
    file(MAKE_DIRECTORY ${CAFFE2_BUILD_ROOT})
    add_custom_command(
        OUTPUT ${CAFFE2_SO}
        WORKING_DIRECTORY ${CAFFE2_BUILD_ROOT}
        COMMAND ${CMAKE_COMMAND} -DUSE_NNPACK=OFF -DUSE_NCCL=OFF -DUSE_MPI=OFF -DUSE_LMDB=OFF -DUSE_LEVELDB=OFF
            -DUSE_NUMA=OFF -DUSE_MPI=OFF -DBUILD_PYTHON=OFF -DBUILD_BINARY=OFF -DBUILD_TEST=OFF
            -DCUDA_TOOLKIT_ROOT_DIR=${CUDA_PATH}
            -DCMAKE_INSTALL_PREFIX=${CAFFE2_BUILD_ROOT}/install
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DCMAKE_DEBUG_POSTFIX=""
            ${CMAKE_CURRENT_SOURCE_DIR}/frameworks/caffe2
        COMMAND make -j${NPROC} caffe2_gpu
        COMMAND make -j${NPROC} install
    )
    add_custom_target(caffe2 DEPENDS ${CAFFE2_SO})
    target_include_directories(backend_obj PRIVATE ${CAFFE2_BUILD_ROOT}/install/include)
    target_link_libraries(backend_obj PRIVATE ${CAFFE2_SO})
    add_dependencies(backend_obj caffe2)
endif()

## backend: Caffe support ##
if(USE_CAFFE)
    target_compile_definitions(backend PRIVATE USE_CAFFE)
    target_sources(backend_obj PRIVATE
        src/nexus/backend/caffe_densecap_model.cpp
        src/nexus/backend/caffe_model.cpp)
    set(CAFFE_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/frameworks/caffe)
    set(CAFFE_BUILD_ROOT ${CAFFE_SRC_ROOT}/build)
    set(CAFFE_SO ${CAFFE_BUILD_ROOT}/lib/libcaffe.so)
    add_custom_command(
        OUTPUT ${CAFFE_SO}
        WORKING_DIRECTORY ${CAFFE_SRC_ROOT}
        COMMAND make -j${NPROC} all)
    add_custom_target(caffe DEPENDS ${CAFFE_SO})
    target_include_directories(backend_obj PRIVATE ${CAFFE_SRC_ROOT}/src)
    target_include_directories(backend_obj PRIVATE ${CAFFE_SRC_ROOT}/include)
    target_link_libraries(backend_obj PRIVATE ${CAFFE_SO})
    add_dependencies(backend_obj caffe)
endif()

## backend: Darknet support ##
if(USE_DARKNET)
    target_compile_definitions(backend PRIVATE USE_DARKNET)
    target_sources(backend_obj PRIVATE src/nexus/backend/darknet_model.cpp)
    set(DARKNET_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/frameworks/darknet)
    set(DARKNET_SO ${DARKNET_SRC_ROOT}/libdarknet.so)
    add_custom_command(
        OUTPUT ${DARKNET_SO}
        WORKING_DIRECTORY ${DARKNET_SRC_ROOT}
        COMMAND make -j${NPROC} all)
    add_custom_target(darknet DEPENDS ${DARKNET_SO})
    target_include_directories(backend_obj PRIVATE ${DARKNET_SRC_ROOT}/src)
    target_include_directories(backend_obj PRIVATE ${DARKNET_SRC_ROOT}/include)
    target_link_libraries(backend_obj PRIVATE ${DARKNET_SO})
    add_dependencies(backend_obj darknet)
endif()

## backend: CUDA support ##
if(USE_GPU)
    target_compile_definitions(common PUBLIC USE_GPU)
    target_compile_definitions(tensorflow_wrapper PUBLIC USE_GPU)
    target_include_directories(common PUBLIC ${CUDA_PATH}/include)
    target_link_libraries(common PUBLIC
            ${CUDA_PATH}/lib64/stubs/libcuda.so
            ${CUDA_PATH}/lib64/libcudart.so
            ${CUDA_PATH}/lib64/libcurand.so)
endif()



###### tools/profiler ######
add_executable(profiler tools/profiler/profiler.cpp)
target_link_libraries(profiler PRIVATE common backend_obj)



###### tools/test_pb ######
# if(USE_GPU)
#     add_executable(test_pb tools/profiler/test_pb.cpp)
#     target_link_libraries(test_pb PRIVATE common backend_obj)
# endif()



###### tools/test_complex_query ######
# add_executable(test_complex_query
#         src/nexus/scheduler/complex_query.cpp
#         src/nexus/scheduler/sch_info.cpp
#         tools/test_complex_query.cpp)
# target_link_libraries(test_complex_query PRIVATE common)



###### tools/bench_tfshare ######
# if(USE_GPU AND USE_TENSORFLOW)
#     add_executable(bench_tfshare tools/bench_tfshare.cpp)
#     target_link_libraries(bench_tfshare PRIVATE common backend_obj)
# endif()



###### tools/bench_dispatcher ######
add_library(bench_dispatcher_obj
    tools/bench_dispatcher/fake_accessor.cpp
    tools/bench_dispatcher/fake_backend.cpp
    tools/bench_dispatcher/fake_frontend.cpp
)
target_include_directories(bench_dispatcher_obj PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/tools)
target_link_libraries(bench_dispatcher_obj PUBLIC dispatcher_obj)

add_executable(run_rankmt tools/bench_dispatcher/run_rankmt_main.cpp)
target_link_libraries(run_rankmt PUBLIC bench_dispatcher_obj)

add_executable(stress_rankmt tools/bench_dispatcher/stress_rankmt_main.cpp)
target_link_libraries(stress_rankmt PUBLIC bench_dispatcher_obj)



###### tools/test_gpu_mem_sharing ######
if(USE_GPU AND USE_TENSORFLOW)
    add_executable(test_gpu_mem_sharing tools/test_gpu_mem_sharing.cpp)
    target_link_libraries(test_gpu_mem_sharing PUBLIC common)
    target_link_libraries(test_gpu_mem_sharing PUBLIC tensorflow_wrapper)
    target_link_libraries(test_gpu_mem_sharing PUBLIC tensorflow_cc)
endif()



###### tools/test_preprocessing_scalability ######
add_executable(test_preprocessing_scalability tools/test_preprocessing_scalability.cpp)
target_link_libraries(test_preprocessing_scalability PUBLIC common)



###### tools/loadgen ######
add_executable(loadgen tools/loadgen.cpp)
target_link_libraries(loadgen PUBLIC common)



###### tests ######
add_executable(runtest
        tests/cpp/rps_meter_test.cpp
        tests/cpp/test_main.cpp
        tests/cpp/value_ranked_map_test.cpp)
target_link_libraries(runtest PRIVATE common GTest::GTest)



###### tests/ario ######
add_executable(ario_test
        tests/ario.cpp)
target_link_libraries(ario_test PRIVATE ario)



###### examples/simple_app ######
add_executable(simple examples/simple_app/src/simple.cpp)
target_link_libraries(simple PRIVATE nexus gflags)
add_executable(multimodel examples/simple_app/src/multimodel.cpp)
target_link_libraries(multimodel PRIVATE nexus gflags)
