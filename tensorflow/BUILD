load(
    "@org_tensorflow//tensorflow:tensorflow.bzl",
    "tf_cc_shared_object",
    "tf_copts",
)

cc_library(
    name = "wrapper",
    srcs = ["tensorflow_wrapper.cpp"],
    hdrs = ["include/nexus/backend/tensorflow_wrapper.h"],
    copts = ["-Iinclude"] + tf_copts(),
    deps = [
        "@org_tensorflow//tensorflow/core:tensorflow",
    ],
)

tf_cc_shared_object(
    name = "libtensorflow_wrapper.so",
    linkopts = [
        "-z defs",
        "-Wl,--version-script=version_script.lds",
    ],
    deps = [
        ":wrapper",
    ],
)
