# Copyright 2020 the gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("//bazel:grpc_build_system.bzl", "grpc_proto_library")
load("//bazel:python_rules.bzl", "py_grpc_library", "py_proto_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])


# The following three rules demonstrate the usage of the cc_grpc_library rule in
# in a mode compatible with the native proto_library and cc_proto_library rules.

cc_proto_library(
    name = "helloworld_cc_proto",
    deps = [":helloworld_proto"],
)

cc_grpc_library(
    name = "helloworld_cc_grpc",
    srcs = [":helloworld_proto"],
    grpc_only = True,
    deps = [":helloworld_cc_proto"],
)

py_proto_library(
    name = "helloworld_py_pb2",
    deps = [":helloworld_proto"],
)

py_proto_library(
    name = "daq_data_py_pb2",
    deps = [":daq_data_proto"],
)

py_proto_library(
    name = "ublox_control_py_pb2",
    deps = [":ublox_control_proto"],
)

py_grpc_library(
    name = "helloworld_py_pb2_grpc",
    srcs = [":helloworld_proto"],
    deps = [":helloworld_py_pb2"],
)

py_grpc_library(
    name = "daq_data_py_pb2_grpc",
    srcs = [":daq_data_proto"],
    deps = [":daq_data_py_pb2"],
)

py_grpc_library(
    name = "ublox_control_py_pb2_grpc",
    srcs = [":ublox_control_proto"],
    deps = [":ublox_control_py_pb2"],
)

grpc_proto_library(
    name = "auth_sample",
    srcs = ["auth_sample.proto"],
)

grpc_proto_library(
    name = "hellostreamingworld",
    srcs = ["hellostreamingworld.proto"],
)

grpc_proto_library(
    name = "keyvaluestore",
    srcs = ["keyvaluestore.proto"],
)

grpc_proto_library(
    name = "route_guide",
    srcs = ["route_guide.proto"],
)

grpc_proto_library(
    name = "ublox_control",
    srcs = ["ublox_control.proto"],
)

grpc_proto_library(
    name = "daq_data",
    srcs = ["daq_data.proto"],
)

proto_library(
    name = "helloworld_proto",
    srcs = ["helloworld.proto"],
)

proto_library(
    name = "keyvaluestore_proto",
    srcs = ["keyvaluestore.proto"],
)

proto_library(
    name = "route_guide_proto",
    srcs = [":route_guide.proto"],
)

proto_library(
    name = "ublox_control_proto",
    srcs = [":ublox_control.proto"],
)

proto_library(
    name = "daq_data_proto",
    srcs = [":daq_data.proto"],
)
