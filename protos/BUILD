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
load("//bazel:python_rules.bzl", "py_grpc_library", "py_proto_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

# --- Base Proto Libraries ---
# Defines the source .proto files.

proto_library(
    name = "daq_data_proto",
    srcs = ["daq_data.proto"],
)

proto_library(
    name = "ublox_control_proto",
    srcs = ["ublox_control.proto"],
)


# --- C++ Targets for daq_data.proto ---

# Generates the C++ message classes (e.g., PanoImage).
cc_proto_library(
    name = "daq_data_cc_proto",
    deps = [":daq_data_proto"],
)

# Generates the C++ gRPC service stubs (e.g., DaqData::Stub).
cc_grpc_library(
    name = "daq_data_cc_grpc",
    srcs = [":daq_data_proto"],
    grpc_only = True,
    deps = [":daq_data_cc_proto"],
)


# --- Python Targets for daq_data.proto ---

# Generates daq_data_pb2.py
py_proto_library(
    name = "daq_data_py_pb2",
    deps = [":daq_data_proto"],
)

# Generates daq_data_pb2_grpc.py
py_grpc_library(
    name = "daq_data_py_pb2_grpc",
    srcs = [":daq_data_proto"],
    deps = [":daq_data_py_pb2"],
)


# --- Python Targets for ublox_control.proto ---

# Generates ublox_control_pb2.py
py_proto_library(
    name = "ublox_control_py_pb2",
    deps = [":ublox_control_proto"],
)

# Generates ublox_control_pb2_grpc.py
py_grpc_library(
    name = "ublox_control_py_pb2_grpc",
    srcs = [":ublox_control_proto"],
    deps = [":ublox_control_py_pb2"],
)
