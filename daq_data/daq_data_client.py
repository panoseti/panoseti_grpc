#!/usr/bin/env python3

"""
The Python implementation of a gRPC DaqUtils client.
Requires the following to work:
    1. All Python packages specified in requirements.txt.
Run this on the headnode to configure the u-blox GNSS receivers in remote domes.
"""
from abc import ABC, abstractmethod
from typing import List, Callable, Tuple, Any, Dict, Generator, AsyncIterator
import argparse
import logging
import os
from pathlib import Path
import sys
import signal
import numpy as np
from contextlib import contextmanager, asynccontextmanager
import json

# rich formatting
from rich import print
from rich.pretty import pprint, Pretty
from rich.console import Console

## gRPC imports
import grpc

# gRPC reflection service: allows clients to discover available RPCs
from google.protobuf.descriptor_pool import DescriptorPool
from grpc_reflection.v1alpha.proto_reflection_descriptor_database import (
    ProtoReflectionDescriptorDatabase,
)
# Standard gRPC protobuf types
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf import timestamp_pb2

# protoc-generated marshalling / demarshalling code
from daq_data import (
    daq_data_pb2,
    daq_data_pb2_grpc,
)
from .daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest, InitHpIoRequest, InitHpIoResponse

## daq_data utils
from .daq_data_resources import make_rich_logger, reflect_services, parse_pano_image, format_stream_images_response
from .daq_data_testing import run_all_tests, is_os_posix

hp_io_config_simulate_path = "daq_data/config/hp_io_config_simulate.json"

class DaqDataClient:
    GRPC_PORT = 50051

    def __init__(self, daq_config: Dict[str, Any], log_level=logging.INFO):
        self.daq_nodes = {}
        for daq_node_cfg in daq_config['daq_nodes']:
            if 'ip_addr' not in daq_node_cfg:
                raise ValueError(f"daq_node={daq_node_cfg} does not have an 'ip_addr' key")
            daq_host = daq_node_cfg['ip_addr']
            self.daq_nodes[daq_host] = {'config': daq_node_cfg}
            self.daq_nodes[daq_host]['channel']: grpc.Channel = None
        self.daq_grpc_state = {}
        self.logger = make_rich_logger(__name__, level=log_level)
        self.valid_daq_hosts = []

    def __enter__(self):
        for daq_host, daq_node in self.daq_nodes.items():
            grpc_connection_target = f"{daq_host}:{self.GRPC_PORT}"
            daq_node['connection_target'] = grpc_connection_target
            try:
                channel = grpc.insecure_channel(grpc_connection_target)
                daq_node['channel'] = channel
                self.valid_daq_hosts.append(daq_host)
            except grpc.RpcError as rpc_error:
                self.logger.error(f"{type(rpc_error)}\n{repr(rpc_error)}")
                continue
        return self

    def __exit__(self, etype, value, traceback):
        self.logger.setLevel(logging.INFO)
        for daq_host, daq_node in self.daq_nodes.items():
            if daq_node['channel'] is not None:
                daq_node['channel'].close()
                self.logger.debug(f"DaqDataClient closed channel to {daq_node['connection_target']}")
        self.logger.setLevel(logging.ERROR)
        if isinstance(value, KeyboardInterrupt) or value is None:
            self.logger.debug(f"{etype=}, {value=}, {traceback=}")
            return True
        self.logger.error(f"{etype=}, {value=}, {traceback=}")
        return False

    def get_valid_daq_hosts(self) -> List[str]:
        return self.valid_daq_hosts

    def is_daq_host_valid(self, daq_host: str) -> bool:
        return daq_host in self.valid_daq_hosts

    def reflect_services(self, daq_host: str) -> str:
        if not self.is_daq_host_valid(daq_host):
            raise ValueError(
                f"daq_host={daq_host} does not have a valid gRPC server channel. Valid daq_hosts: {self.valid_daq_hosts}")
        daq_node = self.daq_nodes[daq_host]
        channel = daq_node['channel']
        if channel is not None:
            self.logger.info(f"Reflecting services on {daq_node['connection_target']}:")
            return reflect_services(channel)

    def stream_images(
        self,
        daq_host: str,
        stream_movie_data: bool,
        stream_pulse_height_data: bool,
        update_interval_seconds: float,
        module_ids: Tuple[int]=(),
        wait_for_ready=False,
        parse_pano_images=True,
    ) ->  Generator[dict[str, Any], Any, Any]:
        """Streams PanoImages from an active observing run."""
        if not self.is_daq_host_valid(daq_host):
            raise ValueError(f"daq_host={daq_host} does not have a valid gRPC server channel. Valid daq_hosts: {self.valid_daq_hosts}")
        daq_node = self.daq_nodes[daq_host]
        channel = daq_node['channel']
        stub = daq_data_pb2_grpc.DaqDataStub(channel)

        # Create the request message
        stream_images_request = StreamImagesRequest(
            stream_movie_data=stream_movie_data,
            stream_pulse_height_data=stream_pulse_height_data,
            update_interval_seconds=update_interval_seconds,
            module_ids=module_ids,
        )
        self.logger.info(
            f"stream_images_request={MessageToDict(stream_images_request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)}")
        # Call the RPC
        stream_images_responses = stub.StreamImages(stream_images_request, wait_for_ready=wait_for_ready)

        def response_generator():
            """Yields responses from  StreamImagesResponse stream."""
            while True:
                stream_images_response = next(stream_images_responses)
                formatted_stream_images_response = format_stream_images_response(stream_images_response)
                self.logger.debug(formatted_stream_images_response)
                if parse_pano_images:
                    yield parse_pano_image(stream_images_response.pano_image)
                else:
                    yield stream_images_response
        return response_generator()

    def init_sim(self, daq_host: str, hp_io_sim_cfg_path=hp_io_config_simulate_path,timeout=5.0) -> bool:
        with open(hp_io_sim_cfg_path, 'r') as f:
            hp_io_config = json.load(f)
            assert hp_io_config['simulate_daq'] is True, f"{hp_io_sim_cfg_path} used init_sim must have simulate_daq=True"
        return self.init_hp_io(daq_host, hp_io_config, timeout=timeout)

    def init_hp_io(self, daq_host: str, hp_io_cfg: dict, timeout=5.0) -> bool:
        if not self.is_daq_host_valid(daq_host):
            raise ValueError(f"daq_host={daq_host} does not have a valid gRPC server channel. Valid daq_hosts: {self.valid_daq_hosts}")

        daq_node = self.daq_nodes[daq_host]
        channel = daq_node['channel']
        stub = daq_data_pb2_grpc.DaqDataStub(channel)

        init_hp_io_request = InitHpIoRequest(
            data_dir=daq_node['config']['data_dir'],
            update_interval_seconds=hp_io_cfg['update_interval_seconds'],
            simulate_daq=hp_io_cfg['simulate_daq'],
            force=hp_io_cfg['force'],
            module_ids=hp_io_cfg['module_ids'],
        )
        self.logger.info(f"Initializing the hp_io thread with "
                        f"{MessageToDict(init_hp_io_request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)}")
        # Call RPC
        init_hp_io_response = stub.InitHpIo(init_hp_io_request, timeout=timeout)
        self.logger.info(f"{init_hp_io_response.success=}")
        return init_hp_io_response.success
