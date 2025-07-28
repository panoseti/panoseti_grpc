#!/usr/bin/env python3

"""
The Python implementation of a gRPC DaqUtils client.
Requires the following to work:
    1. All Python packages specified in requirements.txt.
Run this on the headnode to configure the u-blox GNSS receivers in remote domes.
"""
from abc import ABC, abstractmethod
import asyncio
from typing import Set, List, Callable, Tuple, Any, Dict, Generator, AsyncIterator
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
from google.protobuf.empty_pb2 import Empty
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
from .daq_data_resources import make_rich_logger, parse_pano_image, format_stream_images_response


hp_io_config_simulate_path = "daq_data/config/hp_io_config_simulate.json"

class DaqDataClient:
    """
    A gRPC client for interacting with the PANOSETI DaqData service.

    This client facilitates communication with gRPC servers running on PANOSETI DAQ (Data Acquisition)
    nodes. It provides methods to ping servers, initialize the data-directory monitoring thread (`hp_io`),
    and stream real-time image data for Pulse-Height and Movie-Mode products.

    The client is designed to be used as a context manager to ensure that gRPC channels
    are properly opened and closed.
    """
    GRPC_PORT = 50051

    def __init__(self, daq_config: Dict[str, Any], log_level=logging.INFO):
        """
        Initializes the DaqDataClient with the configuration for one or more DAQ nodes.

        Args:
            daq_config (Dict[str, Any]): A configuration dictionary, typically loaded from a
                `daq_config.json` file. It must contain a 'daq_nodes' key with a list of
                dictionaries, where each dictionary represents a DAQ node and must contain
                at least an 'ip_addr' field.
            log_level (int): The logging verbosity level (e.g., logging.INFO, logging.DEBUG).

        Raises:
            ValueError: If the 'daq_nodes' list is empty, missing, or improperly formatted.
        """
        if 'daq_nodes' not in daq_config or daq_config['daq_nodes'] is None or len(daq_config['daq_nodes']) == 0:
            raise ValueError(f"daq_nodes is empty: {daq_config=}")
        self.daq_nodes = {}
        for daq_node_cfg in daq_config['daq_nodes']:
            if 'ip_addr' not in daq_node_cfg:
                raise ValueError(f"daq_node={daq_node_cfg} does not have an 'ip_addr' key")
            daq_host = daq_node_cfg['ip_addr']
            self.daq_nodes[daq_host] = {'config': daq_node_cfg}
            self.daq_nodes[daq_host]['channel']: grpc.Channel = None
            self.daq_nodes[daq_host]['stub']: daq_data_pb2_grpc.DaqDataStub = None
        self.daq_grpc_state = {}
        self.logger = make_rich_logger(__name__, level=log_level)
        self.valid_daq_hosts = set()

    def __enter__(self):
        """
        Establishes gRPC channels to all configured DAQ nodes upon entering a context block.

        Returns:
            DaqDataClient: The instance of the client.
        """
        for daq_host, daq_node in self.daq_nodes.items():
            grpc_connection_target = f"{daq_host}:{self.GRPC_PORT}"
            daq_node['connection_target'] = grpc_connection_target
            try:
                # channel = grpc.insecure_channel(grpc_connection_target)
                channel = grpc.insecure_channel(grpc_connection_target)
                daq_node['channel'] = channel
                daq_node['stub'] = daq_data_pb2_grpc.DaqDataStub(channel)
                if self.ping(daq_host):
                    self.valid_daq_hosts.add(daq_host)
            except grpc.RpcError as rpc_error:
                self.logger.error(f"{type(rpc_error)}\n{repr(rpc_error)}")
                continue
        return self

    def __exit__(self, etype, value, traceback):
        """
        Closes all open gRPC channels upon exiting a context block.
        """
        self.logger.setLevel(logging.INFO)
        for daq_host, daq_node in self.daq_nodes.items():
            if daq_node['channel'] is not None:
                daq_node['channel'].close()
                self.logger.debug(f"DaqDataClient closed channel to {daq_node['connection_target']}")
        self.logger.setLevel(logging.ERROR)
        exit_ok = False
        if value is None or isinstance(value, KeyboardInterrupt):
            exit_ok = True
        elif isinstance(value, SystemExit) and value.code == 0:
            exit_ok = True

        if exit_ok:
            self.logger.debug(f"{etype=}, {value=}, {traceback=}")
            return True
        self.logger.error(f"{etype=}, {value=}, {traceback=}")
        return False

    def get_valid_daq_hosts(self) -> Set[str]:
        """
        Returns a set of valid DAQ hosts that responded successfully to a ping.

        Returns:
            Set[str]: A set of IP addresses or hostnames of responsive DAQ nodes.
        """
        return self.valid_daq_hosts

    def is_daq_host_valid(self, host: str) -> bool:
        """
        Checks if a given host is responsive.

        Args:
            host (str): IP or hostname of the DAQ node.

        Returns:
            bool: True if the host is valid and responsive.
        """
        if host not in self.daq_nodes:
            return False
        if not self.ping(host):
            if host in self.valid_daq_hosts:
                return False
        self.valid_daq_hosts.add(host)
        return True

    def validate_daq_hosts(self, hosts: List[str] or str) -> List[str] or None:
        """
        Validates that a given list of hosts are active and reachable.

        If the input list is empty or None, it defaults to all known valid hosts.

        Args:
            hosts (Union[List[str], str]): A single host or list of hosts to validate.

        Returns:
            List[str]: A list of validated hostnames or IP addresses.

        Raises:
            ValueError: If any host is invalid or if no valid hosts can be found.
        """
        if isinstance(hosts, str):
            hosts = [hosts]
        elif hosts is None or len(hosts) == 0:
            valid_hosts = self.get_valid_daq_hosts()
            if len(valid_hosts) == 0:
                raise ValueError("No valid daq hosts found")
            hosts = valid_hosts
        for host in hosts:
            if not self.is_daq_host_valid(host):
                raise ValueError(f"daq_host={host} does not have a valid gRPC server channel. Valid daq_hosts: {self.valid_daq_hosts}")
        return hosts

    def reflect_services(self, hosts: List[str] or str) -> str:
        """
        Discovers and lists all available gRPC services and RPCs on the specified hosts.

        This method uses gRPC server reflection to dynamically query the server for its
        registered services, providing a human-readable summary.

        Args:
            hosts (Union[List[str], str]): One or more hosts to query. If empty, queries all
                known valid hosts.

        Returns:
            str: A formatted string detailing the available services and their RPC methods.
        """

        def format_rpc_service(method):
            name = method.name
            input_type = method.input_type.name
            output_type = method.output_type.name
            stream_fmt = '[magenta]stream[/magenta] '
            client_stream = stream_fmt if method.client_streaming else ""
            server_stream = stream_fmt if method.server_streaming else ""
            return f"rpc {name}({client_stream}{input_type}) returns ({server_stream}{output_type})"

        ret = ""
        hosts = self.validate_daq_hosts(hosts)
        for host in hosts:
            daq_node = self.daq_nodes[host]
            channel = daq_node['channel']
            reflection_db = ProtoReflectionDescriptorDatabase(channel)
            services = reflection_db.get_services()
            desc_pool = DescriptorPool(reflection_db)
            service_desc = desc_pool.FindServiceByName("daqdata.DaqData")
            ret += f"Reflecting services on {daq_node['connection_target']}:\n"
            msg = f"\tfound services: {services}\n"
            msg += f"\tfound [yellow]DaqData[/yellow] service with name: [yellow]{service_desc.full_name}[/yellow]"
            for method in service_desc.methods:
                msg += f"\n\tfound: {format_rpc_service(method)}"
            ret += msg
            ret += '\n'
        return ret

    def stream_images(
        self,
        hosts: List[str] or str,
        stream_movie_data: bool,
        stream_pulse_height_data: bool,
        update_interval_seconds: float,
        module_ids: Tuple[int]=(),
        wait_for_ready=False,
        parse_pano_images=True,
    ) ->  Generator[dict[str, Any], Any, Any]:
        """
        Establishes a real-time stream of PANOSETI image data from one or more DAQ nodes.

        This method sends a `StreamImagesRequest` to the specified hosts and returns an
        infinite generator that yields image data as it arrives from the servers.

        Args:
            hosts (Union[List[str], str]): The DAQ host(s) to stream from.
            stream_movie_data (bool): Set to True to request movie-mode images.
            stream_pulse_height_data (bool): Set to True to request pulse-height images.
            update_interval_seconds (float): The requested minimum time interval in seconds
                between consecutive image frames sent by the server.
            module_ids (Tuple[int], optional): A tuple of integer module IDs to subscribe to.
                If empty, the server will stream data from all active modules. Defaults to ().
            parse_pano_images (bool, optional): If True, the raw protobuf message is parsed
                into a Python dictionary. If False, the raw `StreamImagesResponse` protobuf
                object is returned. Defaults to True.

        Returns:
            Generator[Dict[str, Any], Any, Any]: An infinite generator that yields either
                parsed image data dictionaries or raw protobuf responses.

        Raises:
            grpc.RpcError: If the stream is interrupted or the connection fails.
        """
        hosts = self.validate_daq_hosts(hosts)

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
        streams = []
        for host in hosts:
            daq_node = self.daq_nodes[host]
            stub = daq_node['stub']
            stream_images_responses = stub.StreamImages(stream_images_request, wait_for_ready=wait_for_ready)
            streams.append(stream_images_responses)
            self.logger.info(f"Created StreamImages RPC to {host=}")

        def response_generator():
            """Yields responses from each StreamImagesResponse stream in a round-robin fashion."""
            while True:
                for stream in streams:
                    stream_images_response = next(stream)
                    formatted_stream_images_response = format_stream_images_response(stream_images_response)
                    self.logger.debug(formatted_stream_images_response)
                    if parse_pano_images:
                        yield parse_pano_image(stream_images_response.pano_image)
                    else:
                        yield stream_images_response
        return response_generator()

    def init_sim(self, hosts: List[str] or str, hp_io_sim_cfg_path=hp_io_config_simulate_path,timeout=5.0) -> bool:
        """
        A convenience method for initializing a simulated run using a JSON config file.

        This is a wrapper around `init_hp_io` that loads a configuration file intended for
        simulated data streams. It is useful for development and testing without access to
        live observatory hardware.

        Args:
            hosts (Union[List[str], str]): The hostname or IP address of the DAQ node.
            hp_io_sim_cfg_path (str, optional): The path to the simulation config file.
                Defaults to the path defined in `hp_io_config_simulate_path`.
            timeout (float, optional): The timeout in seconds for the RPC call. Defaults to 5.0.

        Returns:
            bool: True if the simulated initialization succeeded.
        """
        with open(hp_io_sim_cfg_path, 'r') as f:
            hp_io_config = json.load(f)
            assert hp_io_config['simulate_daq'] is True, f"{hp_io_sim_cfg_path} used init_sim must have simulate_daq=True"
        return self.init_hp_io(hosts, hp_io_config, timeout=timeout)

    def init_hp_io(self, hosts: List[str] or str, hp_io_cfg: dict, timeout=5.0) -> bool:
        """
        Initializes or reconfigures the `hp_io` thread on the DaqData server.

        The `hp_io` thread is responsible for monitoring a specified run directory for new
        data files and broadcasting them to `StreamImages` clients. This RPC call is required
        to start the data flow before clients can connect via `stream_images`.

        Args:
            hosts (Union[List[str], str]): One or more DAQ hosts to initialize.
            hp_io_cfg (dict): A configuration dictionary defining initialization parameters.
                It should contain the following keys from your `hp_io_config.json`:
                - `update_interval_seconds` (float): The directory polling interval.
                - `force` (bool): If True, forces reconfiguration even if other clients are
                  connected, disconnecting them in the process.
                - `simulate_daq` (bool): If True, streams archived data instead of monitoring a
                  live directory. Overrides `data_dir`.
                - `module_ids` (list[int]): A whitelist of module IDs to track. If empty, all
                  active modules are tracked.
            timeout (float, optional): The timeout in seconds for the RPC call. Defaults to 5.0.

        Returns:
            bool: True if the `InitHpIo` RPC succeeds on all specified hosts, False otherwise.
        """
        hosts = self.validate_daq_hosts(hosts)

        # Call InitHpIo RPCs
        init_successes = []
        for host in hosts:
            daq_node = self.daq_nodes[host]
            stub = daq_node['stub']

            init_hp_io_request = InitHpIoRequest(
                data_dir=daq_node['config']['data_dir'],
                update_interval_seconds=hp_io_cfg['update_interval_seconds'],
                simulate_daq=hp_io_cfg['simulate_daq'],
                force=hp_io_cfg['force'],
                module_ids=hp_io_cfg['module_ids'],
            )
            self.logger.info(f"Initializing the hp_io thread with "
                             f"{MessageToDict(init_hp_io_request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)}")

            init_hp_io_response = stub.InitHpIo(init_hp_io_request, timeout=timeout)
            self.logger.info(f"{host=}: {init_hp_io_response.success=}")
            init_successes.append(init_hp_io_response.success)
        return all(init_successes)

    def ping(self, host: str, timeout=0.5) -> bool:
        """
        Pings a DAQ host to check if its DaqData gRPC server is active and responsive.

        Args:
            host (str): The hostname or IP address of the DAQ node.
            timeout (float, optional): The timeout in seconds for the Ping call. Defaults to 0.5.

        Returns:
            bool: True if the host responds successfully within the timeout, False otherwise.
        """
        if host not in self.daq_nodes:
            return False
        stub = self.daq_nodes[host]['stub']
        try:
            stub.Ping(Empty(), timeout=timeout)
            return True
        except grpc.RpcError as e:
            return False

