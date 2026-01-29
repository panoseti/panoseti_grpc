#!/usr/bin/env python3

"""
The Python implementation of a gRPC DaqUtils client.
Requires the following to work:
    1. All Python packages specified in requirements.txt.
Run this on the headnode to configure the u-blox GNSS receivers in remote domes.
"""
import asyncio
from typing import Set, List, Callable, Tuple, Any, Dict, Generator, AsyncIterator, Union, AsyncGenerator, Optional
import logging
import os
import json
from pathlib import Path
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
from panoseti_grpc.generated import (
    daq_data_pb2,
    daq_data_pb2_grpc,
)
from panoseti_grpc.generated.daq_data_pb2 import (
    PanoImage, StreamImagesResponse, StreamImagesRequest, InitHpIoRequest, InitHpIoResponse, UploadImageRequest
)
from panoseti_grpc.panoseti_util import control_utils

## daq_data utils
from .resources import make_rich_logger, parse_pano_image, format_stream_images_response, load_package_json

hp_io_config_simulate = load_package_json("panoseti_grpc.daq_data", "config/hp_io_config_simulate.json")

class DaqDataClient:
    """A synchronous gRPC client for the PANOSETI DaqData service.

    This client provides blocking methods to interact with one or more DAQ nodes,
    including pinging for health checks, initializing the data flow, and streaming
    image data. It is ideal for scripting and simple, sequential operations.

    It is designed to be used as a context manager, which automatically handles
    the setup and teardown of gRPC connections:

    with DaqDataClient(...) as client:
        if client.ping("localhost"):
            client.init_hp_io(...)
    """
    GRPC_PORT = 50051

    def __init__(
        self,
        daq_config: Union[str, Path, Dict[str, Any]],
        network_config: Optional[Union[str, Path, Dict[str, Any]]],
        log_level: int =logging.INFO
    ):
        """Initializes the DaqDataClient with DAQ and network configurations.

        Args:
            daq_config (Union[str, Path, Dict]): Path to a daq_config.json file
                or a pre-loaded dictionary. Must contain a 'daq_nodes' key.
            network_config (Optional[Union[str, Path, Dict]]): Path to a
                network_config.json file or a dictionary for port forwarding.
                Can be None if no port forwarding is needed.
            log_level (int): The logging verbosity level (e.g., logging.INFO).
        """
        self.logger = make_rich_logger("daq_data.client", level=log_level)

        # Load daq config, if necessary
        if daq_config is None:
            raise ValueError("daq_config cannot be None")
        elif isinstance(daq_config, str) or isinstance(daq_config, Path):
            if not os.path.exists(daq_config):
                abs_path = os.path.abspath(daq_config)
                emsg = f"daq_config_path={abs_path} does not exist"
                self.logger.error(emsg)
                raise FileNotFoundError(emsg)
            with open(daq_config, 'r') as f:
                daq_config_dict = json.load(f)
        elif isinstance(daq_config, dict):
            daq_config_dict = daq_config
        else:
            raise ValueError(f"daq_config is not a str, Path, or dict: {daq_config=}")

        # validate daq_config
        if 'daq_nodes' not in daq_config_dict or daq_config_dict['daq_nodes'] is None or len(daq_config_dict['daq_nodes']) == 0:
            raise ValueError(f"daq_nodes is empty: {daq_config_dict=}")
        for daq_node in daq_config_dict['daq_nodes']:
            if 'ip_addr' not in daq_node:
                raise ValueError(f"daq_node={daq_node} does not have an 'ip_addr' key")

        # Validate network_config
        if not network_config:
            network_config_dict = None
        elif isinstance(network_config, str) or isinstance(network_config, Path):
            if not os.path.exists(network_config):
                abs_path = os.path.abspath(network_config)
                emsg = f"network_config_path={abs_path} does not exist"
                self.logger.error(emsg)
                raise FileNotFoundError(emsg)
            with open(network_config, 'r') as f:
                network_config_dict = json.load(f)
        elif isinstance(network_config, dict):
            network_config_dict = network_config
        else:
            raise ValueError(f"network_config is not a str, Path, or dict: {network_config=}")

        # add port forwarding info to daq_config if network_config is specified
        if network_config_dict is not None:
            if 'daq_nodes' not in network_config_dict or network_config_dict['daq_nodes'] is None or len(
                    network_config_dict['daq_nodes']) == 0:
                raise ValueError(f"daq_nodes is empty: {network_config_dict=}")
            control_utils.attach_daq_config(daq_config_dict, network_config_dict)

        # Parse real host ips for each daq node
        self.valid_daq_hosts = set()
        self.daq_nodes = {}
        for daq_node in daq_config_dict['daq_nodes']:
            daq_cfg_ip = daq_node['ip_addr']
            if 'port_forwarding' in daq_node:
                real_ip = daq_node['port_forwarding']['gw_ip']
                port = self.GRPC_PORT
                self.logger.info(f'Using port forwarding: "{daq_cfg_ip=}:{port}" --> "{real_ip=}:{port}"')
                daq_host = real_ip
            else:
                daq_host = daq_cfg_ip
            self.daq_nodes[daq_host] = {'config': daq_node}
            self.daq_nodes[daq_host]['channel']: grpc.Channel = None
            self.daq_nodes[daq_host]['stub']: daq_data_pb2_grpc.DaqDataStub = None

    def __enter__(self):
        """
        Establishes gRPC channels to all configured DAQ nodes upon entering a context block.

        Returns:
            DaqDataClient: The instance of the client.
        """
        for daq_host, daq_node in self.daq_nodes.items():
            if daq_host.startswith('unix:'):
                grpc_connection_target = f"{daq_host}"
            else:
                grpc_connection_target = f"{daq_host}:{self.GRPC_PORT}"
            daq_node['connection_target'] = grpc_connection_target
            try:
                channel = grpc.insecure_channel(grpc_connection_target)
                daq_node['channel'] = channel
                daq_node['stub'] = daq_data_pb2_grpc.DaqDataStub(channel)
                if self.ping(daq_host):
                    self.valid_daq_hosts.add(daq_host)
            except grpc.RpcError as rpc_error:
                self.logger.error(f"{type(rpc_error)}\n{repr(rpc_error)}")
                continue
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes all open gRPC channels upon exiting a context block."""
        self.logger.debug("Closing all client gRPC channels.")
        for daq_host, daq_node in self.daq_nodes.items():
            if daq_node.get('channel'):
                daq_node['channel'].close()
                self.logger.debug(f"Closed channel to {daq_node['connection_target']}")

        self.logger.info("All client channels closed.")

        # Let the context manager suppress expected exceptions on exit, but re-raise others.
        if exc_type and exc_type in [ConnectionError]:
            self.logger.warning(f"Client exiting: {exc_val}")
            return True  # Don't re-raise the exception
        elif exc_type and exc_type not in [grpc.FutureCancelledError, grpc.RpcError, KeyboardInterrupt, SystemExit]:
            self.logger.error(f"Client exiting due to an unhandled exception: {exc_val}")
            return False # Re-raise the exception
        return True # Suppress expected exceptions

    def get_valid_daq_hosts(self) -> List[str]:
        """
        Returns a set of valid DAQ hosts that responded successfully to a ping.

        Returns:
            Set[str]: A set of IP addresses or hostnames of responsive DAQ nodes.
        """
        for host in self.daq_nodes:
            self.is_daq_host_valid(host)
        return list(self.valid_daq_hosts)

    def get_daq_host_status(self) -> Dict[str, bool]:
        valid_status = {}
        for host in self.daq_nodes:
            connection_target = self.daq_nodes[host]['connection_target']
            valid_status[connection_target] = self.is_daq_host_valid(host)
        return valid_status

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
                self.valid_daq_hosts.remove(host)
            return False
        self.valid_daq_hosts.add(host)
        return True

    def validate_daq_hosts(self, hosts: Optional[Union[List[str], str]]) -> List[str]:
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
        host_set = set()
        if isinstance(hosts, str) and len(hosts) > 0:
            host_set = {hosts}
        elif isinstance(hosts, list) and len(hosts) > 0:
            host_set = set(hosts)
        elif isinstance(hosts, set) and len(hosts) > 0:
            host_set = set(hosts)
        elif hosts is None or len(hosts) == 0:
            host_set = self.get_valid_daq_hosts()
        else:
            raise ValueError(f"hosts={repr(hosts)} must be a non-empty str, list of str, or None, got {type(hosts)}")
        for host in host_set:
            if not self.is_daq_host_valid(host):
                raise ConnectionError(
                    f"host={repr(host)} does not have a valid gRPC server channel. Valid daq_hosts: {self.valid_daq_hosts}")
        valid_hosts = self.get_valid_daq_hosts()
        if len(valid_hosts) == 0:
            raise ConnectionError("No valid daq hosts found")
        return list(host_set)

    def reflect_services(self, hosts: Union[List[str], str]) -> str:
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
        valid_hosts = self.validate_daq_hosts(hosts)
        for host in valid_hosts:
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
        hosts: Optional[Union[List[str], str]],
        stream_movie_data: bool,
        stream_pulse_height_data: bool,
        update_interval_seconds: float,
        module_ids: Union[Tuple[int], Tuple[()]]=(),
        wait_for_ready=False,
        parse_pano_images=True,
        timeout=36_000
    ) ->  Generator[dict[str, Any], Any, Any]:
        """Establishes a real-time stream of PANOSETI image data.

        This method sends a `StreamImagesRequest` and returns a generator that
        yields image data as it arrives from the servers. This is a blocking call
        that will run indefinitely.

        Args:
            hosts (Union[List[str], str]): The DAQ host(s) to stream from.
            stream_movie_data (bool): If True, request movie-mode images.
            stream_pulse_height_data (bool): If True, request pulse-height images.
            update_interval_seconds (float): The requested server-side update interval.
            module_ids (Tuple[int], optional): A tuple of module IDs to subscribe to.
                If empty, streams data from all active modules. Defaults to ().
            parse_pano_images (bool, optional): If True, parses the raw protobuf message
                into a Python dictionary. Defaults to True.
            wait_for_ready (bool, optional): If True, waits for the server to be ready
                before attempting to stream. Defaults to False.
            timeout (float, optional): The timeout in seconds for the RPC call. Defaults to float('inf').

        Returns:
            Generator[Dict[str, Any], None, None]: A generator that yields either
            parsed image data dictionaries or raw protobuf responses.
        """
        valid_hosts = self.validate_daq_hosts(hosts)

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
        for host in valid_hosts:
            daq_node = self.daq_nodes[host]
            stub = daq_node['stub']
            stream_images_responses = stub.StreamImages(stream_images_request, wait_for_ready=wait_for_ready, timeout=timeout)
            streams.append(stream_images_responses)
            self.logger.info(f"Created StreamImages RPC to {host=}")

        def response_generator():
            """Yields responses from each StreamImagesResponse stream in a round-robin fashion."""
            while True:
                for stream in streams:
                    try:
                        stream_images_response = next(stream)
                    except StopIteration:
                        return
                    formatted_stream_images_response = format_stream_images_response(stream_images_response)
                    self.logger.debug(formatted_stream_images_response)
                    if parse_pano_images:
                        yield parse_pano_image(stream_images_response.pano_image)
                    else:
                        yield stream_images_response
        return response_generator()

    def init_sim(self, hosts: Optional[Union[List[str], str]], hp_io_cfg: Optional[Dict] = None, timeout=10.0) -> bool:
        """
        A convenience method for initializing a simulated run using a JSON config file.

        This is a wrapper around `init_hp_io` that loads a configuration file intended for
        simulated data streams. It is useful for development and testing without access to
        live observatory hardware.

        Args:
            hosts (Union[List[str], str]): The hostname or IP address of the DAQ node.
            hp_io_cfg (Dict, optional): The simulation config. Defaults to None.
            timeout (float, optional): The timeout in seconds for the RPC call. Defaults to 10.0.

        Returns:
            bool: True if the simulated initialization succeeded.
        """
        if hp_io_cfg is None:
            config_to_send = hp_io_config_simulate
            config_to_send['simulate_daq'] = True
        else:
            config_to_send = hp_io_cfg
        assert config_to_send['simulate_daq'] is True, f"{hp_io_cfg} for init_sim must have simulate_daq=True"
        return self.init_hp_io(hosts, config_to_send, timeout)


    def init_hp_io(self, hosts: Optional[Union[List[str], str]], hp_io_cfg: dict, timeout=10.0) -> bool:
        """Initializes or reconfigures the HpIoManager task on the server.

        This is a prerequisite for streaming data. It tells the server which
        directory to monitor for data or whether to start a simulation.

        Args:
            hosts (Union[List[str], str]): One or more DAQ hosts to initialize.
            hp_io_cfg (dict): A configuration dictionary containing parameters like
                'data_dir', 'simulate_daq', 'force', etc.
            timeout (float, optional): Timeout in seconds for the RPC call. Defaults to 10.0.

        Returns:
            bool: True if the InitHpIo RPC succeeds on all specified hosts.
        """
        valid_hosts = self.validate_daq_hosts(hosts)

        # Call InitHpIo RPCs
        init_successes = []
        for host in valid_hosts:
            daq_node = self.daq_nodes[host]
            stub = daq_node['stub']

            init_hp_io_request = InitHpIoRequest(
                data_dir=daq_node['config']['data_dir'],
                update_interval_seconds=hp_io_cfg['update_interval_seconds'],
                simulate_daq=hp_io_cfg['simulate_daq'],
                force=hp_io_cfg['force'],
                module_ids=hp_io_cfg['module_ids'],
            )
            self.logger.info(f"Initializing hp_io on '{daq_node['connection_target']}'...")
            try:
                init_hp_io_response = stub.InitHpIo(init_hp_io_request, timeout=timeout)
            except grpc.RpcError as e:
                self.logger.error(f"Failed to init {host}: {e}")
                return False
            self.logger.info(f"{host=}: {init_hp_io_response.success=}")
            init_successes.append(init_hp_io_response.success)
        return all(init_successes)

    def ping(self, host: str, timeout=0.3) -> bool:
        """
        Pings a DAQ host to check if its DaqData gRPC server is active and responsive.

        Args:
            host (str): The hostname or IP address of the DAQ node.
            timeout (float, optional): The timeout in seconds for the Ping call. Defaults to 0.5.

        Returns:
            bool: True if the host responds successfully within the timeout, False otherwise.
        """
        if host not in self.daq_nodes:
            self.logger.debug(f"host={host} not found in daq_nodes. Valid hosts: {self.daq_nodes.keys()}")
            return False
        stub = self.daq_nodes[host]['stub']
        try:
            ping_response = stub.Ping(Empty(), timeout=timeout, wait_for_ready=True)
            return True
        except grpc.RpcError as e:
            return False

    def upload_images(self, hosts: Union[List[str], str], image_iterator: Generator[PanoImage, None, None]):
        """Uploads a stream of PanoImage protobuf objects to the server.

        This allows a client to act as a data source, injecting images directly
        into the server's processing queue. This is primarily used by the 'rpc'
        simulation strategy.

        Args:
            hosts (Union[List[str], str]): The DAQ host(s) to upload to.
            image_iterator (Generator[PanoImage, None, None]): A generator that yields
                PanoImage protobuf objects.
        """
        valid_hosts = self.validate_daq_hosts(hosts)

        def request_generator(iterator):
            for image in iterator:
                yield UploadImageRequest(pano_image=image, wait_for_ready=True)

        for host in valid_hosts:
            stub = self.daq_nodes[host]['stub']
            try:
                stub.UploadImages(request_generator(image_iterator))
                self.logger.info(f"Finished uploading images to {host}.")
            except grpc.RpcError as e:
                self.logger.error(f"Failed to upload images to {host}: {e}")
                raise



class AioDaqDataClient:
    """An asynchronous gRPC client for the PANOSETI DaqData service.

    Built on `grpc.aio`, this client provides non-blocking methods to interact
    with DAQ nodes. It is designed for use within an `asyncio` event loop and
    is ideal for building responsive applications and concurrent tasks.

    It supports a `stop_event` for graceful shutdown of streams and should be
    used as an async context manager:

    async with AioDaqDataClient(...) as client:
        status = await client.ping("localhost")
        if status:
            await client.init_hp_io(...)
    """
    GRPC_PORT = 50051

    def __init__(
        self,
        daq_config: Union[str, Path, Dict[str, Any]],
        network_config: Optional[Union[str, Path, Dict[str, Any]]],
        stop_event: Optional[asyncio.Event] = None,
        log_level: int = logging.INFO,
    ):
        """Initializes the AioDaqDataClient.

        Args:
            daq_config (Union[str, Path, Dict]): Path to a daq_config.json file
                or a pre-loaded dictionary. Must contain a 'daq_nodes' key.
            network_config (Optional[Union[str, Path, Dict]]): Path to a
                network_config.json file or a dictionary for port forwarding.
            stop_event (Optional[asyncio.Event]): An event to signal graceful
                shutdown of long-running streams.
            log_level (int): The logging verbosity level (e.g., logging.INFO).
        """
        self.logger = make_rich_logger("daq_data.client", level=log_level)

        # Load daq config, if necessary
        if daq_config is None:
            raise ValueError("daq_config cannot be None")
        elif isinstance(daq_config, str) or isinstance(daq_config, Path):
            if not os.path.exists(daq_config):
                abs_path = os.path.abspath(daq_config)
                emsg = f"daq_config_path={abs_path} does not exist"
                self.logger.error(emsg)
                raise FileNotFoundError(emsg)
            with open(daq_config, 'r') as f:
                daq_config_dict = json.load(f)
        elif isinstance(daq_config, dict):
            daq_config_dict = daq_config
        else:
            raise ValueError(f"daq_config is not a str, Path, or dict: {daq_config=}")

        # validate daq_config
        if 'daq_nodes' not in daq_config_dict or daq_config_dict['daq_nodes'] is None or len(daq_config_dict['daq_nodes']) == 0:
            raise ValueError(f"daq_nodes is empty: {daq_config_dict=}")
        for daq_node in daq_config_dict['daq_nodes']:
            if 'ip_addr' not in daq_node:
                raise ValueError(f"daq_node={daq_node} does not have an 'ip_addr' key")

        # Validate network_config
        if not network_config:
            network_config_dict = None
        elif isinstance(network_config, str) or isinstance(network_config, Path):
            if not os.path.exists(network_config):
                abs_path = os.path.abspath(network_config)
                emsg = f"network_config_path={abs_path} does not exist"
                self.logger.error(emsg)
                raise FileNotFoundError(emsg)
            with open(network_config, 'r') as f:
                network_config_dict = json.load(f)
        elif isinstance(network_config, dict):
            network_config_dict = network_config
        else:
            raise ValueError(f"network_config is not a str, Path, or dict: {network_config=}")

        # add port forwarding info to daq_config if network_config is specified
        if network_config_dict is not None:
            if 'daq_nodes' not in network_config_dict or network_config_dict['daq_nodes'] is None or len(
                    network_config_dict['daq_nodes']) == 0:
                raise ValueError(f"daq_nodes is empty: {network_config_dict=}")
            control_utils.attach_daq_config(daq_config_dict, network_config_dict)

        # Parse real host ips for each daq node
        self.valid_daq_hosts = set()
        self.daq_nodes = {}
        for daq_node in daq_config_dict['daq_nodes']:
            daq_cfg_ip = daq_node['ip_addr']
            if 'port_forwarding' in daq_node:
                real_ip = daq_node['port_forwarding']['gw_ip']
                port = self.GRPC_PORT
                self.logger.info(f'Using port forwarding: "{daq_cfg_ip=}:{port}" --> "{real_ip=}:{port}"')
                daq_host = real_ip
            else:
                daq_host = daq_cfg_ip
            self.daq_nodes[daq_host] = {'config': daq_node}
            self.daq_nodes[daq_host]['channel']: grpc.aio.Channel = None
            self.daq_nodes[daq_host]['stub']: daq_data_pb2_grpc.DaqDataStub = None
        self._stop_event = stop_event

    async def __aenter__(self):
        """Establishes async gRPC channels to all configured DAQ nodes."""
        for daq_host, daq_node in self.daq_nodes.items():
            if daq_host.startswith('unix:'):
                grpc_connection_target = f"{daq_host}"
            else:
                grpc_connection_target = f"{daq_host}:{self.GRPC_PORT}"
            daq_node['connection_target'] = grpc_connection_target
            try:
                channel = grpc.aio.insecure_channel(grpc_connection_target) # Use async channel
                daq_node['channel'] = channel
                daq_node['stub'] = daq_data_pb2_grpc.DaqDataStub(channel)
                if await self.ping(daq_host):
                    self.valid_daq_hosts.add(daq_host)
            except grpc.RpcError as rpc_error:
                self.logger.error(f"Failed to connect to {daq_host}: {rpc_error}")
                continue
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Closes all open async gRPC channels."""
        self.logger.debug("Closing all async client gRPC channels.")
        tasks = []
        for daq_host, daq_node in self.daq_nodes.items():
            if daq_node.get('channel'):
                # Create a task to close each channel
                task = asyncio.create_task(daq_node['channel'].close())
                tasks.append(task)

        if tasks:
            # Wait for all close tasks to complete, even if some have errors
            await asyncio.gather(*tasks, return_exceptions=True)

        self.logger.info("All async client channels closed.")

        # Suppress common exceptions that occur during a graceful shutdown (like Ctrl+C),
        # but allow other, unexpected exceptions to propagate.
        if exc_type and exc_type in [ConnectionError]:
            self.logger.warning(f"Client exiting: {exc_val}")
            return True  # Don't re-raise the exception
        elif exc_type and exc_type not in [asyncio.CancelledError, grpc.FutureCancelledError, grpc.RpcError, KeyboardInterrupt, SystemExit]:
            self.logger.error(f"Client exiting due to an unhandled exception: {exc_val}")
            return False # Re-raise the exception
        return True # Suppress expected exceptions

    async def get_valid_daq_hosts(self) -> List[str]:
        """
        Returns a set of valid DAQ hosts that responded successfully to a ping.

        Returns:
            Set[str]: A set of IP addresses or hostnames of responsive DAQ nodes.
        """
        for host in self.daq_nodes:
            await self.is_daq_host_valid(host)
        return list(self.valid_daq_hosts)

    async def get_daq_host_status(self) -> Dict[str, bool]:
        valid_status = {}
        for host in self.daq_nodes:
            connection_target = self.daq_nodes[host]['connection_target']
            valid_status[connection_target] = await self.is_daq_host_valid(host)
        return valid_status

    async def is_daq_host_valid(self, host: str) -> bool:
        """
        Checks if a given host is responsive.

        Args:
            host (str): IP or hostname of the DAQ node.

        Returns:
            bool: True if the host is valid and responsive.
        """
        if host not in self.daq_nodes:
            return False
        if not await self.ping(host):
            if host in self.valid_daq_hosts:
                self.valid_daq_hosts.remove(host)
            return False
        self.valid_daq_hosts.add(host)
        return True

    async def validate_daq_hosts(self, hosts: Union[List[str], str]) -> List[str]:
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
        host_set = set()
        if isinstance(hosts, str) and len(hosts) > 0:
            host_set = {hosts}
        elif isinstance(hosts, list) and len(hosts) > 0:
            host_set = set(hosts)
        elif isinstance(hosts, set) and len(hosts) > 0:
            host_set = set(hosts)
        elif hosts is None or len(hosts) == 0:
            host_set = await self.get_valid_daq_hosts()
        else:
            raise ValueError(f"hosts={repr(hosts)} must be a non-empty str, list of str, or None, got {type(hosts)}")
        for host in host_set:
            if not await self.is_daq_host_valid(host):
                raise ConnectionError(
                    f"host={repr(host)} does not have a valid gRPC server channel. Valid daq_hosts: {self.valid_daq_hosts}")
        valid_hosts = await self.get_valid_daq_hosts()
        if len(valid_hosts) == 0:
            raise ConnectionError("No valid daq hosts found")
        return list(host_set)

    async def reflect_services(self, hosts: Union[List[str], str]) -> str:
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
        valid_hosts = await self.validate_daq_hosts(hosts)
        for host in valid_hosts:
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

    async def stream_images(
            self,
            hosts: Union[List[str], str],
            stream_movie_data: bool,
            stream_pulse_height_data: bool,
            update_interval_seconds: float,
            module_ids: Union[Tuple[int], Tuple[()]] = (),
            wait_for_ready=False,
            parse_pano_images=True,
            timeout=36_000
    ) -> AsyncIterator[dict[str, Any]]:
        """Establishes an asynchronous, real-time stream of PANOSETI image data.

        This method sends a `StreamImagesRequest` and returns a generator that
        yields image data as it arrives from the servers. This is a non-blocking call
        that will run indefinitely or until timeout is reached.

        Args:
            hosts (Union[List[str], str]): The DAQ host(s) to stream from.
            stream_movie_data (bool): If True, request movie-mode images.
            stream_pulse_height_data (bool): If True, request pulse-height images.
            update_interval_seconds (float): The requested server-side update interval.
            module_ids (Tuple[int], optional): A tuple of module IDs to subscribe to.
                If empty, streams data from all active modules. Defaults to ().
            parse_pano_images (bool, optional): If True, parses the raw protobuf message
                into a Python dictionary. Defaults to True.
            wait_for_ready (bool, optional): If True, waits for the server to be ready
                before attempting to stream. Defaults to False.
            timeout (float, optional): The timeout in seconds for the RPC call. Defaults to float('inf').

        Returns:
            Generator[Dict[str, Any], None, None]: A generator that yields either
            parsed image data dictionaries or raw protobuf responses.
        """
        valid_hosts = await self.validate_daq_hosts(hosts)

        stream_images_request = StreamImagesRequest(
            stream_movie_data=stream_movie_data,
            stream_pulse_height_data=stream_pulse_height_data,
            update_interval_seconds=update_interval_seconds,
            module_ids=module_ids,
        )

        streams = [self.daq_nodes[host]['stub'].StreamImages(stream_images_request, wait_for_ready=wait_for_ready, timeout=timeout) for
                   host in valid_hosts]
        self.logger.info(f"Created {len(streams)} StreamImages RPCs to hosts: {valid_hosts}")

        async def response_generator():
            queue = asyncio.Queue()

            async def _forward_stream(stream, host_id):
                try:
                    async for response in stream:
                        await queue.put(response)
                except grpc.aio.AioRpcError as e:
                    # Swallow expected disconnection errors, but propagate others.
                    if e.code() in (grpc.StatusCode.CANCELLED, grpc.StatusCode.UNAVAILABLE):
                        self.logger.warning(
                            f"Stream from host '{host_id}' terminated with an expected disconnection: {e.details()}")
                        await queue.put(None)  # Signal clean shutdown of this sub-stream
                    else:
                        self.logger.error(f"Stream from host '{host_id}' failed with a critical error: {e.details()}")
                        await queue.put(e)  # Propagate the critical error
                finally:
                    # Ensure a sentinel is placed if the stream ends without error.
                    if 'e' not in locals():
                        await queue.put(None)

            tasks = [asyncio.create_task(_forward_stream(s, h)) for s, h in zip(streams, valid_hosts)]

            try:
                finished_streams = 0
                while finished_streams < len(streams) and not (self._stop_event and self._stop_event.is_set()):
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    if item is None:
                        finished_streams += 1
                        continue

                    # Check for and re-raise propagated exceptions
                    if isinstance(item, Exception):
                        raise item

                    if parse_pano_images:
                        yield parse_pano_image(item.pano_image)
                    else:
                        yield item
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        return response_generator()

    async def init_sim(self, hosts: Union[List[str], str], hp_io_cfg: Optional[Dict]=None,
                       timeout=5.0) -> bool:
        """
        Asynchronously initializes a simulated run using a JSON config file.

        This is a wrapper around `init_hp_io` that loads a configuration file intended for
        simulated data streams. It is useful for development and testing without access to
        live observatory hardware.

        Args:
            hosts (Union[List[str], str]): The hostname or IP address of the DAQ node.
            hp_io_cfg (dict, optional): The simulation config. Defaults to None.
            timeout (float, optional): The timeout in seconds for the RPC call. Defaults to 5.0.

        Returns:
            bool: True if the simulated initialization succeeded.
        """
        # If no config is provided, create a minimal one to trigger simulation mode
        # on the server, which will then use its own default settings.
        if hp_io_cfg is None:
            config_to_send = hp_io_config_simulate
            config_to_send['simulate_daq'] = True
        else:
            config_to_send = hp_io_cfg
        assert config_to_send['simulate_daq'] is True, f"{hp_io_cfg} for init_sim must have simulate_daq=True"
        return await self.init_hp_io(hosts, config_to_send, timeout)


    async def init_hp_io(self, hosts: Union[List[str], str], hp_io_cfg: dict, timeout=10.0) -> bool:
        """Asynchronously initializes or reconfigures the HpIoManager on the server.

        This coroutine sends concurrent InitHpIo requests to all specified hosts
        and waits for them to complete.

        Args:
            hosts (Union[List[str], str]): One or more DAQ hosts to initialize.
            hp_io_cfg (dict): Configuration dictionary with parameters for the
                server's HpIoManager.
            timeout (float, optional): Timeout in seconds for each RPC call. Defaults to 10.0.

        Returns:
            bool: True if the InitHpIo RPC succeeds on all specified hosts.
        """
        valid_hosts = await self.validate_daq_hosts(hosts)

        async def _init_single_host(host):
            daq_node = self.daq_nodes[host]
            stub = daq_node['stub']
            init_hp_io_request = InitHpIoRequest(
                data_dir=hp_io_cfg.get('data_dir', ''),
                update_interval_seconds=hp_io_cfg['update_interval_seconds'],
                simulate_daq=hp_io_cfg['simulate_daq'],
                force=hp_io_cfg.get('force', False),
                module_ids=hp_io_cfg.get('module_ids', []),
            )

            self.logger.info(f"Initializing hp_io on {host}...")
            try:
                init_hp_io_response = await stub.InitHpIo(init_hp_io_request, timeout=timeout)
                self.logger.info(f"{host=}: {init_hp_io_response.success=}")
                return init_hp_io_response.success
            except grpc.aio.AioRpcError as e:
                self.logger.error(f"Failed to init {host}: {e}")
                raise e

        # Run all InitHpIo calls concurrently
        results = await asyncio.gather(*[_init_single_host(host) for host in valid_hosts])
        return all(results)

    async def ping(self, host: str, timeout=0.3) -> bool:
        """Pings a DAQ host asynchronously to check if its server is responsive."""
        if host not in self.daq_nodes:
            self.logger.debug(f"host={host} not found in daq_nodes. Valid hosts: {self.daq_nodes.keys()}")
            return False
        stub = self.daq_nodes[host]['stub']
        try:
            await stub.Ping(Empty(), timeout=timeout, wait_for_ready=True)
            return True
        except grpc.aio.AioRpcError:
            return False

    async def upload_images(self, hosts: Union[List[str], str], image_iterator: AsyncGenerator[PanoImage, None]):
        """Asynchronously uploads a stream of PanoImage objects to the server.

        This method is a coroutine that runs an upload stream to the specified
        server(s). It is primarily used by the 'rpc' simulation strategy.

        Args:
            hosts (Union[List[str], str]): The DAQ host(s) to upload to.
            image_iterator (AsyncGenerator[PanoImage, None]): An async generator
                that yields PanoImage protobuf objects to upload.
        """
        valid_hosts = await self.validate_daq_hosts(hosts)
        async def request_generator(iterator):
            async for image in iterator:
                yield UploadImageRequest(pano_image=image)

        upload_tasks = []
        try:
            for host in valid_hosts:
                stub = self.daq_nodes[host]['stub']
                #task = asyncio.create_task(stub.UploadImages(request_generator(image_iterator)))
                task = stub.UploadImages(request_generator(image_iterator), wait_for_ready=True)
                upload_tasks.append(task)
            
            await asyncio.gather(*upload_tasks)
            self.logger.info(f"Finished uploading images to all specified hosts: {valid_hosts}")
        except grpc.aio.AioRpcError as e:
            self.logger.error(f"Failed to upload images to all specified hosts: {valid_hosts}")
            raise e
        finally:
            for task in upload_tasks:
                task.cancel()
            await asyncio.gather(*upload_tasks, return_exceptions=True)

