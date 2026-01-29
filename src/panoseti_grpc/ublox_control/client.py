#!/usr/bin/env python3
"""
The Python implementation of a gRPC UbloxControl client API.
This module provides both a synchronous and an asynchronous client for interacting
with the UbloxControl service, designed for extensibility and ease of use.
"""
import asyncio
import logging
from typing import List, Dict, Any, Union, Optional, Generator, AsyncIterator

import grpc
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict, MessageToDict

from panoseti_grpc.generated import ublox_control_pb2
from panoseti_grpc.generated import ublox_control_pb2_grpc
from .resources import make_rich_logger


# --- Synchronous Client ---

class UbloxControlClient:
    """A synchronous gRPC client for the UbloxControl service.
    This client provides blocking methods to interact with one or more servers.
    It is ideal for scripting and simple, sequential operations.

    Usage:
        with UbloxControlClient(["localhost:50051"]) as client:
            if client.ping("localhost:50051"):
                client.init_f9t(...)
    """

    def __init__(self, hosts: List[str], log_level: int = logging.INFO):
        self.logger = make_rich_logger("ublox_control.client.sync", level=log_level)
        if not hosts:
            raise ValueError("hosts list cannot be empty")
        self.hosts = {host: {} for host in hosts}
        self.valid_hosts = set()

    def __enter__(self):
        for host, data in self.hosts.items():
            try:
                channel = grpc.insecure_channel(host)
                grpc.channel_ready_future(channel).result(timeout=1.0)
                data['channel'] = channel
                data['stub'] = ublox_control_pb2_grpc.UbloxControlStub(channel)
                self.valid_hosts.add(host)
                self.logger.info(f"Successfully connected to {host}")
            except grpc.FutureTimeoutError:
                self.logger.error(f"Timeout connecting to {host}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for data in self.hosts.values():
            if data.get('channel'):
                data['channel'].close()
        self.logger.info("All client channels closed.")

    def ping(self, host: str, timeout=1.0) -> bool:
        """Checks if a host is responsive by verifying channel readiness."""
        if host not in self.hosts or 'channel' not in self.hosts[host]:
            return False
        try:
            grpc.channel_ready_future(self.hosts[host]['channel']).result(timeout=timeout)
            return True
        except grpc.FutureTimeoutError:
            return False

    def init_f9t(self, hosts: List[str], f9t_cfg: Dict[str, Any], timeout=10.0) -> bool:
        """Initializes the F9T device on the specified hosts."""
        success_flags = []
        for host in hosts:
            if host not in self.valid_hosts:
                self.logger.warning(f"Skipping invalid host: {host}")
                continue
            stub = self.hosts[host]['stub']
            request = ublox_control_pb2.InitF9tRequest(f9t_cfg=ParseDict(f9t_cfg, Struct()))
            try:
                response = stub.InitF9t(request, timeout=timeout)
                if response.init_status == ublox_control_pb2.InitF9tResponse.InitStatus.SUCCESS:
                    self.logger.info(f"InitF9t succeeded on {host}")
                    success_flags.append(True)
                else:
                    self.logger.error(f"InitF9t failed on {host}: {response.message}")
                    success_flags.append(False)
            except grpc.RpcError as e:
                self.logger.error(f"RPC error during InitF9t on {host}: {e}")
                success_flags.append(False)
        return all(success_flags)

    def capture_ublox(self, host: str, patterns: Optional[List[str]] = None) -> Generator[Dict[str, Any], None, None]:
        """Streams and yields parsed data from the CaptureUblox RPC."""
        if host not in self.valid_hosts:
            raise ConnectionError(f"Host {host} is not valid or connected.")
        stub = self.hosts[host]['stub']
        request = ublox_control_pb2.CaptureUbloxRequest(patterns=patterns or [])
        try:
            for response in stub.CaptureUblox(request):
                yield MessageToDict(response, preserving_proto_field_name=True)
        except grpc.RpcError as e:
            self.logger.error(f"Stream from host '{host}' failed: {e.details()}")


# --- Asynchronous Client ---

class AioUbloxControlClient:
    """An asynchronous gRPC client for the UbloxControl service.
    Built on `grpc.aio`, this client is designed for use within an asyncio
    event loop and is ideal for building responsive, concurrent applications.

    Usage:
        async with AioUbloxControlClient(["localhost:50051"]) as client:
            if await client.ping("localhost:50051"):
                await client.init_f9t(...)
                async for data in client.capture_ublox(...):
                    print(data)
    """

    def __init__(self, hosts: List[str], stop_event: Optional[asyncio.Event] = None, log_level: int = logging.INFO):
        self.logger = make_rich_logger("ublox_control.client.async", level=log_level)
        if not hosts:
            raise ValueError("hosts list cannot be empty")
        self.hosts = {host: {} for host in hosts}
        self._stop_event = stop_event
        self.valid_hosts = set()

    async def __aenter__(self):
        for host, data in self.hosts.items():
            try:
                channel = grpc.aio.insecure_channel(host)
                await asyncio.wait_for(channel.channel_ready(), timeout=1.0)
                data['channel'] = channel
                data['stub'] = ublox_control_pb2_grpc.UbloxControlStub(channel)
                self.valid_hosts.add(host)
                self.logger.info(f"Successfully connected to {host}")
            except (asyncio.TimeoutError, grpc.aio.AioRpcError):
                self.logger.error(f"Failed to connect to {host}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        tasks = [data['channel'].close() for data in self.hosts.values() if data.get('channel')]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.info("All async client channels closed.")
        # Suppress common exceptions during graceful shutdown
        return exc_type is None or exc_type in [asyncio.CancelledError, grpc.RpcError]

    async def ping(self, host: str, timeout=1.0) -> bool:
        """Checks if a host is responsive by verifying channel readiness."""
        if host not in self.hosts or 'channel' not in self.hosts[host]:
            return False
        try:
            await asyncio.wait_for(self.hosts[host]['channel'].channel_ready(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def init_f9t(self, hosts: List[str], f9t_cfg: Dict[str, Any], timeout=10.0) -> bool:
        """Concurrently initializes the F9T device on the specified hosts."""

        async def _init_single_host(host):
            if host not in self.valid_hosts:
                self.logger.warning(f"Skipping invalid host: {host}")
                return False
            stub = self.hosts[host]['stub']
            request = ublox_control_pb2.InitF9tRequest(f9t_cfg=ParseDict(f9t_cfg, Struct()))
            try:
                response = await stub.InitF9t(request, timeout=timeout)
                if response.init_status == ublox_control_pb2.InitF9tResponse.InitStatus.SUCCESS:
                    self.logger.info(f"InitF9t succeeded on {host}")
                    return True
                self.logger.error(f"InitF9t failed on {host}: {response.message}")
                return False
            except grpc.aio.AioRpcError as e:
                self.logger.error(f"RPC error on {host}: {e}")
                return False

        results = await asyncio.gather(*[_init_single_host(host) for host in hosts])
        return all(results)

    async def capture_ublox(self, hosts: List[str], patterns: Optional[List[str]] = None) -> AsyncIterator[
        Dict[str, Any]]:
        """Concurrently streams data from multiple hosts and merges the results."""
        request = ublox_control_pb2.CaptureUbloxRequest(patterns=patterns or [])
        streams = [self.hosts[h]['stub'].CaptureUblox(request) for h in hosts if h in self.valid_hosts]
        if not streams: return

        queue = asyncio.Queue()

        async def _forward(stream, host_id):
            try:
                async for response in stream:
                    response_dict = MessageToDict(response, preserving_proto_field_name=True)
                    response_dict['host'] = host_id
                    await queue.put(response_dict)
            except grpc.aio.AioRpcError as e:
                if e.code() != grpc.StatusCode.CANCELLED:
                    self.logger.error(f"Stream error from {host_id}: {e}")
            finally:
                await queue.put(None)  # Sentinel

        tasks = [asyncio.create_task(_forward(s, h)) for s, h in zip(streams, hosts)]
        finished = 0
        while finished < len(tasks):
            if self._stop_event and self._stop_event.is_set(): break
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
                if item is None:
                    finished += 1
                else:
                    yield item
            except asyncio.TimeoutError:
                continue

        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
