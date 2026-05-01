"""
DAQ Data v2 Client.
Simplified client that connects to the centralized DaqDataV2 aggregator.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Generator
from typing import Any

import grpc
from google.protobuf.empty_pb2 import Empty
from google.protobuf.json_format import MessageToDict

from panoseti_grpc.generated import daq_data_v2_pb2, daq_data_v2_pb2_grpc
from panoseti_grpc.telemetry.logger import get_logger

class DaqDataV2Client:
    """Synchronous client for DaqDataV2."""
    def __init__(self, target: str, log_level: int = logging.INFO):
        self.target = target
        self.logger = get_logger("daq_data_v2.client", level=log_level)
        self.channel = grpc.insecure_channel(target)
        self.stub = daq_data_v2_pb2_grpc.DaqDataV2Stub(self.channel)

    def __enter__(self) -> DaqDataV2Client:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.channel.close()

    def ping(self, timeout: float = 1.0) -> bool:
        try:
            self.stub.Ping(Empty(), timeout=timeout)
            return True
        except grpc.RpcError:
            return False

    def stream_images(
        self,
        stream_movie: bool = True,
        stream_ph: bool = True,
        update_interval: float = 1.0,
        module_ids: list[int] | None = None,
    ) -> Generator[daq_data_v2_pb2.StreamImagesResponse, None, None]:
        request = daq_data_v2_pb2.StreamImagesRequest(
            stream_movie_data=stream_movie,
            stream_pulse_height_data=stream_ph,
            update_interval_seconds=update_interval,
            module_ids=module_ids or [],
        )
        yield from self.stub.StreamImages(request)

class AioDaqDataV2Client:
    """Asynchronous client for DaqDataV2."""
    def __init__(self, target: str, log_level: int = logging.INFO):
        self.target = target
        self.logger = get_logger("daq_data_v2.client", level=log_level)
        self.channel = grpc.aio.insecure_channel(target)
        self.stub = daq_data_v2_pb2_grpc.DaqDataV2Stub(self.channel)

    async def __aenter__(self) -> AioDaqDataV2Client:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.channel.close()

    async def ping(self, timeout: float = 1.0) -> bool:
        try:
            await self.stub.Ping(Empty(), timeout=timeout)
            return True
        except grpc.aio.AioRpcError:
            return False

    async def stream_images(
        self,
        stream_movie: bool = True,
        stream_ph: bool = True,
        update_interval: float = 1.0,
        module_ids: list[int] | None = None,
    ) -> AsyncIterator[daq_data_v2_pb2.StreamImagesResponse]:
        request = daq_data_v2_pb2.StreamImagesRequest(
            stream_movie_data=stream_movie,
            stream_pulse_height_data=stream_ph,
            update_interval_seconds=update_interval,
            module_ids=module_ids or [],
        )
        async for response in self.stub.StreamImages(request):
            yield response
