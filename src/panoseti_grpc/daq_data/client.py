from __future__ import annotations

import asyncio
import logging
import warnings
from collections.abc import AsyncIterator, Generator
from typing import Any

import grpc
from google.protobuf.empty_pb2 import Empty

from panoseti_grpc.generated import daq_data_pb2_grpc
from panoseti_grpc.generated.daq_data_pb2 import (
    InitHpIoRequest,
    StatusResponse,
    StreamImagesRequest,
    StreamImagesResponse,
)
from panoseti_grpc.grpc_utils import from_rpc_error, grpc_call
from panoseti_grpc.grpc_utils.channel import AsyncChannelManager, keepalive_options
from panoseti_grpc.grpc_utils.health import HealthClient
from panoseti_grpc.grpc_utils.retries import build_retry_service_config
from panoseti_grpc.telemetry.logger import get_logger

from .client_models import InitHpIoParameters, StreamImagesParameters
from .resources import load_package_json, parse_pano_image

_HEALTH_SERVICE = "daqdata.DaqData"
_hp_io_config_simulate: dict[str, Any] = load_package_json(
    "panoseti_grpc.daq_data", "config/hp_io_config_simulate.json"
)


class AioDaqDataClient:
    """Async gRPC client for the PANOSETI DaqData service.

    Single-target design: connects to one server endpoint (host:port).
    Multi-node fan-out is handled server-side by the aggregator gateway
    (Phase 4 — see ``daq_data/aggregator.py``).

    Use as an async context manager::

        async with AioDaqDataClient("daqnode-1", 50051) as client:
            if await client.ping():
                async for frame in client.stream_images(...):
                    process(frame)
    """

    def __init__(self, host: str = "localhost", port: int = 50051, log_level: int = logging.INFO) -> None:
        self._host = host
        self._port = port
        self.target = f"{host}:{port}"
        _options = [*keepalive_options(), ("grpc.service_config", build_retry_service_config())]
        self._channel_mgr = AsyncChannelManager(host, port, options=_options)
        self._stub: daq_data_pb2_grpc.DaqDataStub | None = None
        self.logger = get_logger("daq_data.client", level=log_level)

    async def __aenter__(self) -> AioDaqDataClient:
        await self._channel_mgr.__aenter__()
        self._stub = daq_data_pb2_grpc.DaqDataStub(self._channel_mgr.channel)
        return self

    async def __aexit__(self, *args: object) -> None:
        await self._channel_mgr.__aexit__(*args)

    @property
    def stub(self) -> daq_data_pb2_grpc.DaqDataStub:
        if self._stub is None:
            raise RuntimeError("AioDaqDataClient must be used as an async context manager.")
        return self._stub

    async def ping(self, timeout: float = 5.0) -> bool:  # noqa: ASYNC109
        """Return True if the DaqData service reports SERVING."""
        hc = HealthClient(self._host, self._port)
        return await asyncio.to_thread(hc.check, _HEALTH_SERVICE, timeout)

    @grpc_call
    async def status(self, timeout: float = 5.0) -> StatusResponse:  # noqa: ASYNC109
        """Return the current DaqData service status."""
        return await self.stub.Status(Empty(), timeout=timeout)  # type: ignore[misc]

    @grpc_call
    async def init_hp_io(self, params: dict[str, Any], timeout: float = 10.0) -> bool:  # noqa: ASYNC109
        """Reconfigure the HpIoManager on the server (optional with auto-init).

        Edge servers with ``init_from_default = true`` in their profile auto-start
        real UDS acquisition at startup, so callers no longer need to call this as
        a prerequisite before streaming.  Use it only when you need to override
        the running configuration (e.g. change ``data_dir``, switch to simulation,
        or force a restart mid-run).

        Args:
            params: Dict matching ``InitHpIoParameters`` fields
                    (``data_dir``, ``simulate_daq``, ``force``, etc.).
            timeout: RPC timeout in seconds.

        Returns:
            True if the server accepted and started the new configuration.

        Raises:
            PanosetiRpcError: On any gRPC-level failure.
        """
        v = InitHpIoParameters(**params)
        req = InitHpIoRequest(
            data_dir=v.data_dir,
            update_interval_seconds=v.update_interval_seconds,
            simulate_daq=v.simulate_daq,
            force=v.force,
            module_ids=v.module_ids,
        )
        self.logger.info(f"InitHpIo({self.target}): simulate={v.simulate_daq}, dir={v.data_dir}")
        resp = await self.stub.InitHpIo(req, timeout=timeout)  # type: ignore[misc]
        return bool(resp.success)

    async def init_sim(self, hp_io_cfg: dict[str, Any] | None = None, timeout: float = 5.0) -> bool:  # noqa: ASYNC109
        """Initialize a simulated data stream.

        .. deprecated::
            Pass ``simulate_daq=True`` directly to :meth:`init_hp_io`.
            This wrapper will be removed in a future release.
        """
        warnings.warn(
            "init_sim() is deprecated; call init_hp_io({'simulate_daq': True, ...}) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        config: dict[str, Any] = _hp_io_config_simulate.copy()
        config["simulate_daq"] = True
        if hp_io_cfg:
            config.update(hp_io_cfg)
        return await self.init_hp_io(config, timeout=timeout)

    @grpc_call
    async def stream_images(
        self,
        stream_movie_data: bool = True,
        stream_pulse_height_data: bool = True,
        update_interval_seconds: float = 1.0,
        module_ids: tuple[int, ...] = (),
        parse_pano_images: bool = True,
        timeout: float = 36_000.0,  # noqa: ASYNC109
        wait_for_ready: bool = False,
    ) -> AsyncIterator[dict[str, Any] | StreamImagesResponse]:
        """Stream PanoImages from the DaqData service.

        Yields:
            Parsed image dicts (``parse_pano_images=True``) or raw
            ``StreamImagesResponse`` protos.

        Raises:
            PanosetiRpcError: On any gRPC-level failure during streaming.
        """
        v = StreamImagesParameters(
            stream_movie_data=stream_movie_data,
            stream_pulse_height_data=stream_pulse_height_data,
            update_interval_seconds=update_interval_seconds,
            module_ids=list(module_ids),
        )
        req = StreamImagesRequest(
            stream_movie_data=v.stream_movie_data,
            stream_pulse_height_data=v.stream_pulse_height_data,
            update_interval_seconds=v.update_interval_seconds,
            module_ids=v.module_ids,
        )
        self.logger.info(f"StreamImages({self.target}): interval={v.update_interval_seconds}s")
        async for resp in self.stub.StreamImages(req, timeout=timeout, wait_for_ready=wait_for_ready):
            if parse_pano_images:
                yield parse_pano_image(resp.pano_image)
            else:
                yield resp


class DaqDataClient:
    """Synchronous gRPC client for the PANOSETI DaqData service.

    Single-target design: connects to one server endpoint (host:port).

    Use as a context manager::

        with DaqDataClient("daqnode-1", 50051) as client:
            if client.ping():
                for frame in client.stream_images(...):
                    process(frame)
    """

    def __init__(self, host: str = "localhost", port: int = 50051, log_level: int = logging.INFO) -> None:
        self._host = host
        self._port = port
        self.target = f"{host}:{port}"
        self._channel: grpc.Channel | None = None
        self._stub: daq_data_pb2_grpc.DaqDataStub | None = None
        self.logger = get_logger("daq_data.client", level=log_level)

    def __enter__(self) -> DaqDataClient:
        _options = keepalive_options()
        self._channel = grpc.insecure_channel(self.target, options=_options)
        self._stub = daq_data_pb2_grpc.DaqDataStub(self._channel)
        return self

    def __exit__(self, *args: object) -> None:
        if self._channel is not None:
            self._channel.close()
            self._channel = None

    @property
    def stub(self) -> daq_data_pb2_grpc.DaqDataStub:
        if self._stub is None:
            raise RuntimeError("DaqDataClient must be used as a context manager.")
        return self._stub

    def ping(self, timeout: float = 5.0) -> bool:
        """Return True if the DaqData service reports SERVING."""
        hc = HealthClient(self._host, self._port)
        return hc.check(_HEALTH_SERVICE, timeout)

    @grpc_call
    def status(self, timeout: float = 5.0) -> StatusResponse:
        """Return the current DaqData service status."""
        return self.stub.Status(Empty(), timeout=timeout)  # type: ignore[misc]

    @grpc_call
    def init_hp_io(self, params: dict[str, Any], timeout: float = 10.0) -> bool:
        """Reconfigure the HpIoManager on the server (optional with auto-init).

        See :meth:`AioDaqDataClient.init_hp_io` for full documentation.
        """
        v = InitHpIoParameters(**params)
        req = InitHpIoRequest(
            data_dir=v.data_dir,
            update_interval_seconds=v.update_interval_seconds,
            simulate_daq=v.simulate_daq,
            force=v.force,
            module_ids=v.module_ids,
        )
        self.logger.info(f"InitHpIo({self.target}): simulate={v.simulate_daq}, dir={v.data_dir}")
        resp = self.stub.InitHpIo(req, timeout=timeout)  # type: ignore[misc]
        return bool(resp.success)

    def init_sim(self, hp_io_cfg: dict[str, Any] | None = None, timeout: float = 5.0) -> bool:
        """Initialize a simulated data stream.

        .. deprecated::
            Pass ``simulate_daq=True`` directly to :meth:`init_hp_io`.
        """
        warnings.warn(
            "init_sim() is deprecated; call init_hp_io({'simulate_daq': True, ...}) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        config: dict[str, Any] = _hp_io_config_simulate.copy()
        config["simulate_daq"] = True
        if hp_io_cfg:
            config.update(hp_io_cfg)
        return self.init_hp_io(config, timeout=timeout)

    def stream_images(
        self,
        stream_movie_data: bool = True,
        stream_pulse_height_data: bool = True,
        update_interval_seconds: float = 1.0,
        module_ids: tuple[int, ...] = (),
        parse_pano_images: bool = True,
        timeout: float = 36_000.0,
        wait_for_ready: bool = False,
    ) -> Generator[dict[str, Any] | StreamImagesResponse]:
        """Stream PanoImages from the DaqData service (blocking generator)."""
        v = StreamImagesParameters(
            stream_movie_data=stream_movie_data,
            stream_pulse_height_data=stream_pulse_height_data,
            update_interval_seconds=update_interval_seconds,
            module_ids=list(module_ids),
        )
        req = StreamImagesRequest(
            stream_movie_data=v.stream_movie_data,
            stream_pulse_height_data=v.stream_pulse_height_data,
            update_interval_seconds=v.update_interval_seconds,
            module_ids=v.module_ids,
        )
        self.logger.info(f"StreamImages({self.target}): interval={v.update_interval_seconds}s")
        try:
            for resp in self.stub.StreamImages(req, timeout=timeout, wait_for_ready=wait_for_ready):
                if parse_pano_images:
                    yield parse_pano_image(resp.pano_image)
                else:
                    yield resp
        except grpc.RpcError as e:
            raise from_rpc_error(e, self.target) from e
