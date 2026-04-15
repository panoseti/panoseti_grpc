"""
PANOSETI Unified gRPC Server Manager.

Hosts multiple PANOSETI services on a single port.  Services are registered
through the :class:`ServiceRegistry` and toggled via :class:`ServiceToggles`
in the unified ``server.toml`` config.

Deployment profiles
-------------------
``panoseti-server``                     — all enabled services (default ``server.toml``)
``panoseti-server --profile daq_node``  — daq_data + daq_control (no local telemetry)
``panoseti-server --profile headnode``  — telemetry only

Initialization order
--------------------
Services are started in INIT_ORDER (telemetry → daq_data → daq_control).
Telemetry is always first so that the gRPC logging endpoint is live before
other servicers begin emitting log RPCs.  gRPC channels are lazy-connecting,
so no hard race condition exists, but the ordering is preserved for clarity
and to minimise dropped log messages during startup.

Extension
---------
To add a new service:
  1. Implement its servicer + proto.
  2. Write an async factory ``_make_<name>_servicer(cfg, shutdown_event)``.
  3. Add a config field to :class:`PanosetiServerConfig` and a toggle to
     :class:`ServiceToggles`.
  4. Call ``ServiceRegistry.register(ServiceDescriptor(...))`` at module level.
  5. Add a ``[<name>]`` section to ``server.toml``.

No changes to :class:`PanosetiServer` are required.
"""

from __future__ import annotations

import asyncio
import importlib.resources as _importlib_resources
import logging
import signal
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import grpc
from grpc_reflection.v1alpha import reflection
from pydantic import BaseModel, Field

# Per-service config models
from panoseti_grpc.daq_control.config import DaqControlServerConfig
from panoseti_grpc.daq_data.config import DaqDataServerConfig

# Protobuf-generated descriptors and registration functions
from panoseti_grpc.generated import (
    daq_control_pb2,
    daq_control_pb2_grpc,
    daq_data_pb2,
    daq_data_pb2_grpc,
    telemetry_pb2,
    telemetry_pb2_grpc,
)
from panoseti_grpc.telemetry.config import TelemetryServerConfig

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ServiceDescriptor & ServiceRegistry
# ---------------------------------------------------------------------------


@dataclass
class ServiceDescriptor:
    """Describes one gRPC service and how to instantiate + register it."""

    name: str
    """Logical service name; must match a field on :class:`PanosetiServerConfig`."""

    servicer_factory: Callable
    """Async factory: ``async (cfg, shutdown_event) -> (servicer, [post_start_coros])``."""

    add_to_server_fn: Callable
    """gRPC registration function, e.g. ``add_DaqDataServicer_to_server``."""

    service_names_for_reflection: list[str]
    """Proto full service names used to register gRPC reflection."""

    config_field: str
    """Attribute name on :class:`PanosetiServerConfig` holding this service's config."""


class ServiceRegistry:
    """Maps service names to their :class:`ServiceDescriptor` objects.

    New services are registered at module import time via :meth:`register`.
    """

    _registry: dict[str, ServiceDescriptor] = {}

    @classmethod
    def register(cls, descriptor: ServiceDescriptor) -> None:
        cls._registry[descriptor.name] = descriptor

    @classmethod
    def get(cls, name: str) -> ServiceDescriptor:
        return cls._registry[name]

    @classmethod
    def all(cls) -> dict[str, ServiceDescriptor]:
        return dict(cls._registry)


# ---------------------------------------------------------------------------
# Pydantic configuration models
# ---------------------------------------------------------------------------


class ServiceToggles(BaseModel):
    """Controls which services are started by the unified server."""

    telemetry: bool = True
    daq_data: bool = True
    daq_control: bool = True


class PanosetiServerConfig(BaseModel):
    """Top-level configuration for the unified gRPC server.

    Loaded from a TOML file whose top-level table structure mirrors the
    nested Pydantic models here.  Unrecognised keys are ignored.
    """

    port: int = Field(50051, ge=1024, le=65535)
    shutdown_grace_period: float = Field(5.0, ge=0)
    log_dir: str | None = None
    grpc_logging: bool = True
    services: ServiceToggles = Field(default_factory=ServiceToggles)

    # Per-service sub-configs; keys must match ServiceDescriptor.config_field values
    telemetry: TelemetryServerConfig = Field(default_factory=TelemetryServerConfig)
    daq_data: DaqDataServerConfig = Field(default_factory=DaqDataServerConfig)
    daq_control: DaqControlServerConfig = Field(default_factory=DaqControlServerConfig)

    model_config = {"extra": "ignore"}

    @classmethod
    def _parse_toml_dict(cls, raw: dict) -> PanosetiServerConfig:
        """Validate a raw TOML dict.

        The TOML uses a ``[server]`` section for server-level settings and
        separate top-level sections for each service (``[daq_data]`` etc.).
        This method merges the ``[server]`` sub-dict into the top level so
        Pydantic can find ``port``, ``services``, etc. directly.
        """
        server_section = raw.pop("server", {})
        merged = {**server_section, **raw}
        return cls.model_validate(merged)

    @classmethod
    def from_toml(cls, path: str | Path) -> PanosetiServerConfig:
        """Load and validate from a TOML file."""
        with open(path, "rb") as f:
            raw = tomllib.load(f)
        return cls._parse_toml_dict(raw)

    @classmethod
    def load_default(cls) -> PanosetiServerConfig:
        """Load the bundled default ``server.toml`` from the package."""
        resource_path = _importlib_resources.files("panoseti_grpc.config").joinpath("server.toml")
        with resource_path.open("rb") as f:
            raw = tomllib.load(f)
        return cls._parse_toml_dict(raw)

    @classmethod
    def load_profile(cls, profile: str) -> PanosetiServerConfig:
        """Load a named bundled deployment profile.

        Valid profiles: ``"default"``, ``"daq_node"``, ``"headnode"``.
        """
        filenames = {
            "default": "server.toml",
            "daq_node": "server_daq_node.toml",
            "headnode": "server_headnode.toml",
        }
        if profile not in filenames:
            raise ValueError(f"Unknown profile '{profile}'. Valid: {list(filenames)}")
        resource_path = _importlib_resources.files("panoseti_grpc.config").joinpath(filenames[profile])
        with resource_path.open("rb") as f:
            raw = tomllib.load(f)
        return cls._parse_toml_dict(raw)


# ---------------------------------------------------------------------------
# Servicer factories
# ---------------------------------------------------------------------------


async def _make_telemetry_servicer(
    cfg: TelemetryServerConfig,
    shutdown_event: asyncio.Event,
) -> tuple[Any, list]:
    """Connect to Redis and create a TelemetryServicer."""
    import redis.asyncio as redis_asyncio

    from panoseti_grpc.telemetry.resources import get_config_path
    from panoseti_grpc.telemetry.server import TelemetryServicer

    # Resolve config path: explicit cfg override → env var → package default
    if cfg.telemetry_config_path:
        p = Path(cfg.telemetry_config_path)
        config_path = p if p.exists() else get_config_path()
    else:
        config_path = get_config_path()

    # Connect to Redis with retries (mirrors telemetry/server.py logic)
    r = None
    max_retries = 10
    for attempt in range(max_retries):
        try:
            _logger.info(
                f"Connecting to Redis at {cfg.redis_host}:{cfg.redis_port} (attempt {attempt + 1}/{max_retries})..."
            )
            r = redis_asyncio.Redis(
                host=cfg.redis_host,
                port=cfg.redis_port,
                db=cfg.redis_db,
                decode_responses=True,
            )
            await r.ping()
            _logger.info("Redis connection established.")
            break
        except redis_asyncio.ConnectionError as e:
            _logger.warning(f"Redis connection failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                raise RuntimeError(
                    f"Could not connect to Redis at {cfg.redis_host}:{cfg.redis_port} after {max_retries} attempts."
                ) from e

    servicer = TelemetryServicer(config_path, r)
    return servicer, []


async def _make_daq_data_servicer(
    cfg: DaqDataServerConfig,
    shutdown_event: asyncio.Event,
) -> tuple[Any, list]:
    from panoseti_grpc.daq_data.server import DaqDataServicer

    servicer = DaqDataServicer(cfg)
    # start_initial_task must run after server.start()
    return servicer, [servicer.start_initial_task()]


async def _make_daq_control_servicer(
    cfg: DaqControlServerConfig,
    shutdown_event: asyncio.Event,
) -> tuple[Any, list]:
    from panoseti_grpc.daq_control.server import DaqControlServicer

    level = getattr(logging, cfg.log_level, logging.INFO)
    servicer = DaqControlServicer(level=level)
    return servicer, []


# ---------------------------------------------------------------------------
# Built-in service registrations
# ---------------------------------------------------------------------------

for _desc in [
    ServiceDescriptor(
        name="telemetry",
        servicer_factory=_make_telemetry_servicer,
        add_to_server_fn=telemetry_pb2_grpc.add_TelemetryServicer_to_server,
        service_names_for_reflection=[
            telemetry_pb2.DESCRIPTOR.services_by_name["Telemetry"].full_name,
        ],
        config_field="telemetry",
    ),
    ServiceDescriptor(
        name="daq_data",
        servicer_factory=_make_daq_data_servicer,
        add_to_server_fn=daq_data_pb2_grpc.add_DaqDataServicer_to_server,
        service_names_for_reflection=[
            daq_data_pb2.DESCRIPTOR.services_by_name["DaqData"].full_name,
        ],
        config_field="daq_data",
    ),
    ServiceDescriptor(
        name="daq_control",
        servicer_factory=_make_daq_control_servicer,
        add_to_server_fn=daq_control_pb2_grpc.add_DaqControlServicer_to_server,
        service_names_for_reflection=[
            daq_control_pb2.DESCRIPTOR.services_by_name["DaqControl"].full_name,
        ],
        config_field="daq_control",
    ),
]:
    ServiceRegistry.register(_desc)


# ---------------------------------------------------------------------------
# PanosetiServer
# ---------------------------------------------------------------------------


class PanosetiServer:
    """Unified gRPC server that hosts multiple PANOSETI services on one port.

    Services are instantiated in :attr:`INIT_ORDER`.  Telemetry is always
    first so that the gRPC logging endpoint is available before other
    servicers emit their first log RPCs.
    """

    INIT_ORDER = ["telemetry", "daq_data", "daq_control"]

    def __init__(self, cfg: PanosetiServerConfig) -> None:
        self.cfg = cfg
        self._server: grpc.aio.Server | None = None
        self._servicers: dict[str, Any] = {}
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Build the gRPC server, register all enabled services, and start listening."""
        self._server = grpc.aio.server()
        all_service_names: list[str] = []
        post_start_coros: list = []

        for name in self.INIT_ORDER:
            if not getattr(self.cfg.services, name, False):
                continue
            descriptor = ServiceRegistry.get(name)
            service_cfg = getattr(self.cfg, descriptor.config_field)

            _logger.info(f"Initialising service: {name}")
            servicer, extra_coros = await descriptor.servicer_factory(service_cfg, self._shutdown_event)
            descriptor.add_to_server_fn(servicer, self._server)
            self._servicers[name] = servicer
            all_service_names.extend(descriptor.service_names_for_reflection)
            post_start_coros.extend(extra_coros)

        if not self._servicers:
            raise RuntimeError("No services are enabled. Check [server.services] in server.toml.")

        reflection.enable_server_reflection(all_service_names + [reflection.SERVICE_NAME], self._server)
        self._server.add_insecure_port(f"[::]:{self.cfg.port}")
        await self._server.start()
        _logger.info(f"PanosetiServer started on port {self.cfg.port} with services: {list(self._servicers)}")

        # Post-start tasks (e.g. DaqDataServicer.start_initial_task)
        for coro in post_start_coros:
            asyncio.create_task(coro)

    async def wait_for_shutdown(self) -> None:
        """Block until the shutdown event is set."""
        await self._shutdown_event.wait()
        await self._stop()

    async def _stop(self) -> None:
        """Ordered shutdown: servicers in reverse init order, then gRPC server."""
        for name in reversed(self.INIT_ORDER):
            servicer = self._servicers.get(name)
            if servicer and hasattr(servicer, "shutdown"):
                _logger.info(f"Shutting down service: {name}")
                try:
                    await servicer.shutdown()
                except Exception as e:
                    _logger.error(f"Error shutting down {name}: {e}", exc_info=True)

        if self._server:
            _logger.info(f"Stopping gRPC server (grace={self.cfg.shutdown_grace_period}s)...")
            await self._server.stop(self.cfg.shutdown_grace_period)

        _logger.info("PanosetiServer stopped.")

    def _install_signal_handlers(self) -> None:
        """Attach SIGINT/SIGTERM to the shutdown event (main thread only)."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._shutdown_event.set)
            except RuntimeError as e:
                _logger.warning(f"Could not install signal handler for {sig}: {e}")

    @classmethod
    async def run(
        cls,
        cfg: PanosetiServerConfig,
        in_main_thread: bool = True,
    ) -> None:
        """Convenience entry point: create, start, wait for shutdown."""
        server = cls(cfg)
        await server.start()
        if in_main_thread:
            server._install_signal_handlers()
        await server.wait_for_shutdown()
