"""
PANOSETI Unified gRPC Server Manager.

Hosts multiple PANOSETI services on a single port.  Services are registered
through the :class:`ServiceRegistry` and toggled via :class:`ServiceToggles`
in the unified ``server.toml`` config.

Deployment profiles
-------------------
``pseti-grpc server``                     — all enabled services (default ``server.toml``)
``pseti-grpc server --profile daq_node``  — daq_data + daq_control (no local telemetry)
``pseti-grpc server --profile headnode``  — telemetry only

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
import os
import signal
import tomllib
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

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
    ml_inference_pb2,
    ml_inference_pb2_grpc,
    telemetry_pb2,
    telemetry_pb2_grpc,
)
from panoseti_grpc.ml_inference.config import MLInferenceServerConfig
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

    servicer_factory: Callable[[Any, asyncio.Event], Coroutine[Any, Any, tuple[Any, list[Coroutine[Any, Any, None]]]]]
    """Async factory: ``async (cfg, shutdown_event) -> (servicer, [post_start_coros])``."""

    add_to_server_fn: Callable[[Any, grpc.aio.Server], None]
    """gRPC registration function, e.g. ``add_DaqDataServicer_to_server``."""

    service_names_for_reflection: list[str]
    """Proto full service names used to register gRPC reflection."""

    config_field: str
    """Attribute name on :class:`PanosetiServerConfig` holding this service's config."""

    deprecated: bool = False
    """If True, the service is in maintenance-only mode and will be removed in a future release."""


class ServiceRegistry:
    """Maps service names to their :class:`ServiceDescriptor` objects.

    New services are registered at module import time via :meth:`register`.
    """

    import typing

    _registry: typing.ClassVar[dict[str, ServiceDescriptor]] = {}

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
    ml_inference: bool = False


class PanosetiServerConfig(BaseModel):
    """Top-level configuration for the unified gRPC server.

    Loaded from a TOML file whose top-level table structure mirrors the
    nested Pydantic models here.  Unrecognised keys are ignored.
    """

    port: int = Field(default_factory=lambda: int(os.getenv("GRPC_PORT", 50051)), ge=1024, le=65535)
    shutdown_grace_period: float = Field(5.0, ge=0)
    log_dir: str | None = None
    grpc_logging: bool = True
    services: ServiceToggles = Field(default_factory=ServiceToggles)

    # Per-service sub-configs; keys must match ServiceDescriptor.config_field values
    telemetry: TelemetryServerConfig = Field(
        default_factory=lambda: TelemetryServerConfig(grpc_port=50051, shutdown_grace_period=5.0, log_level="INFO")
    )
    daq_data: DaqDataServerConfig = Field(
        default_factory=lambda: DaqDataServerConfig(
            max_concurrent_rpcs=100,
            max_read_queue_size=50,
            min_hp_io_update_interval_seconds=0.001,
            max_client_update_interval_seconds=60.0,
            max_reader_enqueue_timeouts=2,
            max_reader_dequeue_timeouts=3,
            reader_timeout=5.0,
            shutdown_grace_period=5.0,
            hp_io_stop_timeout=5.0,
        )
    )
    daq_control: DaqControlServerConfig = Field(
        default_factory=lambda: DaqControlServerConfig(grpc_port=50051, shutdown_grace_period=5.0, log_level="INFO")
    )
    ml_inference: MLInferenceServerConfig = Field(default_factory=MLInferenceServerConfig)

    model_config = {"extra": "ignore"}

    @classmethod
    def _parse_toml_dict(cls, raw: dict[str, Any]) -> PanosetiServerConfig:
        """Validate a raw TOML dict.

        The TOML uses a ``[server]`` section for server-level settings and
        separate top-level sections for each service (``[daq_data]`` etc.).
        This method merges the ``[server]`` sub-dict into the top level so
        Pydantic can find ``port``, ``services``, etc. directly.
        """
        raw_copy = raw.copy()
        server_section = raw_copy.pop("server", {})
        merged = {**server_section, **raw_copy}
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
            "gateway": "server_gateway.toml",
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
    cfg: TelemetryServerConfig, shutdown_event: asyncio.Event
) -> tuple[Any, list[Coroutine[Any, Any, None]]]:
    import redis.asyncio as redis

    from panoseti_grpc.telemetry.server import TelemetryServicer

    r = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, db=cfg.redis_db, decode_responses=True)
    # Ping to check connection
    await cast(Any, r.ping())

    # We need a path here, but TelemetryServicer currently takes Path.
    # If not provided in cfg, use internal default.
    from panoseti_grpc.telemetry.resources import get_config_path

    config_path = Path(cfg.telemetry_config_path) if cfg.telemetry_config_path else get_config_path()
    servicer = TelemetryServicer(config_path, r)
    return servicer, []


async def _make_daq_data_servicer(
    cfg: DaqDataServerConfig, shutdown_event: asyncio.Event
) -> tuple[Any, list[Coroutine[Any, Any, None]]]:
    if cfg.role == "gateway":
        from panoseti_grpc.daq_data.aggregator import DaqDataGatewayServicer

        gateway_servicer = DaqDataGatewayServicer(cfg)
        return gateway_servicer, [gateway_servicer.startup()]

    from panoseti_grpc.daq_data.server import DaqDataServicer

    edge_servicer = DaqDataServicer(cfg)
    return edge_servicer, [edge_servicer.start_initial_task()]


async def _make_daq_control_servicer(
    cfg: DaqControlServerConfig, shutdown_event: asyncio.Event
) -> tuple[Any, list[Coroutine[Any, Any, None]]]:
    from panoseti_grpc.daq_control.server import DaqControlServicer
    from panoseti_grpc.telemetry.logger import LoggerConfig

    # Convert string log level to int
    level = LoggerConfig.normalize_level(cfg.log_level)
    servicer = DaqControlServicer(level=level, hashpipe_path=cfg.hashpipe_path, hashpipe_name=cfg.hashpipe_name)
    return servicer, []


async def _make_ml_inference_servicer(
    cfg: MLInferenceServerConfig, shutdown_event: asyncio.Event
) -> tuple[Any, list[Coroutine[Any, Any, None]]]:
    from panoseti_grpc.ml_inference.server import MLInferenceServicer

    servicer = MLInferenceServicer(cfg)
    return servicer, []


# Register core services
ServiceRegistry.register(
    ServiceDescriptor(
        name="telemetry",
        servicer_factory=_make_telemetry_servicer,
        add_to_server_fn=telemetry_pb2_grpc.add_TelemetryServicer_to_server,
        service_names_for_reflection=[telemetry_pb2.DESCRIPTOR.services_by_name["Telemetry"].full_name],
        config_field="telemetry",
    )
)

ServiceRegistry.register(
    ServiceDescriptor(
        name="daq_data",
        servicer_factory=_make_daq_data_servicer,
        add_to_server_fn=daq_data_pb2_grpc.add_DaqDataServicer_to_server,
        service_names_for_reflection=[daq_data_pb2.DESCRIPTOR.services_by_name["DaqData"].full_name],
        config_field="daq_data",
    )
)

ServiceRegistry.register(
    ServiceDescriptor(
        name="daq_control",
        servicer_factory=_make_daq_control_servicer,
        add_to_server_fn=daq_control_pb2_grpc.add_DaqControlServicer_to_server,
        service_names_for_reflection=[daq_control_pb2.DESCRIPTOR.services_by_name["DaqControl"].full_name],
        config_field="daq_control",
    )
)

ServiceRegistry.register(
    ServiceDescriptor(
        name="ml_inference",
        servicer_factory=_make_ml_inference_servicer,
        add_to_server_fn=ml_inference_pb2_grpc.add_MLInferenceServicer_to_server,
        service_names_for_reflection=[ml_inference_pb2.DESCRIPTOR.services_by_name["MLInference"].full_name],
        config_field="ml_inference",
    )
)


# ---------------------------------------------------------------------------
# PanosetiServer
# ---------------------------------------------------------------------------

# Services are started in this order.
# ml_inference is last: it is a pure pub-sub broker with no dependencies on
# other services, but it emits predictions during/after daq_data streams.
INIT_ORDER = ["telemetry", "daq_data", "daq_control", "ml_inference"]


class PanosetiServer:
    """The unified gRPC server."""

    @staticmethod
    async def run(cfg: PanosetiServerConfig) -> None:
        """Instantiates and runs the unified gRPC server."""
        _logger.info(f"Starting PANOSETI Unified Server on port {cfg.port}")

        shutdown_event = asyncio.Event()

        def _handle_signal() -> None:
            _logger.info("Shutdown signal received.")
            shutdown_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)

        server = grpc.aio.server()

        active_servicers = []
        post_start_coros: list[Coroutine[Any, Any, None]] = []
        reflection_service_names = [reflection.SERVICE_NAME]

        # Instantiate enabled services
        for svc_name in INIT_ORDER:
            if not getattr(cfg.services, svc_name):
                continue

            descriptor = ServiceRegistry.get(svc_name)
            svc_cfg = getattr(cfg, descriptor.config_field)

            _logger.info(f"Initialising service: {svc_name}")
            servicer, coros = await descriptor.servicer_factory(svc_cfg, shutdown_event)

            descriptor.add_to_server_fn(servicer, server)
            active_servicers.append(servicer)
            post_start_coros.extend(coros)
            reflection_service_names.extend(descriptor.service_names_for_reflection)

        # Enable reflection
        reflection.enable_server_reflection(reflection_service_names, server)

        # Register standard gRPC health protocol (grpc.health.v1) if available.
        # Marks every active service as SERVING so that health probes and
        # `grpc_health_probe` work out of the box.  If grpcio-health-checking
        # is not installed the warning is logged but startup continues.
        try:
            from panoseti_grpc.grpc_utils.health import register_health

            proto_service_names = [n for n in reflection_service_names if n != reflection.SERVICE_NAME]
            health_toggle = register_health(server, proto_service_names)
            _logger.info("gRPC health checks registered for %d service(s).", len(proto_service_names))
            # Hand the toggle to each servicer so they can flip NOT_SERVING during
            # disruptive operations (writer-lock acquisition, hashpipe restart, etc.).
            for svc in active_servicers:
                if hasattr(svc, "health_toggle"):
                    svc.health_toggle = health_toggle
        except ImportError:
            _logger.warning(
                "grpcio-health-checking not installed; health probes disabled. "
                "Install with: pip install grpcio-health-checking"
            )

        # Bind port
        server.add_insecure_port(f"[::]:{cfg.port}")

        # Start server
        await server.start()
        _logger.info("gRPC server is live.")

        # Run any post-start background tasks
        background_tasks = [asyncio.create_task(c) for c in post_start_coros]

        # Wait for shutdown
        await shutdown_event.wait()

        # Stop sequence
        _logger.info(f"Stopping server (grace period {cfg.shutdown_grace_period}s)...")
        await server.stop(cfg.shutdown_grace_period)

        # Call shutdown on all servicers that implement it
        for s in active_servicers:
            if hasattr(s, "shutdown"):
                await s.shutdown()

        # Cancel background tasks
        for t in background_tasks:
            if not t.done():
                t.cancel()

        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

        _logger.info("Unified Server stopped.")
