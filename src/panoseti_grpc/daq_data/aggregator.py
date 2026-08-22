"""
DaqData aggregator gateway servicer.

Runs on the headnode and fans in ``StreamImages`` calls from N edge DAQ nodes
into a single stream for each downstream consumer.  Edge connectivity, port
forwarding, and data-dir overrides are handled server-side; consumers need only
the single ``host:port`` of the headnode.

Role selection is driven by ``DaqDataServerConfig.role``:

  - ``"edge"`` (default) — runs the standard :class:`DaqDataServicer`.
  - ``"gateway"`` — runs this :class:`DaqDataGatewayServicer`.

The gateway reads ``daq_config_path`` / ``network_config_path`` from its config
section, falling back to the ``PSETI_GRPC_DATA_CONFIG`` / ``PSETI_GRPC_NETWORK_CONFIG``
environment variables when a field is left unset in the TOML (see
:func:`_resolve_gateway_path`) so a fleet-wide value can be provided without
hand-editing every node's ``server_gateway.toml``. It opens one persistent gRPC
channel per edge node at startup, and keeps those channels alive for the
lifetime of the process.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from typing import Any

import grpc
import grpc.aio
from google.protobuf.empty_pb2 import Empty

from panoseti_grpc.generated import daq_data_pb2, daq_data_pb2_grpc
from panoseti_grpc.generated.daq_data_pb2 import (
    InitHpIoResponse,
    StatusResponse,
    StreamImagesResponse,
)
from panoseti_grpc.grpc_utils.channel import AsyncChannelManager, keepalive_options
from panoseti_grpc.grpc_utils.health import HealthToggle
from panoseti_grpc.grpc_utils.retries import build_retry_service_config
from panoseti_grpc.telemetry.logger import get_logger

from .config import DaqDataServerConfig


def _load_json(path: str) -> dict[str, Any]:
    import json

    with open(path) as f:
        return json.load(f)  # type: ignore[no-any-return]


def _resolve_gateway_path(
    logger: logging.Logger, field_name: str, cfg_value: str | None, env_var: str
) -> str | None:
    """Resolve a ``[daq_data.gateway]`` path: config file value, then env var fallback.

    Precedence: the TOML config value always wins when set (so operators don't
    have to touch the environment to override it); when it's empty, fall back
    to ``env_var`` so a fleet-wide value can be provided without hand-editing
    every node's ``server_gateway.toml``. Logs where the value came from, or a
    warning if neither source has it.
    """
    if cfg_value:
        return cfg_value
    env_value = os.getenv(env_var)
    if env_value:
        logger.info(
            f"[daq_data.gateway] {field_name} not set in config; "
            f"using {env_var}={env_value!r} from the environment."
        )
        return env_value
    logger.warning(
        f"[daq_data.gateway] {field_name} is not set in the server config, "
        f"and {env_var} is not set in the environment either."
    )
    return None


class DaqDataGatewayServicer(daq_data_pb2_grpc.DaqDataServicer):
    """DaqData gateway: fans in streams from N edge nodes into one consumer endpoint.

    Startup sequence (called by the unified server factory):
      1. ``_make_daq_data_servicer`` creates this object.
      2. ``startup()`` is posted as a background coroutine.
      3. ``startup()`` reads daq_config, opens channels to every edge node.

    Shutdown:
      - ``shutdown()`` closes all edge channels gracefully.
    """

    health_toggle: HealthToggle | None = None
    _HEALTH_SERVICE_NAME = "daqdata.DaqData"

    def __init__(self, cfg: DaqDataServerConfig, log_level: int = logging.INFO) -> None:
        self.cfg = cfg
        self.logger = get_logger("daq_data.gateway", level=log_level)
        self._channels: list[AsyncChannelManager] = []
        # target → stub; populated in startup()
        self._edge_stubs: dict[str, daq_data_pb2_grpc.DaqDataStub] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def startup(self) -> None:
        """Open channels to all configured edge nodes.

        Errors are logged and swallowed so that a misconfigured gateway still
        starts.  Callers can check ``len(self._edge_stubs) > 0`` to confirm
        edges were reached.
        """
        gw_cfg = self.cfg.gateway
        daq_config_path = _resolve_gateway_path(
            self.logger, "daq_config_path", gw_cfg.daq_config_path, "PSETI_GRPC_DATA_CONFIG"
        )
        if not daq_config_path:
            self.logger.warning("Gateway has no daq_config_path resolved — no edge nodes will be contacted.")
            return

        try:
            raw_daq: dict[str, Any] = await asyncio.to_thread(_load_json, daq_config_path)
        except OSError as exc:
            self.logger.error(f"Cannot read daq_config_path={daq_config_path!r}: {exc}")
            return

        try:
            from .client_models import DaqConfig, NetworkConfig

            daq_cfg = DaqConfig.model_validate(raw_daq)

            network_config_path = _resolve_gateway_path(
                self.logger, "network_config_path", gw_cfg.network_config_path, "PSETI_GRPC_NETWORK_CONFIG"
            )
            network_cfg = None
            if network_config_path:
                try:
                    raw_net: dict[str, Any] = await asyncio.to_thread(_load_json, network_config_path)
                    network_cfg = NetworkConfig.model_validate(raw_net)
                except OSError as exc:
                    self.logger.warning(
                        f"Cannot read network_config_path={network_config_path!r}: {exc}. "
                        "Proceeding without port-forwarding overrides."
                    )

            for node in daq_cfg.daq_nodes:
                host = str(node.ip_addr)
                port = gw_cfg.edge_port

                # Apply port-forwarding override when provided
                if network_cfg:
                    for nnode in network_cfg.daq_nodes:
                        if str(nnode.ip_addr) == host and nnode.port_forwarding.status:
                            host = str(nnode.port_forwarding.gw_ip)
                            port = nnode.port_forwarding.grpc_port
                            break

                target = f"{host}:{port}"
                if target in self._edge_stubs:
                    self.logger.debug(f"Skipping duplicate edge target: {target}")
                    continue

                options = [*keepalive_options(), ("grpc.service_config", build_retry_service_config())]
                mgr = AsyncChannelManager(host, port, options=options)
                await mgr.__aenter__()
                self._channels.append(mgr)
                self._edge_stubs[target] = daq_data_pb2_grpc.DaqDataStub(mgr.channel)
                self.logger.info(f"Gateway opened channel to edge: {target}")

        except Exception as exc:
            self.logger.error(f"Gateway startup error: {exc}", exc_info=True)

        self.logger.info(f"Gateway ready — {len(self._edge_stubs)} edge node(s): {list(self._edge_stubs)}")

    async def shutdown(self) -> None:
        """Close all edge channels gracefully."""
        self.logger.info("Gateway shutting down — closing edge channels.")
        for mgr in self._channels:
            await mgr.__aexit__(None, None, None)
        self._channels.clear()
        self._edge_stubs.clear()

    # ------------------------------------------------------------------
    # RPC implementations
    # ------------------------------------------------------------------

    async def StreamImages(
        self,
        request: daq_data_pb2.StreamImagesRequest,
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[StreamImagesResponse]:
        """Fan-in StreamImages from all edges into a single consumer stream.

        Best-effort: a down or erroring edge is logged and excluded; remaining
        edges continue streaming.  The gateway stream ends when all edges have
        finished or the consumer disconnects.
        """
        if not self._edge_stubs:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "Gateway has no connected edge nodes. Check [daq_data.gateway] daq_config_path.",
            )
            return

        n_edges = len(self._edge_stubs)
        queue: asyncio.Queue[StreamImagesResponse | None] = asyncio.Queue(maxsize=200)

        async def _drain_edge(target: str, stub: daq_data_pb2_grpc.DaqDataStub) -> None:
            try:
                async for resp in stub.StreamImages(request):
                    if context.cancelled():
                        return
                    await queue.put(resp)
            except grpc.aio.AioRpcError as e:
                if e.code() != grpc.StatusCode.CANCELLED:
                    self.logger.warning(f"Edge {target} stream ended: [{e.code().name}] {e.details()}")
            except Exception as e:
                self.logger.error(f"Edge {target} unexpected stream error: {e}")
            finally:
                await queue.put(None)  # sentinel: this edge is done

        tasks = [asyncio.create_task(_drain_edge(target, stub)) for target, stub in self._edge_stubs.items()]
        self.logger.info(f"Gateway StreamImages: fanning in from {n_edges} edge(s).")

        try:
            finished = 0
            while finished < n_edges and not context.cancelled():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=1.0)
                except TimeoutError:
                    continue
                if item is None:
                    finished += 1
                else:
                    yield item
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                    self.logger.error("Edge drain task error during cleanup: %s", r)

        self.logger.info("Gateway StreamImages ended.")

    async def InitHpIo(
        self, request: daq_data_pb2.InitHpIoRequest, context: grpc.aio.ServicerContext
    ) -> InitHpIoResponse:
        """Proxy InitHpIo to all edges concurrently (best-effort outcome-collection)."""
        if not self._edge_stubs:
            return InitHpIoResponse(success=False, error_message="Gateway has no connected edge nodes.")

        async def _init_one(target: str, stub: daq_data_pb2_grpc.DaqDataStub) -> tuple[str, bool, str]:
            try:
                resp = await stub.InitHpIo(request, timeout=15.0)
                return target, bool(resp.success), resp.error_message
            except Exception as exc:
                return target, False, str(exc)

        async with asyncio.TaskGroup() as tg:
            tasks: dict[str, asyncio.Task[Any]] = {
                target: tg.create_task(_init_one(target, stub)) for target, stub in self._edge_stubs.items()
            }

        outcomes = [tasks[t].result() for t in tasks]
        all_ok = all(ok for _, ok, _ in outcomes)
        errors = [f"{t}: {msg}" for t, ok, msg in outcomes if not ok]
        self.logger.info(f"Gateway InitHpIo: {len(outcomes)} edges, all_ok={all_ok}")
        return InitHpIoResponse(
            success=all_ok,
            error_message="; ".join(errors) if errors else "",
        )

    async def Status(self, request: Empty, context: grpc.aio.ServicerContext) -> StatusResponse:
        """Aggregate Status from all edges."""
        if not self._edge_stubs:
            return StatusResponse(hp_io_initialized=False, message="Gateway has no connected edge nodes.")

        async def _status_one(target: str, stub: daq_data_pb2_grpc.DaqDataStub) -> tuple[str, bool, str]:
            try:
                resp = await stub.Status(Empty(), timeout=5.0)
                return target, bool(resp.hp_io_initialized), ""
            except Exception as exc:
                return target, False, str(exc)

        async with asyncio.TaskGroup() as tg:
            tasks: dict[str, asyncio.Task[Any]] = {
                target: tg.create_task(_status_one(target, stub)) for target, stub in self._edge_stubs.items()
            }

        outcomes = [tasks[t].result() for t in tasks]
        all_init = all(ok for _, ok, _ in outcomes)
        parts = [f"{t}: initialized={ok}" + (f" (err: {e})" if e else "") for t, ok, e in outcomes]
        return StatusResponse(hp_io_initialized=all_init, message="; ".join(parts))

    async def Ping(self, request: Empty, context: grpc.aio.ServicerContext) -> Empty:
        """Return Empty. Prefer grpc.health.v1 health checks over Ping."""
        return Empty()

    async def UploadImages(
        self,
        request_iterator: Any,
        context: grpc.aio.ServicerContext,
    ) -> Empty:
        """Placeholder — not yet implemented on the gateway."""
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "UploadImages is not implemented on the gateway.")
        return Empty()
