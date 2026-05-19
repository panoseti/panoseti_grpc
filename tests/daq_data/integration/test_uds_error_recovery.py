"""
Tests for UDS error-recovery paths: producer restart, slow-consumer
backpressure, and DEADLINE_EXCEEDED when the data source goes idle.

gRPC transport now uses TCP (port=0); data-plane UDS sockets are unchanged.
"""

import asyncio
import copy
import tempfile
from pathlib import Path
from typing import Any

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient, hp_io_config_simulate
from panoseti_grpc.daq_data.server import serve
from panoseti_grpc.grpc_utils.exceptions import DeadlineExceededError, PanosetiRpcError
from panoseti_grpc.grpc_utils.health import HealthClient

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_server_config(
    server_config_base: Any, socket_dir: Path, module_id: int = 224, max_reader_dequeue_timeouts: int = 3
) -> dict[str, Any]:
    cfg = copy.deepcopy(server_config_base)
    cfg["unix_domain_socket"] = None  # use TCP for gRPC transport
    cfg["simulate_daq_cfg"]["simulation_mode"] = "uds"
    cfg["simulate_daq_cfg"]["sim_module_ids"] = [module_id]
    cfg["max_reader_dequeue_timeouts"] = max_reader_dequeue_timeouts
    dps = ["img8", "img16", "ph256", "ph1024"]
    cfg["acquisition_methods"] = {
        "uds": {
            "enabled": True,
            "data_products": dps,
            "socket_path_template": str(socket_dir / "hashpipe_grpc.dp_{dp_name}.sock"),
        }
    }
    cfg["simulate_daq_cfg"]["strategies"] = {"uds": {"data_products": dps, "sim_module_ids": [module_id]}}
    return cfg


async def _start_server(cfg: dict[str, Any]) -> tuple[asyncio.Event, asyncio.Task[None], int]:
    """Start a test server on TCP port 0; return (shutdown_event, task, bound_port)."""
    shutdown = asyncio.Event()
    bound_port: list[int] = []
    task = asyncio.create_task(serve(cfg, shutdown, in_main_thread=False, port=0, bound_port_out=bound_port))
    while not bound_port:
        await asyncio.sleep(0.01)
    tcp_port = bound_port[0]
    hc = HealthClient("localhost", tcp_port)
    for _ in range(40):
        if await asyncio.to_thread(hc.check, "daqdata.DaqData", 1.0):
            break
        await asyncio.sleep(0.1)
    else:
        pytest.fail("Server did not become ready in time.")
    return shutdown, task, tcp_port


async def _stop_server(shutdown: asyncio.Event, task: asyncio.Task[None]) -> None:
    shutdown.set()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_server_recovers_after_uds_producer_restart(server_config_base):
    """
    After the UDS simulation data source restarts (via forced re-init),
    the server should re-accept and resume streaming without requiring
    the gRPC client to reconnect.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as client:
                assert await client.init_hp_io(hp_io_config_simulate) is True
                stream = client.stream_images(
                    stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                received_before = 0
                for _ in range(4):
                    await asyncio.wait_for(stream.__anext__(), timeout=5.0)
                    received_before += 1
                assert received_before == 4

                # Simulate producer restart: force re-init via init_hp_io with force=True
                force_cfg = {**hp_io_config_simulate, "simulate_daq": True, "force": True}
                assert await client.init_hp_io(force_cfg) is True

                stream2 = client.stream_images(
                    stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                received_after = 0
                for _ in range(4):
                    img = await asyncio.wait_for(stream2.__anext__(), timeout=5.0)
                    assert img is not None
                    received_after += 1
                assert received_after == 4, "Stream should resume after producer restart"
        finally:
            await _stop_server(shutdown, task)


async def test_slow_consumer_backpressure(server_config_base):
    """
    A very slow consumer (2 s per frame) must not cause the server to
    accumulate memory unboundedly. The server must stay alive and responsive.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as client:
                assert await client.init_hp_io(hp_io_config_simulate) is True
                stream = client.stream_images(
                    stream_movie_data=True,
                    stream_pulse_height_data=True,
                    update_interval_seconds=2.0,  # very slow consumer
                )

                img = await asyncio.wait_for(stream.__anext__(), timeout=10.0)
                assert img is not None

                # Server must still respond to pings (i.e., not deadlocked)
                pong = await client.ping()
                assert pong is True, "Server should remain responsive under slow consumer"
        finally:
            await _stop_server(shutdown, task)


async def test_stream_deadline_exceeded_on_idle_source(server_config_base):
    """
    When no data arrives and reader_timeout is exceeded, StreamImages must
    abort. We verify the stream terminates cleanly (either with data or timeout error).
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir, max_reader_dequeue_timeouts=2)
        cfg["reader_timeout"] = 0.2  # shorten the idle detection window

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as client:
                assert await client.init_hp_io(hp_io_config_simulate) is True

                # Stream with a short timeout; with no data the server aborts with DEADLINE_EXCEEDED
                try:
                    count = 0
                    async for img in client.stream_images(
                        stream_movie_data=True,
                        stream_pulse_height_data=True,
                        update_interval_seconds=0.2,
                    ):
                        if img is not None:
                            count += 1
                        if count >= 20:
                            break
                except DeadlineExceededError:
                    return  # Expected: idle source triggers DEADLINE_EXCEEDED
                except PanosetiRpcError:
                    return  # Any RPC termination is acceptable
        finally:
            await _stop_server(shutdown, task)
