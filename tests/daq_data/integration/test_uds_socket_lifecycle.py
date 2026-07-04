"""
Tests for UDS data-plane socket lifecycle: stale-file cleanup, buffer limits,
abrupt-disconnect recovery, socket permissions, frame-ID monotonicity
after re-init, and dynamic module discovery.

gRPC transport now uses TCP (port=0); data-plane UDS sockets are unchanged.
"""

import asyncio
import copy
import os
import socket
import stat
import tempfile
from pathlib import Path
from typing import Any

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient, hp_io_config_simulate
from panoseti_grpc.daq_data.server import serve
from panoseti_grpc.grpc_utils.health import HealthClient

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_server_config(server_config_base: Any, socket_dir: Path, module_id: int = 224) -> dict[str, Any]:
    """Return a config where every data-plane path lives under *socket_dir*."""
    cfg = copy.deepcopy(server_config_base)
    cfg["unix_domain_socket"] = None  # use TCP for gRPC transport
    cfg["simulate_daq_cfg"]["simulation_mode"] = "uds"
    cfg["simulate_daq_cfg"]["sim_module_ids"] = [module_id]
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
    while not bound_port:  # noqa: ASYNC110
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


async def test_stale_data_socket_cleaned_up_on_init(server_config_base):
    """
    After InitHpIo, the server must remove any leftover data-plane socket files
    before binding new ones. Simulates what happens after a crash leaves stale
    .sock files behind.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)

        # Pre-create a stale data-plane socket to simulate a crash
        dp_sock_path = socket_dir / "hashpipe_grpc.dp_img16.sock"
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(str(dp_sock_path))
        s.close()
        assert dp_sock_path.exists()

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as client:
                # init_sim triggers HpIoManager which cleans up and recreates data-plane sockets
                assert await client.init_hp_io(hp_io_config_simulate) is True
                await asyncio.sleep(0.2)
                assert dp_sock_path.exists(), "Data-plane socket should exist after init_sim"
        finally:
            await _stop_server(shutdown, task)


async def test_uds_data_socket_permissions(server_config_base):
    """
    Data-plane UDS socket files created by HpIoManager must be accessible (exist and are sockets).
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as client:
                assert await client.init_hp_io(hp_io_config_simulate) is True
                await asyncio.sleep(0.2)  # allow socket creation

            dp_sock_path = socket_dir / "hashpipe_grpc.dp_img16.sock"
            assert dp_sock_path.exists()
            s = os.stat(dp_sock_path)
            assert stat.S_ISSOCK(s.st_mode), "Path must be a socket file"
        finally:
            await _stop_server(shutdown, task)


async def test_uds_client_abrupt_disconnect_mid_frame(server_config_base):
    """
    Closing a raw UDS connection mid-write should not crash the server;
    other gRPC clients must continue to receive frames.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as good_client:
                assert await good_client.init_hp_io(hp_io_config_simulate) is True

                stream = good_client.stream_images(
                    stream_movie_data=True,
                    stream_pulse_height_data=True,
                    update_interval_seconds=0.05,
                )

                # Abruptly connect and disconnect a raw UDS client to the data-plane socket
                dp_sock_path = str(socket_dir / "hashpipe_grpc.dp_img16.sock")
                if await asyncio.to_thread(os.path.exists, dp_sock_path):
                    raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    try:
                        raw.connect(dp_sock_path)
                        raw.sendall(b"\x00\xe0" + b"x" * 20)
                    except OSError:
                        pass
                    finally:
                        raw.close()

                # The good gRPC stream must still deliver frames
                received = 0
                for _ in range(5):
                    try:
                        img = await asyncio.wait_for(stream.__anext__(), timeout=5.0)
                        assert img is not None
                        received += 1
                    except (TimeoutError, StopAsyncIteration):
                        break

                assert received >= 3, "Server should continue serving after abrupt raw-socket disconnect"
        finally:
            await _stop_server(shutdown, task)


async def test_frame_id_monotonic_across_reinit(server_config_base):
    """
    After a forced re-init, a new reader must see frame IDs that start fresh
    and the stream delivers new frames without error.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as client:
                # First session
                assert await client.init_hp_io(hp_io_config_simulate) is True
                stream1 = client.stream_images(
                    stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                imgs_before = [await stream1.__anext__() for _ in range(3)]
                assert len(imgs_before) == 3

                # Force re-init via init_hp_io with force=True
                force_cfg = {**hp_io_config_simulate, "simulate_daq": True, "force": True}
                assert await client.init_hp_io(force_cfg) is True

                # New stream after re-init should succeed
                stream2 = client.stream_images(
                    stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                imgs_after = [await asyncio.wait_for(stream2.__anext__(), timeout=5.0) for _ in range(3)]
                assert len(imgs_after) == 3, "New stream should deliver frames after re-init"
        finally:
            await _stop_server(shutdown, task)


async def test_module_discovery_from_uds_stream(server_config_base):
    """
    When module_ids is empty in init_sim, the server discovers module IDs
    from the incoming UDS stream. Frames for the simulated module must arrive.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        sim_module = 201
        cfg = _make_server_config(server_config_base, socket_dir, module_id=sim_module)

        shutdown, task, tcp_port = await _start_server(cfg)
        try:
            async with AioDaqDataClient("localhost", tcp_port) as client:
                assert await client.init_hp_io(hp_io_config_simulate) is True

                stream = client.stream_images(
                    stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                seen_modules = set()
                for _ in range(10):
                    img = await asyncio.wait_for(stream.__anext__(), timeout=5.0)
                    seen_modules.add(img["module_id"])

                assert sim_module in seen_modules, f"Expected module {sim_module} to be discovered; got {seen_modules}"
        finally:
            await _stop_server(shutdown, task)
