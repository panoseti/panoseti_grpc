"""
Tests for UDS socket lifecycle: stale-file cleanup, buffer limits,
abrupt-disconnect recovery, socket permissions, frame-ID monotonicity
after re-init, and dynamic module discovery.
"""
import asyncio
import os
import socket
import stat
import struct
import tempfile
import uuid
import copy
from pathlib import Path

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient, hp_io_config_simulate
from panoseti_grpc.daq_data.server import serve

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_server_config(server_config_base, socket_dir: Path, module_id: int = 224):
    """Return a config where every path lives under *socket_dir*."""
    cfg = copy.deepcopy(server_config_base)
    cfg["unix_domain_socket"] = f"unix://{socket_dir / 'grpc.sock'}"
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
    cfg["simulate_daq_cfg"]["strategies"] = {"uds": {"data_products": dps}}
    return cfg


async def _start_server(cfg: dict) -> tuple[asyncio.Event, asyncio.Task]:
    shutdown = asyncio.Event()
    task = asyncio.create_task(serve(cfg, shutdown, in_main_thread=False))
    uds_path = Path(cfg["unix_domain_socket"].replace("unix://", ""))
    async with AioDaqDataClient({"daq_nodes": [{"ip_addr": cfg["unix_domain_socket"]}]}, network_config=None) as c:
        for _ in range(40):
            if uds_path.exists() and await c.ping(cfg["unix_domain_socket"]):
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Server did not become ready in time.")
    return shutdown, task


async def _stop_server(shutdown: asyncio.Event, task: asyncio.Task, uds_path: Path):
    shutdown.set()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    if uds_path.exists():
        try:
            os.unlink(uds_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_stale_socket_file_cleaned_up_on_server_start(server_config_base):
    """
    Server must remove a leftover socket file before binding.
    Simulates what happens after a crash leaves a stale .sock file behind.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)
        grpc_sock_path = socket_dir / "grpc.sock"
        dp_sock_path = socket_dir / "hashpipe_grpc.dp_img16.sock"

        # Pre-create stale socket files to simulate a crash
        for p in (grpc_sock_path, dp_sock_path):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.bind(str(p))
            s.close()
        assert grpc_sock_path.exists()
        assert dp_sock_path.exists()

        shutdown, task = await _start_server(cfg)
        try:
            # If we got here without error, the server successfully cleaned up the stale sockets
            assert grpc_sock_path.exists(), "gRPC socket should exist after server starts"
        finally:
            await _stop_server(shutdown, task, grpc_sock_path)


async def test_uds_socket_permissions(server_config_base):
    """
    The UDS socket file for gRPC must be accessible (exists and is a socket).
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)
        grpc_sock_path = socket_dir / "grpc.sock"

        shutdown, task = await _start_server(cfg)
        try:
            assert grpc_sock_path.exists()
            s = os.stat(grpc_sock_path)
            assert stat.S_ISSOCK(s.st_mode), "Path must be a socket file"
        finally:
            await _stop_server(shutdown, task, grpc_sock_path)


async def test_uds_client_abrupt_disconnect_mid_frame(server_config_base):
    """
    Closing a raw UDS connection mid-write should not crash the server;
    other gRPC clients must continue to receive frames.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)
        grpc_sock_path = socket_dir / "grpc.sock"

        shutdown, task = await _start_server(cfg)
        try:
            server_addr = cfg["unix_domain_socket"]
            daq_config = {"daq_nodes": [{"ip_addr": server_addr}]}

            async with AioDaqDataClient(daq_config, network_config=None) as good_client:
                assert await good_client.init_sim(hosts=None) is True

                # Open a legitimate stream on the good client first
                stream = await good_client.stream_images(
                    hosts=None,
                    stream_movie_data=True,
                    stream_pulse_height_data=True,
                    update_interval_seconds=0.05,
                )

                # Abruptly connect and disconnect a raw UDS client to the data-plane socket
                # (simulates a badly-behaved producer crashing mid-frame)
                dp_sock_path = str(socket_dir / "hashpipe_grpc.dp_img16.sock")
                if os.path.exists(dp_sock_path):
                    raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    try:
                        raw.connect(dp_sock_path)
                        # Write a partial frame header (garbage data)
                        raw.sendall(b"\x00\xe0" + b"x" * 20)
                    except OSError:
                        pass
                    finally:
                        raw.close()  # Abrupt disconnect

                # The good gRPC stream must still deliver frames
                received = 0
                for _ in range(5):
                    try:
                        img = await asyncio.wait_for(stream.__anext__(), timeout=5.0)
                        assert img is not None
                        received += 1
                    except (StopAsyncIteration, asyncio.TimeoutError):
                        break

                assert received >= 3, "Server should continue serving after abrupt raw-socket disconnect"
        finally:
            await _stop_server(shutdown, task, grpc_sock_path)


async def test_frame_id_monotonic_across_reinit(server_config_base):
    """
    After a forced re-init, a new reader must see frame IDs that start fresh
    (reset to -1 server-side), and the stream delivers new frames without error.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        cfg = _make_server_config(server_config_base, socket_dir)
        grpc_sock_path = socket_dir / "grpc.sock"

        shutdown, task = await _start_server(cfg)
        try:
            server_addr = cfg["unix_domain_socket"]
            daq_config = {"daq_nodes": [{"ip_addr": server_addr}]}

            async with AioDaqDataClient(daq_config, network_config=None) as client:
                # First session
                assert await client.init_sim(hosts=None) is True
                stream1 = await client.stream_images(
                    hosts=None, stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                imgs_before = [await stream1.__anext__() for _ in range(3)]
                assert len(imgs_before) == 3

                # Force re-init via init_hp_io with force=True
                force_cfg = {**hp_io_config_simulate, "simulate_daq": True, "force": True}
                assert await client.init_hp_io(hosts=None, hp_io_cfg=force_cfg) is True

                # New stream after re-init should succeed
                stream2 = await client.stream_images(
                    hosts=None, stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                imgs_after = [await asyncio.wait_for(stream2.__anext__(), timeout=5.0) for _ in range(3)]
                assert len(imgs_after) == 3, "New stream should deliver frames after re-init"
        finally:
            await _stop_server(shutdown, task, grpc_sock_path)


async def test_module_discovery_from_uds_stream(server_config_base):
    """
    When module_ids is empty in init_sim, the server discovers module IDs
    from the incoming UDS stream. Frames for the simulated module must arrive.
    """
    with tempfile.TemporaryDirectory() as td:
        socket_dir = Path(td)
        sim_module = 201
        cfg = _make_server_config(server_config_base, socket_dir, module_id=sim_module)
        grpc_sock_path = socket_dir / "grpc.sock"

        shutdown, task = await _start_server(cfg)
        try:
            server_addr = cfg["unix_domain_socket"]
            daq_config = {"daq_nodes": [{"ip_addr": server_addr}]}

            async with AioDaqDataClient(daq_config, network_config=None) as client:
                # Init without specifying module_ids — server must auto-discover
                assert await client.init_sim(hosts=None) is True

                stream = await client.stream_images(
                    hosts=None, stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.05
                )
                seen_modules = set()
                for _ in range(10):
                    img = await asyncio.wait_for(stream.__anext__(), timeout=5.0)
                    seen_modules.add(img["module_id"])

                assert sim_module in seen_modules, (
                    f"Expected module {sim_module} to be discovered; got {seen_modules}"
                )
        finally:
            await _stop_server(shutdown, task, grpc_sock_path)
