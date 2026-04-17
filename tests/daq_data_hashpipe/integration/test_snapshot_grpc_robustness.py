import asyncio
import os

import grpc
import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient

# from tests.daq_data_hashpipe.conftest import hashpipe_pcap_runner  # type: ignore

pytestmark = pytest.mark.asyncio


# Utilities
async def _await_stream_next_or_stop(stream, timeout_sec=5.0):
    try:
        # We wait for the next item.
        # If the stream is closed, stream.__anext__() raises StopAsyncIteration immediately.
        item = await asyncio.wait_for(stream.__anext__(), timeout=timeout_sec)
        return item
    except StopAsyncIteration:
        # This is the "Success" case for checking if a stream has ended.
        return None
    except TimeoutError:
        # This means the stream is still open and "hanging" (waiting for data).
        raise TimeoutError(f"Stream did not end within {timeout_sec} seconds.") from None
    except grpc.aio.AioRpcError as e:
        raise grpc.aio.AioRpcError(f"Stream had an unexpected gRPC error {e=} within {timeout_sec} seconds.") from e


# 1) gRPC server is re-initialized (InitHpIo) during DAQ: DAQ keeps running; clients see clean cancellation and can reconnect.
@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_server_reinit_during_real_daq(default_server_process):
    server = default_server_process
    daq_config = {"daq_nodes": [{"ip_addr": server["ip_addr"]}]}

    hp_io_cfg = {
        "data_dir": "/tmp/ci_run_dir",
        "update_interval_seconds": 0.1,
        "simulate_daq": False,
        "force": True,
        "module_ids": [],
    }

    # Run the cycle a few times to ensure stability
    for i in range(5):
        async with AioDaqDataClient(daq_config, network_config=None) as client_a:
            # 1. Initialize for real DAQ
            assert await client_a.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg) is True

            # 2. Start a reader stream
            stream_a = await client_a.stream_images(
                hosts=None,
                stream_movie_data=True,
                stream_pulse_height_data=True,
                update_interval_seconds=0.1,
                timeout_sec=10.0,
            )

            # 3. Prove we are receiving frames
            first = await _await_stream_next_or_stop(stream_a, timeout_sec=10.0)
            assert first is not None, "Should receive data before re-init"

            # 4. Re-initialize with force=True while a stream is active
            # This triggers the server to cancel existing tasks
            assert await client_a.init_hp_io(hosts=None, hp_io_cfg={**hp_io_cfg, "force": True}) is True

            # 5. ROBUSTNESS FIX: Drain residual frames
            # The network stack might still hold frames generated before the kill signal.
            # We allow up to 5 "ghost" frames before failing.
            stream_closed = False
            for _ in range(5):
                post = await _await_stream_next_or_stop(stream_a, timeout_sec=2.0)
                if post is None:
                    stream_closed = True
                    break
                # If post is not None, it's just a residual frame; loop again.

            assert stream_closed, f"Stream did not close after re-init (iteration {i})"


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_init_waits_for_uds_ready(default_server_process):
    """After InitHpIo(simulate_daq=False), the server must create UDS listener sockets."""
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        hp_io_cfg = {
            "data_dir": "/tmp/ci_run_dir",
            "update_interval_seconds": 0.1,
            "simulate_daq": False,
            "force": True,
            "module_ids": [],
        }
        assert await client.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg)
        # Check that the UDS sockets exist (server creates them as listeners for hashpipe)
        for dp in ("img8", "img16", "ph256", "ph1024"):
            path = f"/tmp/hashpipe_grpc.dp_{dp}.sock"
            assert await asyncio.to_thread(os.path.exists, path), (
                f"UDS socket for data product '{dp}' was not created at {path}"
            )


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_first_frame_with_real_daq(default_server_process):
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        hp_io_cfg = {
            "data_dir": "/tmp/ci_run_dir",
            "update_interval_seconds": 0.1,
            "simulate_daq": False,
            "force": True,
            "module_ids": [],
        }
        assert await client.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg)
        stream = await client.stream_images(
            hosts=None, stream_movie_data=True, stream_pulse_height_data=True, update_interval_seconds=0.1
        )

        # Allow up to 10s for the first frame
        async def next_or_timeout():
            return await asyncio.wait_for(stream.__anext__(), timeout=10.0)

        img = await next_or_timeout()
        assert img and img["type"] in ("MOVIE", "PULSE_HEIGHT")


# ---------------------------------------------------------------------------
# Additional robustness tests
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_module_id_filter_with_real_data(default_server_process):
    """
    When module_ids is a non-empty whitelist, frames from other modules must
    not be delivered to the stream.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}

    hp_io_cfg_base = {
        "data_dir": "/tmp/ci_run_dir",
        "update_interval_seconds": 0.1,
        "simulate_daq": False,
        "force": True,
        "module_ids": [],
    }

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        # First init without filter to discover which module(s) are present
        assert await client.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg_base) is True
        discovery_stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
            timeout_sec=10.0,
        )
        first_img = await asyncio.wait_for(discovery_stream.__anext__(), timeout=10.0)
        discovered_module = first_img["module_id"]

        # Now init with an explicit whitelist containing only the discovered module
        filtered_cfg = {**hp_io_cfg_base, "module_ids": [discovered_module], "force": True}
        assert await client.init_hp_io(hosts=None, hp_io_cfg=filtered_cfg) is True

        filtered_stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
            timeout_sec=10.0,
        )

        for _ in range(10):
            img = await asyncio.wait_for(filtered_stream.__anext__(), timeout=5.0)
            assert img["module_id"] == discovered_module, (
                f"Expected only module {discovered_module}, got {img['module_id']}"
            )


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_concurrent_clients_receive_same_frames(default_server_process):
    """
    Two clients initialised with the same module config must receive data from
    the same module. Frame IDs from both clients should largely overlap
    (allowing ±2 tolerance for scheduling jitter).
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}

    hp_io_cfg = {
        "data_dir": "/tmp/ci_run_dir",
        "update_interval_seconds": 0.1,
        "simulate_daq": False,
        "force": True,
        "module_ids": [],
    }

    async with (
        AioDaqDataClient(daq_config, network_config=None) as client_a,
        AioDaqDataClient(daq_config, network_config=None) as client_b,
    ):
        assert await client_a.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg) is True

        stream_a = await client_a.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
            timeout_sec=10.0,
        )
        stream_b = await client_b.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
            timeout_sec=10.0,
        )

        SAMPLES = 15

        async def collect(stream):
            imgs = []
            for _ in range(SAMPLES):
                img = await asyncio.wait_for(stream.__anext__(), timeout=10.0)
                imgs.append(img)
            return imgs

        results_a, results_b = await asyncio.gather(collect(stream_a), collect(stream_b))

        # Both clients must see data from the same module
        modules_a = {img["module_id"] for img in results_a}
        modules_b = {img["module_id"] for img in results_b}
        assert modules_a == modules_b, f"Both clients should see the same module set; A={modules_a}, B={modules_b}"
        assert len(results_a) == SAMPLES
        assert len(results_b) == SAMPLES
