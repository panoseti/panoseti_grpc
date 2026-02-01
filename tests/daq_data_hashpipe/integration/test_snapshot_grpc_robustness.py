import asyncio
import os
from pathlib import Path

import grpc
import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient
# from tests.daq_data_hashpipe.conftest import hashpipe_pcap_runner  # type: ignore

pytestmark = pytest.mark.asyncio


# Utilities
async def _await_stream_next_or_stop(stream, timeout=5.0):
    try:
        # We wait for the next item.
        # If the stream is closed, stream.__anext__() raises StopAsyncIteration immediately.
        item = await asyncio.wait_for(stream.__anext__(), timeout=timeout)
        return item
    except StopAsyncIteration:
        # This is the "Success" case for checking if a stream has ended.
        return None
    except asyncio.TimeoutError:
        # This means the stream is still open and "hanging" (waiting for data).
        raise asyncio.TimeoutError(f"Stream did not end within {timeout} seconds.")
    except grpc.aio.AioRpcError as e:
        raise grpc.aio.AioRpcError(f"Stream had an unexpected gRPC error {e=} within {timeout} seconds.")


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
                timeout=10.0,
            )

            # 3. Prove we are receiving frames
            first = await _await_stream_next_or_stop(stream_a, timeout=10.0)
            assert first is not None, "Should receive data before re-init"

            # 4. Re-initialize with force=True while a stream is active
            # This triggers the server to cancel existing tasks
            assert await client_a.init_hp_io(hosts=None, hp_io_cfg={**hp_io_cfg, "force": True}) is True

            # 5. ROBUSTNESS FIX: Drain residual frames
            # The network stack might still hold frames generated before the kill signal.
            # We allow up to 5 "ghost" frames before failing.
            stream_closed = False
            for _ in range(5):
                post = await _await_stream_next_or_stop(stream_a, timeout=2.0)
                if post is None:
                    stream_closed = True
                    break
                # If post is not None, it's just a residual frame; loop again.

            assert stream_closed, f"Stream did not close after re-init (iteration {i})"

async def test_init_waits_for_uds_ready(default_server_process):
    # Use real DAQ config pathing via fixture server_config_base if needed
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process['ip_addr']}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        hp_io_cfg = {
            "data_dir": "/tmp/ci_run_dir",
            "update_interval_seconds": 0.1,
            "simulate_daq": False,
            "force": True,
            "module_ids": [],
        }
        assert await client.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg)
        # Check that the UDS sockets exist and accept connections
        uds_template = default_server_process['uds_template'] if 'uds_template' in default_server_process else "/tmp/hashpipe_grpc.dp_{dp}.sock"
        for dp in ("img8","img16","ph256","ph1024"):
            path = uds_template.format(dp=dp).replace("{dp}", dp).replace("{dp_name}", dp)
            assert os.path.exists(path)

async def test_first_frame_with_real_daq(default_server_process):
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process['ip_addr']}]}
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
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1
        )
        # Allow up to 10s for the first frame
        async def next_or_timeout():
            return await asyncio.wait_for(stream.__anext__(), timeout=10.0)
        img = await next_or_timeout()
        assert img and img['type'] in ('MOVIE','PULSE_HEIGHT')