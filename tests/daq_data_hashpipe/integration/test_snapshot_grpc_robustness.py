import asyncio
import os
from pathlib import Path

import grpc
import pytest

from daq_data.client import AioDaqDataClient
# from tests.daq_data_hashpipe.conftest import hashpipe_pcap_runner  # type: ignore

pytestmark = pytest.mark.asyncio


# Utilities
async def _await_stream_next_or_stop(stream, timeout=3.0):
    try:
        return await asyncio.wait_for(stream.__anext__(), timeout=timeout)
    except (StopAsyncIteration, grpc.aio.AioRpcError, asyncio.TimeoutError):
        return None


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
    for i in range(5):
        async with AioDaqDataClient(daq_config, network_config=None) as client_a:
            # Initialize for real DAQ
            assert await client_a.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg) is True

            # Start a reader stream
            stream_a = await client_a.stream_images(
                hosts=None,
                stream_movie_data=True,
                stream_pulse_height_data=True,
                update_interval_seconds=0.1,
                timeout=15.0,
            )
            # Prove we are receiving frames
            first = await _await_stream_next_or_stop(stream_a, timeout=5.0)
            assert first is not None, "Should receive data before re-init"

            # Re-initialize with force=True while a stream is active
            assert await client_a.init_hp_io(hosts=None, hp_io_cfg={**hp_io_cfg, "force": True}) is True

            # After forced re-init, existing stream should end
            post = await _await_stream_next_or_stop(stream_a, timeout=3.0)
            assert post is None, "Stream should end cleanly after forced re-init"

            # Start a new stream to verify post-reinit pipeline works
            stream_b = await client_a.stream_images(
                hosts=None,
                stream_movie_data=True,
                stream_pulse_height_data=True,
                update_interval_seconds=0.1,
                timeout=15.0,
            )
            again = await _await_stream_next_or_stop(stream_b, timeout=5.0)
            assert again is not None, "Should receive data after re-init"

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