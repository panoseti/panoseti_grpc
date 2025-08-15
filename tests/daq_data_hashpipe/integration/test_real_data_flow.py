# In tests/daq_data/test_real_data_flow.py

import pytest
import asyncio
from daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_grpc_server_streams_real_hashpipe_data(default_server_process):
    """
    Validates the full data pipeline: tcpreplay -> hashpipe -> UDS -> gRPC Server -> Client.

    Args:
        hashpipe_pcap_runner: Fixture that manages the hashpipe and tcpreplay processes.
        default_server_process: Fixture that starts the DaqData gRPC server.
    """
    server_details = default_server_process
    daq_config = {"daq_nodes": [{"ip_addr": server_details["ip_addr"]}]}

    # Configuration for initializing the server in non-simulation mode
    hp_io_cfg = {
        "data_dir": "/tmp/ci_run_dir",
        "update_interval_seconds": 0.1,
        "simulate_daq": False,  # Critical: we are testing the real data path
        "force": True,
        "module_ids": [],
    }

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        # 1. Initialize the server to use its UdsDataSource
        init_success = await client.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg)
        assert init_success, "Failed to initialize server for real data mode."

        # 2. Subscribe to the image streams
        stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1
        )

        # 3. Collect 10 frames to confirm the pipeline is working
        received_frames = 0
        async for _ in stream:
            received_frames += 1
            print(f"Received frame #{received_frames}")
            if received_frames >= 10:
                break

        assert received_frames >= 10, "Did not receive the expected number of frames."
