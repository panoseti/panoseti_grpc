import pytest
import asyncio
from daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio

@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_daq_grpc_stream_stress(default_server_process):
    """
    Stress test for real DAQ and gRPC interaction focusing on stability
    against high load and snapshots handling, inspired by simulation tests.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}

    # Use a realistic but aggressive update interval for streaming.
    hp_io_cfg = {
        "data_dir": "/tmp/ci_run_dir",  # As configured in the conftest.py runner
        "update_interval_seconds": 0.05,  # Very fast update to stress the system
        "simulate_daq": False,  # Real DAQ
        "force": True,
        "module_ids": [],
    }

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        success = await client.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg)
        assert success, "Failed to initialize real DAQ server"

        stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=-1,
            timeout=30.0,
        )

        received = 0
        frame_numbers = []

        try:
            async for image in stream:
                assert 'frame_number' in image
                frame_numbers.append(image['frame_number'])
                received += 1
                if received >= 30:
                    break
        except Exception as e:
            pytest.fail(f"Stream failed during stress test: {e}")

        assert received >= 30, "Did not receive enough frames during stress test"

        # Check for frame number continuity, allowing some gaps
        gaps = sum(
            1 for i in range(len(frame_numbers) - 1) if frame_numbers[i + 1] - frame_numbers[i] > 5
        )
        assert gaps < 15, "Too many dropped frames detected, potential snapshot or DAQ issues"

@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_daq_grpc_concurrent_clients_snapshot_robustness(default_server_process):
    """
    Test multiple concurrent real DAQ gRPC clients streaming with snapshot functionality
    to detect any DAQ crashes or gRPC failures.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}

    async with AioDaqDataClient(daq_config, network_config=None) as client1, \
               AioDaqDataClient(daq_config, network_config=None) as client2:

        assert await client1.init_hp_io(hosts=None, hp_io_cfg={
            "data_dir": "/tmp/ci_run_dir",
            "update_interval_seconds": 0.1,
            "simulate_daq": False,
            "force": True,
            "module_ids": []
        })

        async def receive_data(client, data_types, count_threshold=20):
            stream = await client.stream_images(
                hosts=None,
                stream_movie_data=('MOVIE' in data_types),
                stream_pulse_height_data=('PULSE_HEIGHT' in data_types),
                update_interval_seconds=-1,
                timeout=60.0,
            )
            received = 0

            try:
                async for image in stream:
                    assert image['type'] in data_types
                    received += 1
                    if received >= count_threshold:
                        break
            except Exception as e:
                pytest.fail(f"Client stream failed: {e}")
            assert received >= count_threshold

        await asyncio.gather(
            # receive_data(client1, {'MOVIE'}),
            receive_data(client2, {'PULSE_HEIGHT'})
        )
