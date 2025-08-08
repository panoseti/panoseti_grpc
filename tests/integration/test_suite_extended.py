import pytest
import asyncio
import os
import grpc
from pathlib import Path
from daq_data.client import AioDaqDataClient
from daq_data.server import DaqDataServicer

pytestmark = pytest.mark.asyncio


# Part 1: advanced Unix-Domain Socket (UDS) streaming tests

async def test_uds_stream_high_frequency(default_server_process):
    """
    Tests the server's ability to handle high-frequency data streaming over a UDS.
    This test configures the server to send data at a very high rate to check for
    data loss, corruption, or back-pressure issues under load.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process['ip_addr']}]}

    # Define a custom config to run the simulation at a high frequency
    hp_io_cfg = {
        "data_dir": "simulated_data_dir",
        "update_interval_seconds": 0.001,  # Match the stream's update interval
        "force": True,
        "simulate_daq": True,
        "module_ids": [],
    }

    async with AioDaqDataClient(daq_config, network_config=None) as client:
        # Initialize the server with the high-frequency simulation config
        assert await client.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg) is True

        # Request a stream with a very small update interval to stress the server
        stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.001,  # High frequency
        )

        received_images = 0
        frame_numbers = []
        async for image in stream:
            assert isinstance(image, dict)
            frame_numbers.append(image['frame_number'])
            received_images += 1
            if received_images >= 100:
                break

        assert received_images >= 100
        # Check if frame numbers are mostly sequential. With matched intervals, gaps should be minimal.
        gaps = sum(1 for i in range(len(frame_numbers) - 1) if frame_numbers[i + 1] - frame_numbers[i] > 2)
        assert gaps < 10, "Too many gaps in frame numbers, indicating data loss."


async def test_uds_back_pressure_handling(default_server_process):
    """
    Tests how the server handles a "slow" client that cannot keep up with the data stream.
    A robust server should not crash or hang; it should either buffer data, drop old data,
    or gracefully disconnect the client.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process['ip_addr']}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_sim(hosts=None) is True

        stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.01,
        )

        images_received = 0
        async for image in stream:
            images_received += 1
            # Simulate a slow client by introducing a delay
            await asyncio.sleep(0.5)
            if images_received >= 5:
                break

        # The primary assertion is that the test completes without the server or client crashing.
        # This indicates that the server's internal queues and writer logic are handling the
        # back-pressure from the slow client.
        assert images_received >= 5


# async def test_client_reconnection_after_server_restart(n_sim_servers_fixture_factory):
#     """
#     Tests if a client can gracefully handle a server restart and reconnect to the new UDS.
#     """
#     # Start one server instance
#     server_details = await n_sim_servers_fixture_factory(1)
#     uds_path = server_details[0]['ip_addr']
#     daq_config = {"daq_nodes": [{"ip_addr": uds_path}]}
#
#     async with AioDaqDataClient(daq_config, network_config=None) as client:
#         assert await client.init_sim(hosts=None) is True
#         stream = await client.stream_images(hosts=None, stream_movie_data=True, stream_pulse_height_data=False,
#                                             update_interval_seconds=0.1)
#
#         # Receive at least one image to confirm the connection
#         first_image = await stream.__anext__()
#         assert first_image is not None
#
#         # Stop the server
#         server_details[0]['stop_event'].set()
#         await asyncio.wait_for(server_details[0]['task'], timeout=5.0)
#         assert not Path(uds_path.replace("unix://", "")).exists()
#
#         # Try to receive another image, expecting an RPC error
#         with pytest.raises(grpc.aio.AioRpcError):
#             await stream.__anext__()
#
#         # Restart the server with the same UDS path (new fixture instance)
#         new_server_details = await n_sim_servers_fixture_factory(1, uds_paths=[uds_path])
#         new_uds_path = new_server_details[0]['ip_addr']
#         assert new_uds_path == uds_path
#
#         # The client should be able to re-initialize and stream again
#         assert await client.init_sim(hosts=None) is True
#         new_stream = await client.stream_images(hosts=None, stream_movie_data=True, stream_pulse_height_data=False,
#                                                 update_interval_seconds=0.1)
#         second_image = await new_stream.__anext__()
#         assert second_image is not None
#
#
# # --- Part 2: Test Suite for a Client Connecting to N Simulation Servers ---
#
# @pytest.mark.parametrize("num_servers", [2, 5])
# async def test_client_connects_to_n_servers(n_sim_servers_fixture_factory, num_servers):
#     """
#     Tests that a single client can connect to and stream from N server instances concurrently.
#     """
#     server_details = await n_sim_servers_fixture_factory(num_servers)
#     daq_config = {"daq_nodes": [{"ip_addr": detail['ip_addr']} for detail in server_details]}
#
#     async with AioDaqDataClient(daq_config, network_config=None) as client:
#         # Initialize all servers
#         assert await client.init_sim(hosts=None) is True
#
#         # Stream from all servers
#         stream = await client.stream_images(
#             hosts=None,  # Omitting hosts should default to all configured nodes
#             stream_movie_data=True,
#             stream_pulse_height_data=True,
#             update_interval_seconds=0.1
#         )
#
#         received_from_module = set()
#         for _ in range(num_servers * 5):  # Receive enough images to likely get one from each server
#             image = await stream.__anext__()
#             received_from_module.add(image['module_id'])
#
#         # In this simulation, each server simulates a different module_id
#         expected_modules = {detail['module_id'] for detail in server_details}
#         assert received_from_module == expected_modules, f"Did not receive images from all {num_servers} servers."
#
#
# async def test_client_handles_one_server_failure(n_sim_servers_fixture_factory):
#     """
#     Tests that the client can continue streaming from remaining servers
#     if one server in a multi-server setup fails.
#     """
#     num_servers = 3
#     server_details = await n_sim_servers_fixture_factory(num_servers)
#     daq_config = {"daq_nodes": [{"ip_addr": detail['ip_addr']} for detail in server_details]}
#
#     async with AioDaqDataClient(daq_config, network_config=None) as client:
#         assert await client.init_sim(hosts=None) is True
#         stream = await client.stream_images(hosts=None, stream_movie_data=True, stream_pulse_height_data=False,
#                                             update_interval_seconds=0.1)
#
#         # Receive from all servers to confirm connections
#         received_from_module_before = set()
#         for _ in range(num_servers * 5):
#             image = await stream.__anext__()
#             received_from_module_before.add(image['module_id'])
#
#         expected_modules = {detail['module_id'] for detail in server_details}
#         assert received_from_module_before == expected_modules
#
#         # Shut down one server
#         server_to_stop = server_details[0]
#         server_to_stop['stop_event'].set()
#         await asyncio.wait_for(server_to_stop['task'], timeout=5.0)
#
#         # Continue streaming and verify we only get images from the remaining servers
#         received_from_module_after = set()
#         try:
#             for _ in range((num_servers - 1) * 10):
#                 image = await stream.__anext__()
#                 received_from_module_after.add(image['module_id'])
#         except grpc.aio.AioRpcError as e:
#             # The client's stream might be implemented to terminate on any single error.
#             # A more robust implementation might try to continue. We accept both behaviors.
#             pass
#
#         remaining_modules = {detail['module_id'] for detail in server_details[1:]}
#         assert server_to_stop['module_id'] not in received_from_module_after
#         # It's possible we don't receive from all remaining modules if the stream errors out,
#         # so we check that the set of received modules is a subset of the remaining ones.
#         assert received_from_module_after.issubset(remaining_modules)
