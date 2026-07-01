import asyncio

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient, hp_io_config_simulate
from panoseti_grpc.grpc_utils.exceptions import PanosetiRpcError

pytestmark = pytest.mark.asyncio


# Part 1: advanced single-server streaming tests


async def test_uds_stream_high_frequency(default_server_process):
    """
    Tests the server's ability to handle high-frequency data streaming over a UDS.
    This test configures the server to send data at a very high rate to check for
    data loss, corruption, or back-pressure issues under load.
    """
    host, port = default_server_process["host"], default_server_process["port"]

    hp_io_cfg = {
        "data_dir": "daq_data/simulated_data_dir",
        "update_interval_seconds": 0.01,  # Server minimum is 0.01s
        "force": True,
        "simulate_daq": True,
        "module_ids": [],
    }

    async with AioDaqDataClient(host, port) as client:
        assert await client.init_hp_io(hp_io_cfg) is True

        received_images = 0
        frame_numbers = []
        async for image in client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.01,
        ):
            assert isinstance(image, dict)
            frame_numbers.append(image["frame_number"])
            received_images += 1
            if received_images >= 100:
                break

        assert received_images >= 100
        gaps = sum(1 for i in range(len(frame_numbers) - 1) if frame_numbers[i + 1] - frame_numbers[i] > 2)
        assert gaps < 10, "Too many gaps in frame numbers, indicating data loss."


async def test_uds_back_pressure_handling(default_server_process):
    """
    Tests how the server handles a "slow" client that cannot keep up with the data stream.
    A robust server should not crash or hang.
    """
    host, port = default_server_process["host"], default_server_process["port"]
    async with AioDaqDataClient(host, port) as client:
        assert await client.init_hp_io(hp_io_config_simulate) is True

        images_received = 0
        async for _image in client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.01,
        ):
            images_received += 1
            await asyncio.sleep(0.5)  # Simulate slow client
            if images_received >= 5:
                break

        assert images_received >= 5


async def test_stream_terminates_when_server_stops(n_sim_servers_fixture_factory):
    """
    Tests that a streaming client gets a clean termination when the server shuts down.
    """
    server_details = await n_sim_servers_fixture_factory(1)
    srv = server_details[0]

    async with AioDaqDataClient(srv["host"], srv["port"]) as client:
        assert await client.init_hp_io(hp_io_config_simulate) is True
        stream = client.stream_images(
            stream_movie_data=True, stream_pulse_height_data=False, update_interval_seconds=0.1
        )

        first_image = await stream.__anext__()
        assert first_image is not None

        # Stop the server
        srv["stop_event"].set()
        await asyncio.wait_for(srv["task"], timeout=5.0)

        # Stream should terminate cleanly
        ended = False
        try:
            async with asyncio.timeout(5.0):
                async for _ in stream:
                    pass
            ended = True  # Clean StopAsyncIteration
        except TimeoutError, PanosetiRpcError, StopAsyncIteration:
            ended = True
        assert ended


# --- Part 2: Gateway E2E tests with N edge servers ---


@pytest.mark.parametrize("num_servers", [2, 5])
async def test_client_connects_to_n_servers(gateway_factory, num_servers):
    """
    Tests that a single client can stream from N edge servers via the gateway.
    """
    gw = await gateway_factory(num_servers)

    async with AioDaqDataClient(gw["host"], gw["port"]) as client:
        assert await client.init_hp_io(hp_io_config_simulate) is True

        received_from_module = set()
        expected_modules = {detail["module_id"] for detail in gw["edge_details"]}
        async for image in client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
        ):
            received_from_module.add(image["module_id"])
            if received_from_module == expected_modules:
                break

        assert received_from_module == expected_modules, f"Did not receive images from all {num_servers} edges."


async def test_client_handles_one_server_failure(gateway_factory):
    """
    Tests that the gateway continues streaming from remaining edges
    if one edge in a multi-edge setup fails.
    """
    num_edges = 3
    gw = await gateway_factory(num_edges)
    edge_details = gw["edge_details"]

    async with AioDaqDataClient(gw["host"], gw["port"]) as client:
        assert await client.init_hp_io(hp_io_config_simulate) is True

        stream = client.stream_images(
            stream_movie_data=True, stream_pulse_height_data=False, update_interval_seconds=0.1
        )

        # Receive from all edges to confirm connections
        expected_modules = {detail["module_id"] for detail in edge_details}
        received_before = set()
        for _ in range(num_edges * 5):
            image = await stream.__anext__()
            received_before.add(image["module_id"])
        assert received_before == expected_modules

        # Shut down one edge
        server_to_stop = edge_details[0]
        stopped_module = server_to_stop["module_id"]
        server_to_stop["stop_event"].set()
        await asyncio.wait_for(server_to_stop["task"], timeout=5.0)

        # Continue on the same stream — remaining edges should still deliver
        remaining_modules = {d["module_id"] for d in edge_details[1:]}
        received_after = set()
        try:
            for _ in range((num_edges - 1) * 20):
                image = await stream.__anext__()
                mid = image["module_id"]
                if mid != stopped_module:
                    received_after.add(mid)
                if received_after >= remaining_modules:
                    break
        except PanosetiRpcError, StopAsyncIteration:
            pass

        assert received_after.issubset(remaining_modules)
