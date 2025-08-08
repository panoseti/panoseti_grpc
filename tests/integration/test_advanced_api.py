
import pytest
import asyncio
import grpc
import json
from daq_data.client import AioDaqDataClient, hp_io_config_simulate_path

pytestmark = pytest.mark.asyncio


async def test_multiple_clients_streaming_concurrently(default_server_process):
    """
    Tests that the server can handle multiple clients connecting and
    streaming different data types concurrently.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process['ip_addr']}]}

    # Use two separate client instances
    async with AioDaqDataClient(daq_config, network_config=None) as client1, \
            AioDaqDataClient(daq_config, network_config=None) as client2:

        # Initialize the server using one of the clients
        assert await client1.init_sim(hosts=None) is True, "Server initialization failed"

        async def receive_data(client, client_id, stream_movie, stream_ph):
            """Coroutine to receive and validate data for one client."""
            stream = await client.stream_images(
                hosts=None,
                stream_movie_data=stream_movie,
                stream_pulse_height_data=stream_ph,
                update_interval_seconds=0.1
            )

            images_received = 0
            async for image in stream:
                if stream_movie:
                    assert image['type'] == 'MOVIE'
                if stream_ph:
                    assert image['type'] == 'PULSE_HEIGHT'
                images_received += 1
                if images_received >= 2:
                    break
            assert images_received >= 2, f"Client {client_id} did not receive enough images"

        # Run coroutines for both clients concurrently
        await asyncio.gather(
            receive_data(client1, 1, stream_movie=True, stream_ph=False),
            receive_data(client2, 2, stream_movie=False, stream_ph=True)
        )


async def test_server_reinitialization_logic(default_server_process):
    """
    Tests that the server correctly handles being re-initialized, including
    forcing reconfiguration while a client is streaming.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process['ip_addr']}]}

    # Load the simulation config to be used for initialization
    with open(hp_io_config_simulate_path, 'r') as f:
        hp_io_cfg = json.load(f)

    # Client A will connect and start reading
    async with AioDaqDataClient(daq_config, network_config=None) as client_a:
        assert await client_a.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg) is True

        image_stream = await client_a.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.2
        )

        # This task will keep the reader stream active in the background
        reader_task = asyncio.create_task(image_stream.__anext__())
        await asyncio.sleep(3.0)  # Allow the reader to be established on the server

        # Client B attempts to re-initialize while Client A is reading
        async with AioDaqDataClient(daq_config, network_config=None) as client_b:
            # 1. Attempt to init without `force=True`. This should fail.
            hp_io_cfg['force'] = False
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await client_b.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg)
            assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION, \
                "Server should reject init when readers are active and force is false"

            # 2. Attempt to init with `force=True`. This should succeed.
            hp_io_cfg['force'] = True
            assert await client_b.init_hp_io(hosts=None, hp_io_cfg=hp_io_cfg) is True, \
                "Server should re-initialize when force is true"

        # The original reader task should have been cancelled by the forced re-initialization
        assert reader_task.done(), "Reader task should be cancelled after the forced re-initialization"

