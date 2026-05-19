import asyncio

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient, hp_io_config_simulate
from panoseti_grpc.grpc_utils.exceptions import FailedPreconditionError

pytestmark = pytest.mark.asyncio


async def test_multiple_clients_streaming_concurrently(default_server_process):
    """
    Tests that the server can handle multiple clients connecting and
    streaming different data types concurrently.
    """
    host, port = default_server_process["host"], default_server_process["port"]

    async with (
        AioDaqDataClient(host, port) as client1,
        AioDaqDataClient(host, port) as client2,
    ):
        assert await client1.init_sim() is True, "Server initialization failed"

        async def receive_data(client, client_id, stream_movie, stream_ph):
            """Coroutine to receive and validate data for one client."""
            images_received = 0
            async for image in client.stream_images(
                stream_movie_data=stream_movie,
                stream_pulse_height_data=stream_ph,
                update_interval_seconds=0.1,
                timeout=15,
            ):
                if stream_movie:
                    assert image["type"] == "MOVIE"
                if stream_ph:
                    assert image["type"] == "PULSE_HEIGHT"
                images_received += 1
                if images_received >= 2:
                    break
            assert images_received >= 2, f"Client {client_id} did not receive enough images"

        await asyncio.gather(
            receive_data(client1, 1, stream_movie=True, stream_ph=False),
            receive_data(client2, 2, stream_movie=False, stream_ph=True),
        )


async def test_server_reinitialization_logic(default_server_process):
    """
    Tests that the server correctly handles being re-initialized, including
    forcing reconfiguration while a client is streaming.
    """
    host, port = default_server_process["host"], default_server_process["port"]
    hp_io_cfg = {**hp_io_config_simulate}

    async with AioDaqDataClient(host, port) as client_a:
        assert await client_a.init_hp_io(hp_io_cfg) is True

        image_stream = client_a.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.2,
            timeout=15,
        )

        # Keep the reader stream active in the background
        reader_task = asyncio.create_task(image_stream.__anext__())
        await asyncio.sleep(3.0)  # Allow the reader to be established on the server

        async with AioDaqDataClient(host, port) as client_b:
            # 1. Attempt to init without `force=True`. This should fail.
            with pytest.raises(FailedPreconditionError):
                await client_b.init_hp_io({**hp_io_cfg, "force": False})

            # 2. Attempt to init with `force=True`. This should succeed.
            assert await client_b.init_hp_io({**hp_io_cfg, "force": True}) is True, (
                "Server should re-initialize when force is true"
            )

        # The original reader task should have been cancelled by the forced re-initialization
        assert reader_task.done(), "Reader task should be cancelled after the forced re-initialization"
