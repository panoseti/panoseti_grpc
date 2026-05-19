import asyncio

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio


async def test_async_ping(async_client):
    """Test ping via health check with the async client."""
    assert await async_client.ping() is True, "Ping should work for a running server"


async def test_async_initialization(async_client):
    """Test the InitHpIo RPC in simulation mode with the async client."""
    success = await async_client.init_sim()
    assert success is True, "init_sim should succeed"


async def test_async_stream_images(async_client):
    """Test the full data streaming workflow: init -> stream -> receive."""
    assert await async_client.init_sim() is True
    received_images = 0
    async for image in async_client.stream_images(
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=0.1,
        timeout=5.0,
    ):
        assert isinstance(image, dict)
        received_images += 1
        if received_images >= 2:
            break
    assert received_images >= 2


async def test_async_stream_can_be_interrupted(default_server_process):
    """Verify that a streaming loop can be broken out of cleanly."""
    async with AioDaqDataClient(
        default_server_process["host"],
        default_server_process["port"],
    ) as client:
        assert await client.init_sim() is True
        images_received = 0
        async for _ in client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.05,
            timeout=5.0,
        ):
            images_received += 1
            if images_received >= 2:
                break
        assert images_received >= 2
