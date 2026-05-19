import asyncio

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient
from panoseti_grpc.grpc_utils.exceptions import InvalidArgumentError, PanosetiRpcError, ResourceExhaustedError

pytestmark = pytest.mark.asyncio


async def test_max_clients_resource_exhaustion(default_server_process, server_config_base):
    """
    Verify that the server correctly rejects new clients when its
    max_concurrent_rpcs limit is reached.
    """
    max_clients = server_config_base["max_concurrent_rpcs"]
    host, port = default_server_process["host"], default_server_process["port"]

    clients = []
    streams = []
    try:
        # 0. Initialize the server
        async with AioDaqDataClient(host, port) as client:
            assert await client.init_sim() is True

        # 1. Connect clients up to the server's limit.
        for i in range(max_clients):
            client = AioDaqDataClient(host, port)
            await client.__aenter__()
            clients.append(client)
            stream = client.stream_images(
                stream_movie_data=True,
                stream_pulse_height_data=False,
                update_interval_seconds=0.75,
                timeout=120,
            )
            streams.append(stream)
            await stream.__anext__()
            print(f"Client {i + 1}/{max_clients} connected and streaming.")

        # 2. Attempt to connect one more client. This should fail.
        print("Attempting to connect one more client, expecting failure...")
        async with AioDaqDataClient(host, port) as extra_client:
            with pytest.raises(ResourceExhaustedError):
                stream = extra_client.stream_images(
                    stream_movie_data=True,
                    stream_pulse_height_data=False,
                    update_interval_seconds=0.1,
                    timeout=5.0,
                )
                await stream.__anext__()
    finally:
        for client in clients:
            await client.__aexit__(None, None, None)


async def test_init_with_nonexistent_data_dir(async_client: AioDaqDataClient):
    """
    Verify that InitHpIo fails with INVALID_ARGUMENT if the data_dir does not exist
    when not in simulation mode.
    """
    invalid_config = {
        "data_dir": "/path/to/a/place/that/does/not/exist",
        "simulate_daq": False,
        "force": True,
        "update_interval_seconds": 0.1,
    }
    with pytest.raises(InvalidArgumentError) as exc_info:
        await async_client.init_hp_io(invalid_config)

    assert "does not exist" in exc_info.value.details


async def test_stream_after_server_shutdown(default_server_process):
    """
    Verify that a streaming client is properly disconnected when the server
    is gracefully shut down.
    """
    host, port = default_server_process["host"], default_server_process["port"]
    stop_event = default_server_process["stop_event"]

    async with AioDaqDataClient(host, port) as client:
        await client.init_sim()
        image_stream = client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.1,
        )

        for _i in range(10):
            await image_stream.__anext__()

        stop_event.set()
        await asyncio.sleep(0.5)

        # Server shutdown causes either StopAsyncIteration or PanosetiRpcError (CANCELLED)
        with pytest.raises((StopAsyncIteration, PanosetiRpcError)):
            await image_stream.__anext__()


async def test_abrupt_client_disconnection(default_server_process):
    """
    Test that the server correctly releases resources when a client
    disconnects abruptly without a graceful exit.
    """
    host, port = default_server_process["host"], default_server_process["port"]

    client_a = AioDaqDataClient(host, port)
    await client_a.__aenter__()
    await client_a.init_sim()
    stream_a = client_a.stream_images(
        stream_movie_data=True,
        stream_pulse_height_data=False,
        update_interval_seconds=0.1,
    )

    streaming_task = asyncio.create_task(stream_a.__anext__())
    await asyncio.sleep(0.5)

    try:
        streaming_task.cancel()
        await streaming_task
    except Exception as e:
        raise AssertionError(f"Client failed to gracefully cancel streaming task: {e}") from e

    async with AioDaqDataClient(host, port) as client_b:
        await client_b.init_sim()
        stream_b = client_b.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.1,
        )
        image = await stream_b.__anext__()
        assert image is not None, "Client B should have received an image, proving the server recovered."
