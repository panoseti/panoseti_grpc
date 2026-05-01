import asyncio
import os
import subprocess

import grpc
import pytest

from panoseti_grpc.daq_data_v2.client import AioDaqDataV2Client
from panoseti_grpc.daq_data_v2.server import DaqDataV2Servicer
from panoseti_grpc.generated import daq_data_v2_pb2_grpc

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def v2_server():
    """Starts a standalone DaqDataV2 server for testing."""
    server = grpc.aio.server()
    logger = asyncio.get_event_loop().run_in_executor(None, lambda: None)  # Mock logger
    import logging

    test_logger = logging.getLogger("test_v2_server")
    servicer = DaqDataV2Servicer(test_logger)
    daq_data_v2_pb2_grpc.add_DaqDataV2Servicer_to_server(servicer, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    yield f"localhost:{port}", servicer
    await server.stop(0)


async def test_v2_data_flow(v2_server):
    """Verifies simulator -> forwarder -> server -> client flow."""
    target, servicer = v2_server

    socket_template = "/tmp/test_v2_dp_{dp_name}.sock"
    # Clean up stale sockets
    for dp in ["img16", "ph256"]:
        path = socket_template.format(dp_name=dp)
        if os.path.exists(path):
            os.unlink(path)

    # Start Simulator
    sim_proc = await asyncio.create_subprocess_exec(
        "python",
        "-m",
        "panoseti_grpc.daq_data_v2.simulator",
        "--socket-template",
        socket_template,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Start Forwarder
    fwd_proc = await asyncio.create_subprocess_exec(
        "python",
        "-m",
        "panoseti_grpc.daq_data_v2.forwarder",
        "--headnode",
        target,
        "--socket-template",
        socket_template,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for data to propagate
        async with AioDaqDataV2Client(target) as client:
            # Ping first
            assert await client.ping() is True

            # Subscribe to stream
            stream = client.stream_images(update_interval=0.1)
            received = 0
            async for response in stream:
                assert response.pano_image.module_id in [1, 3]
                received += 1
                if received >= 5:
                    break

            assert received >= 5
    finally:
        if sim_proc.returncode is None:
            sim_proc.terminate()
            stdout, stderr = await sim_proc.communicate()
            if stdout:
                print(f"Simulator STDOUT: {stdout.decode()}")
            if stderr:
                print(f"Simulator STDERR: {stderr.decode()}")

        if fwd_proc.returncode is None:
            fwd_proc.terminate()
            stdout, stderr = await fwd_proc.communicate()
            if stdout:
                print(f"Forwarder STDOUT: {stdout.decode()}")
            if stderr:
                print(f"Forwarder STDERR: {stderr.decode()}")

        await asyncio.gather(sim_proc.wait(), fwd_proc.wait())
        for dp in ["img16", "ph256"]:
            path = socket_template.format(dp_name=dp)
            if os.path.exists(path):
                os.unlink(path)
