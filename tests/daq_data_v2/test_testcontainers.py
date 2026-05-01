import asyncio
import os
import shutil
import tempfile

import pytest
import pytest_asyncio
from testcontainers.core.container import DockerContainer

from panoseti_grpc.daq_data_v2.client import AioDaqDataV2Client


@pytest_asyncio.fixture(scope="module")
async def aggregator_container():
    """Starts the DaqDataV2 aggregator in a testcontainer."""
    # Build context for the aggregator (Headnode role)
    # We use the standard pseti-daqnode image but run it in headnode mode
    # For now, let's just use a simple python:3.14-slim and install dependencies
    # or mount the local source.

    grpc_src = os.path.abspath("src")
    container = DockerContainer("python:3.14-slim")
    container.with_volume_mapping(grpc_src, "/grpc/src", "rw")
    container.with_env("PYTHONPATH", "/grpc/src")

    # Simple start command for the unified server in headnode profile
    # We need to make sure dependencies are installed.
    # In a real CI, we'd use a pre-built image.
    # For this test, let's assume 'pip install' works or use the pseti-daqnode image.
    # Let's try using pseti-daqnode:latest if it exists, as it should have deps.

    container.image = "pseti-daqnode:latest"
    container.with_command("python -m panoseti_grpc.unified_main --profile headnode --services daq_data_v2")
    container.with_exposed_ports(50051)

    with container:
        # Wait for port
        host = container.get_container_host_ip()
        port = container.get_exposed_port(50051)
        target = f"{host}:{port}"

        # Health check
        async with AioDaqDataV2Client(target) as client:
            for _ in range(30):
                if await client.ping():
                    break
                await asyncio.sleep(0.5)
            else:
                pytest.fail("Aggregator container did not become ready")

        yield target


@pytest_asyncio.fixture(scope="function")
async def forwarder_container(aggregator_container):
    """Starts a forwarder in a testcontainer, pushing to the aggregator."""
    agg_target = aggregator_container
    # On macOS, localhost inside container refers to the container itself.
    # We need to use the bridge gateway IP to reach the host-mapped aggregator port.
    # testcontainers handles this if we use a shared network, but let's keep it simple.

    # Actually, if both are in the same Docker network, they can talk via container names.
    # But testcontainers-python's network support is a bit different.

    # Let's use the host's reachable IP from the aggregator_container.

    grpc_src = os.path.abspath("src")
    socket_dir = tempfile.mkdtemp(prefix="v2_test_sockets_")
    os.chmod(socket_dir, 0o777)

    container = DockerContainer("pseti-daqnode:latest")
    container.with_volume_mapping(grpc_src, "/grpc/src", "rw")
    container.with_volume_mapping(socket_dir, "/tmp/sockets", "rw")
    container.with_env("PYTHONPATH", "/grpc/src")

    # Point forwarder to the aggregator (reachable via host IP)
    container.with_command(
        f"python -m panoseti_grpc.daq_data_v2.forwarder "
        f"--headnode {agg_target} "
        f"--socket-template /tmp/sockets/dp_{{dp_name}}.sock"
    )

    with container:
        yield {"container": container, "socket_dir": socket_dir, "socket_template": "/tmp/sockets/dp_{dp_name}.sock"}

    shutil.rmtree(socket_dir)


@pytest.mark.asyncio
async def test_v2_container_data_flow(aggregator_container, forwarder_container):
    """Verifies end-to-end data flow using testcontainers."""
    agg_target = aggregator_container
    fwd = forwarder_container
    host_socket_dir = fwd["socket_dir"]
    socket_template = fwd["socket_template"]

    # Start a simulator on the HOST, writing to the mounted socket dir
    # so the forwarder container can read it.
    import logging

    from panoseti_grpc.daq_data_v2.simulator import Simulator

    logger = logging.getLogger("test_v2_sim")

    # Adapt socket template for host perspective
    host_socket_template = os.path.join(host_socket_dir, "dp_{dp_name}.sock")

    sim_configs = [
        {
            "dp_name": "img16",
            "pff_path": "daq_data/simulated_data_dir/obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd/start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_0.debug_TRUNCATED.pff",
            "module_id": 1,
            "bpp": 2,
            "shape": (32, 32),
        }
    ]

    sim = Simulator(host_socket_template, sim_configs, logger)
    sim_task = asyncio.create_task(sim.run())

    try:
        async with AioDaqDataV2Client(agg_target) as client:
            assert await client.ping() is True

            stream = client.stream_images(update_interval=0.1)
            received = 0
            async for response in stream:
                assert response.pano_image.module_id == 1
                received += 1
                if received >= 3:
                    break

            assert received >= 3
    finally:
        sim.stop_event.set()
        await sim_task
