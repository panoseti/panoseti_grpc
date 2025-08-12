import pytest
import pytest_asyncio
import asyncio
import json
import logging
from pathlib import Path
import os
import urllib.parse
import uuid
import copy
from typing import Optional, Tuple

from daq_data.server import serve
from daq_data.client import AioDaqDataClient, DaqDataClient
from daq_data.daq_data_pb2 import PanoImage
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict

TEST_CFG_DIR = Path("tests/config")
TEST_CFG_DIR.mkdir(exist_ok=True)


@pytest.fixture(scope="session")
def server_config_base():
    """Provides a base server configuration dictionary."""
    with open(TEST_CFG_DIR / "daq_data_server_config.json", "r") as f:
        cfg = json.load(f)
    return cfg


#Fixtures for Simulation Tests (Parameterized)
@pytest.fixture(scope="session")
def rpc_sim_server_config(server_config_base):
    cfg = copy.deepcopy(server_config_base)
    cfg['simulate_daq_cfg']['simulation_mode'] = 'rpc'
    cfg['acquisition_methods'] = {"rpc": {"enabled": True}}
    cfg['simulate_daq_cfg']['strategies'] = {"rpc": {}}
    return cfg


@pytest.fixture(scope="session")
def uds_sim_server_config(server_config_base):
    cfg = copy.deepcopy(server_config_base)
    cfg['simulate_daq_cfg']['simulation_mode'] = 'uds'
    dps = ["img16", "ph256"]
    cfg['acquisition_methods'] = {"uds": {
        "enabled": True,
        "data_products": dps,
        "socket_path_template": "/tmp/hashpipe_grpc.module_{module_id}.dp_{dp_name}.sock"
    }}
    cfg['simulate_daq_cfg']['strategies'] = {"uds": {"data_products": dps}}
    return cfg

@pytest.fixture(scope="session")
def filesystem_pipe_sim_server_config(server_config_base):
    cfg = copy.deepcopy(server_config_base)
    cfg['simulate_daq_cfg']['simulation_mode'] = 'filesystem_pipe'
    dps = ["img16", "ph256"]
    cfg['acquisition_methods'] = {"filesystem_pipe": {"enabled": True}}
    cfg['simulate_daq_cfg']['strategies'] = {"filesystem_pipe": {"frames_per_pff": 100, "frame_limit": 1000}}
    return cfg

@pytest.fixture(scope="session")
def filesystem_poll_sim_server_config(server_config_base):
    cfg = copy.deepcopy(server_config_base)
    cfg['simulate_daq_cfg']['simulation_mode'] = 'filesystem_poll'
    dps = ["img16", "ph256"]
    cfg['acquisition_methods'] = {"filesystem_poll": {"enabled": True}}
    cfg['simulate_daq_cfg']['strategies'] = {"filesystem_poll": {"frames_per_pff": 100, "frame_limit": 1000}}
    return cfg


@pytest_asyncio.fixture(scope="function")
async def sim_server_process(request):
    """Parameterized fixture to start a server with a specific simulation config."""
    config = request.getfixturevalue(request.param)
    uds_path_str = config["unix_domain_socket"]
    uds_path = Path(urllib.parse.urlparse(uds_path_str).path)
    shutdown_event = asyncio.Event()

    server_task = asyncio.create_task(serve(config, shutdown_event, in_main_thread=False))

    # Wait for the server to be fully ready by pinging it
    async with AioDaqDataClient({"daq_nodes": [{"ip_addr": uds_path_str}]}, network_config=None) as client:
        for _ in range(30):
            if uds_path.exists() and await client.ping(uds_path_str):
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("Parameterized server did not become ready in time.", pytrace=False)

    yield uds_path_str

    # Graceful shutdown sequence
    shutdown_event.set()
    try:
        # Wait for the server task to finish, which it should upon the event being set.
        # This allows the 'serve' function to complete its cleanup.
        await asyncio.wait_for(server_task, timeout=5.0)
    except asyncio.TimeoutError:
        # If the server hangs, then cancel the task as a fallback.
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)
    finally:
        # Ensure the Unix Domain Socket file is always removed.
        if uds_path.exists():
            try:
                os.unlink(uds_path)
            except OSError:
                pass



@pytest_asyncio.fixture(scope="function")
async def default_server_process(uds_sim_server_config):
    """A non-parameterized fixture that runs a standard RPC simulation server."""
    assert os.name == 'posix', "Only supported on POSIX systems."
    config = uds_sim_server_config

    # Use a unique socket path for each test invocation to prevent collisions
    uds_path_str = f"unix:///tmp/test_daq_data_{uuid.uuid4().hex}.sock"
    config["unix_domain_socket"] = uds_path_str
    uds_path = Path(urllib.parse.urlparse(uds_path_str).path)

    shutdown_event = asyncio.Event()
    server_task = asyncio.create_task(serve(config, shutdown_event, in_main_thread=False))

    try:
        # Wait for the server to be fully ready by pinging it
        daq_config = {"daq_nodes": [{"ip_addr": uds_path_str, "data_dir": "daq_data/simulated_data_dir"}]}
        async with AioDaqDataClient(daq_config, network_config=None, stop_event=shutdown_event) as client:
            for _ in range(30):  # 3-second timeout
                if uds_path.exists() and await client.ping(uds_path_str):
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("Default server did not become ready in time.", pytrace=False)
        yield {
            "ip_addr": uds_path_str,
            "data_dir": "daq_data/simulated_data_dir",
            "stop_event": shutdown_event,
        }
    finally:
        # Graceful shutdown sequence
        shutdown_event.set()
        try:
            # Wait for the server task to finish, which it should upon the event being set
            await asyncio.wait_for(server_task, timeout=5.0)
        except asyncio.TimeoutError:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)

        # Ensure the socket file is removed
        if uds_path.exists():
            try:
                os.unlink(uds_path)
            except OSError:
                pass


@pytest_asyncio.fixture(scope="function")
async def n_sim_servers_fixture_factory(server_config_base):
    """
    A pytest fixture factory that starts N simulation server instances.
    """
    all_server_details = []

    async def _factory(num_servers: int, uds_paths: Optional[list[str]] = None):
        if uds_paths and len(uds_paths) != num_servers:
            raise ValueError("The number of provided UDS paths must match num_servers.")

        for i in range(num_servers):
            config = copy.deepcopy(server_config_base)
            uds_path_str = uds_paths[i] if uds_paths else f"unix:///tmp/test_daq_data_{uuid.uuid4().hex}.sock"
            module_id = 200 + i

            config["unix_domain_socket"] = uds_path_str
            config['simulate_daq_cfg']['sim_module_ids'] = [module_id]
            config['simulate_daq_cfg']['simulation_mode'] = 'uds'
            config['acquisition_methods']['uds']['enabled'] = True
            config['acquisition_methods']['filesystem_poll']['enabled'] = True
            config['acquisition_methods']['filesystem_pipe']['enabled'] = True
            config['acquisition_methods']['uds'][
                'socket_path_template'] = "/tmp/hashpipe_grpc.module_{module_id}.dp_{dp_name}.sock"

            uds_path = Path(urllib.parse.urlparse(uds_path_str).path)
            if uds_path.exists():
                uds_path.unlink()

            shutdown_event = asyncio.Event()

            server_task = asyncio.create_task(serve(config, shutdown_event, in_main_thread=False))

            # readiness check
            async with AioDaqDataClient({"daq_nodes": [{"ip_addr": uds_path_str}]}, network_config=None) as client:
                for _ in range(30):
                    if uds_path.exists() and await client.ping(uds_path_str):
                        break
                    await asyncio.sleep(0.1)
                else:
                    pytest.fail(f"Server {i} gRPC endpoint did not become ready.", pytrace=False)

            server_details = {
                "ip_addr": uds_path_str,
                "task": server_task,
                "stop_event": shutdown_event,
                "path": uds_path,
                "module_id": module_id,
            }
            all_server_details.append(server_details)

        return all_server_details

    try:
        yield _factory
    finally:
        # Graceful shutdown sequence
        for sd in all_server_details:
            if sd.get('stop_event'):
                sd['stop_event'].set()

        server_tasks = [sd['task'] for sd in all_server_details if sd.get('task')]

        if server_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*server_tasks), timeout=5.0)
            except asyncio.TimeoutError:
                for task in server_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*server_tasks, return_exceptions=True)

        for sd in all_server_details:
            p = sd.get('path')
            if p and p.exists():
                try:
                    os.unlink(p)
                except OSError:
                    pass


@pytest_asyncio.fixture
async def async_client(default_server_process):
    """Provides a connected AioDaqDataClient for API tests."""

    daq_config = {
        "daq_nodes": [
            {
                "ip_addr": default_server_process['ip_addr'],
                "data_dir": default_server_process['data_dir']
            }
        ]
    }
    async with AioDaqDataClient(
            daq_config,
            network_config=None,
            log_level=logging.DEBUG,
            stop_event=default_server_process['stop_event']
    ) as client:
        yield client


@pytest.fixture
def sync_client(default_server_process):
    """Provides a connected DaqDataClient for API tests."""
    daq_config = {
        "daq_nodes": [
            {
                "ip_addr": default_server_process['ip_addr'],
                "data_dir": default_server_process['data_dir']
            }
        ]
    }
    with DaqDataClient(daq_config, network_config=None, log_level=logging.DEBUG) as client:
        yield client


@pytest.fixture
def sample_pano_image():
    header_dict = {"test_field": "test_value"}
    return PanoImage(
        type=PanoImage.Type.MOVIE, header=ParseDict(header_dict, Struct()),
        image_array=[i for i in range(256)], shape=[16, 16],
        bytes_per_pixel=1, file="test_upload.pff", module_id=101,
    )
