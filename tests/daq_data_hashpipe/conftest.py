import pytest
import pytest_asyncio
import asyncio
import json
import sys
import signal
import logging
from pathlib import Path
import subprocess
import time
import os
import urllib.parse
import tempfile
import uuid
import copy
from typing import Optional, Tuple

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict

from daq_data.server import serve
from daq_data.client import AioDaqDataClient, DaqDataClient
from daq_data.daq_data_pb2 import PanoImage
from panoseti_util import control_utils as util

TEST_CFG_DIR = Path("tests/daq_data/config")
TEST_CFG_DIR.mkdir(exist_ok=True)

def is_utility_available(name):
    """Check if a command-line utility is in the system PATH."""
    return subprocess.run(["which", name], capture_output=True).returncode == 0


@pytest.fixture(scope="session")
def hashpipe_pcap_runner():
    """
    A session-scoped fixture that creates a realistic hashpipe run environment,
    starts tcpreplay to feed it data from a pcap file, and launches the
    hashpipe process with command-line arguments that mimic a production run.

    This enables end-to-end testing of the data flow:
    tcpreplay -> hashpipe (net_thread) -> UDS -> gRPC Server -> Client
    """
    if not os.getenv("RUN_REAL_DATA_TESTS"):
        pytest.skip("Skipping real data flow tests. Set RUN_REAL_DATA_TESTS=1 to enable.")
    if not is_utility_available("hashpipe") or not is_utility_available("tcpreplay"):
        pytest.fail("Required utility 'hashpipe' or 'tcpreplay' not found in PATH.")

    # --- 1. Define Paths and Configuration ---
    pcap_file = "/app/test_data.pcapng"

    # Define a base directory for the test, which will be the Current Working Directory (CWD)
    # for the hashpipe process. This matches the behavior of the production start_daq.py script.
    base_dir = Path("/tmp/ci_run_dir")

    # Define a relative run name. It must start with "obs_" for the server to find it.
    run_name = "obs_ci_run"

    # Create the directory structure that `make_run_dirs` in start.py would create.
    # The gRPC server (via HpIoManager) will look for data inside base_dir/module_X/run_name/
    module_ids = [1, 254]
    cfg_str = ""
    for mid in module_ids:
        module_dir = base_dir / "module_{}".format(mid) / run_name
        module_dir.mkdir(parents=True, exist_ok=True)
        cfg_str += f"{mid}\n"

    # The config file for hashpipe goes in base_dir/run_name/
    config_dir = base_dir / run_name
    config_dir.mkdir(exist_ok=True)

    # The module.config file tells hashpipe which module to listen for.
    module_config_path = config_dir / "module.config"
    with open(module_config_path, "w") as f:
        f.write(cfg_str)

    # --- 2. Build Commands ---
    # Command to loop the pcap file to the loopback interface, simulating network traffic.
    tcpreplay_cmd = [
        "tcpreplay",
        "--mbps=1",
        "--loop=0",  # Loop indefinitely
        "--intf1=lo",  # Send to loopback interface
        pcap_file
    ]

    # This command now uses relative paths for RUNDIR and CONFIG, matching production.
    hashpipe_cmd = [
        "hashpipe",
        "-p", "hashpipe.so",
        "-I", "0",
        "-o", "BINDHOST=lo",
        "-o", f"RUNDIR={run_name}",
        "-o", f"CONFIG={run_name}/module.config",
        "-o", "MAXFILESIZE=1",
        "-o", "GROUPPHFRAMES=0",
        "-o", "OBS=TEST",
        "net_thread", "compute_thread", "output_thread"
    ]

    # --- 3. Start Processes ---
    # Start tcpreplay to generate UDP packets.
    tcpreplay_proc = subprocess.Popen(tcpreplay_cmd)#, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Start the hashpipe process with the CWD set to the base directory.
    hashpipe_proc = subprocess.Popen(
        hashpipe_cmd,
        cwd=base_dir
    )

    # --- 4. Wait for Initialization and Validation ---
    num_retries = 20
    for i in range(num_retries):
        # Allow time for processes to initialize and sockets to be created.
        if tcpreplay_proc.poll() is not None:
            pytest.fail(f"tcpreplay failed to start. Exit code: {tcpreplay_proc.returncode}")
        elif hashpipe_proc.poll() is not None:
            pytest.fail(f"hashpipe failed to start. Exit code: {hashpipe_proc.returncode}. Check logs in {base_dir}.")
        if util.is_hashpipe_running():
            print(f"hashpipe is running after {i} retries.")
            break
        print(f"hashpipe is not running after {i}/{num_retries} retries. Retrying in 1 second.")
        time.sleep(1)
    else:
        pytest.fail(f"hashpipe failed to start after {num_retries} retries. Check logs in {base_dir}.")

    # Yield to let the tests run
    yield

    # --- 5. Teardown ---
    print("\n-- Tearing down hashpipe and tcpreplay processes --")

    # First, stop tcpreplay so it stops feeding data.
    tcpreplay_proc.terminate()
    try:
        tcpreplay_proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        tcpreplay_proc.kill()

    # Now, run stop_daq.py to gracefully shut down hashpipe.
    # The Dockerfile places project source code in /app.
    stop_daq_script_path = "/app/panoseti_util/stop_daq.py"
    if os.path.exists(stop_daq_script_path):
        print(f"-- Running {stop_daq_script_path} in {base_dir} --")
        # stop_daq.py expects to be run from the data dir (which is our run_dir)
        # and reads the PID from a file in its cwd.
        try:
            completed_process = subprocess.run(
                [sys.executable, stop_daq_script_path],
                cwd=base_dir,
                capture_output=True,
                text=True,
                timeout=10  # Add a timeout to prevent hanging
            )
            print(f"stop_daq.py stdout:\n{completed_process.stdout}")
            print(f"stop_daq.py stderr:\n{completed_process.stderr}")
            if completed_process.returncode != 0:
                # If it fails, fall back to killing the process directly.
                print("stop_daq.py failed, falling back to direct process termination.")
                if hashpipe_proc.poll() is None:
                    hashpipe_proc.send_signal(signal.SIGINT)  # Graceful shutdown
                    try:
                        hashpipe_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        hashpipe_proc.kill()  # Forceful shutdown
        except subprocess.TimeoutExpired:
            print("stop_daq.py timed out. Killing hashpipe process directly.")
            if hashpipe_proc.poll() is None:
                hashpipe_proc.kill()
    else:
        print(f"{stop_daq_script_path} not found. Terminating hashpipe process directly.")
        # Fallback to original method if script is not found.
        if hashpipe_proc.poll() is None:
            hashpipe_proc.send_signal(signal.SIGINT)  # Graceful shutdown
            try:
                hashpipe_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                hashpipe_proc.kill()  # Forceful shutdown


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
    dps = ["img8", "img16", "ph256", "ph1024"]
    cfg['acquisition_methods'] = {"uds": {
        "enabled": True,
        "data_products": dps,
        "socket_path_template": "/tmp/hashpipe_grpc.dp_{dp_name}.sock"
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
    A pytest fixture factory that starts N sandboxed simulation server instances.
    Each server runs in its own temporary directory to prevent socket conflicts.
    """
    all_server_details = []
    # This list will hold TemporaryDirectory objects for automatic cleanup
    temp_dirs = []

    async def _factory(num_servers: int, uds_paths: Optional[list[str]] = None):
        # The uds_paths argument is now ignored in favor of automatic temp dirs.

        for i in range(num_servers):
            # 1. Create a unique temporary directory for this server instance.
            temp_dir = tempfile.TemporaryDirectory()
            temp_dirs.append(temp_dir)
            temp_dir_path = Path(temp_dir.name)

            config = copy.deepcopy(server_config_base)
            module_id = 200 + i

            # 2. Modify paths in the config to point into the unique temp directory.
            # Main gRPC socket for the client to connect to.
            uds_path_str = f"unix://{temp_dir_path / 'grpc_test.sock'}"
            config["unix_domain_socket"] = uds_path_str

            # Data-plane UDS socket template for the internal simulation data flow.
            uds_cfg = config['acquisition_methods']['uds']
            template_basename = Path(uds_cfg['socket_path_template']).name
            new_template_path = str(temp_dir_path / template_basename)
            uds_cfg['socket_path_template'] = new_template_path

            # Update other configuration details for this specific instance.
            config['simulate_daq_cfg']['sim_module_ids'] = [module_id]
            config['simulate_daq_cfg']['simulation_mode'] = 'uds'
            config['acquisition_methods']['uds']['enabled'] = True

            uds_path = Path(urllib.parse.urlparse(uds_path_str).path)
            shutdown_event = asyncio.Event()
            server_task = asyncio.create_task(serve(config, shutdown_event, in_main_thread=False))

            # Readiness check
            async with AioDaqDataClient({"daq_nodes": [{"ip_addr": uds_path_str}]}, network_config=None) as client:
                for _ in range(30):  # 3-second timeout
                    if uds_path.exists() and await client.ping(uds_path_str):
                        break
                    await asyncio.sleep(0.1)
                else:
                    pytest.fail(f"Server {i} gRPC endpoint did not become ready.", pytrace=False)

            server_details = {
                "ip_addr": uds_path_str,
                "task": server_task,
                "stop_event": shutdown_event,
                "module_id": module_id,
            }
            all_server_details.append(server_details)
        return all_server_details

    try:
        yield _factory
    finally:
        # Stop all gRPC server tasks
        server_tasks = [sd['task'] for sd in all_server_details if sd.get('task')]
        for sd in all_server_details:
            if sd.get('stop_event'):
                sd['stop_event'].set()

        if server_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*server_tasks), timeout=5.0)
            except asyncio.TimeoutError:
                for task in server_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*server_tasks, return_exceptions=True)

        # 3. Clean up all temporary directories and their contents.
        for temp_dir in temp_dirs:
            temp_dir.cleanup()


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
