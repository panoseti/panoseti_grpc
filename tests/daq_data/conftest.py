import asyncio
import copy
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

import grpc
import pytest
import pytest_asyncio
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from panoseti_grpc.daq_data.client import AioDaqDataClient, DaqDataClient
from panoseti_grpc.daq_data.server import serve
from panoseti_grpc.generated import daq_data_pb2_grpc
from panoseti_grpc.generated.daq_data_pb2 import PanoImage
from panoseti_grpc.grpc_utils.health import HealthClient

TEST_CFG_DIR = Path("tests/daq_data/config")
TEST_CFG_DIR.mkdir(exist_ok=True)

_HEALTH_SERVICE = "daqdata.DaqData"


@pytest.fixture(scope="session")
def server_config_base() -> dict[str, Any]:
    """Provides a base server configuration dictionary."""
    with open(TEST_CFG_DIR / "daq_data_server_config.json") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def uds_sim_server_config(server_config_base: dict[str, Any]) -> dict[str, Any]:
    cfg = copy.deepcopy(server_config_base)
    cfg["simulate_daq_cfg"]["simulation_mode"] = "uds"
    dps = ["img8", "img16", "ph256", "ph1024"]
    cfg["acquisition_methods"] = {
        "uds": {"enabled": True, "data_products": dps, "socket_path_template": "/tmp/hashpipe_grpc.dp_{dp_name}.sock"}
    }
    cfg["simulate_daq_cfg"]["strategies"] = {"uds": {"data_products": dps, "sim_module_ids": [224]}}
    return cfg


async def _wait_for_server(host: str, port: int, *, attempts: int = 40) -> None:
    """Poll the health endpoint until the server reports SERVING."""
    hc = HealthClient(host, port)
    for _ in range(attempts):
        if await asyncio.to_thread(hc.check, _HEALTH_SERVICE, 1.0):
            return
        await asyncio.sleep(0.1)
    pytest.fail(f"Server at {host}:{port} did not become ready in time.", pytrace=False)


async def _start_edge_server(config: dict[str, Any]) -> dict[str, Any]:
    """Start a single edge server; return its details dict."""
    cfg = copy.deepcopy(config)
    cfg["unix_domain_socket"] = None  # use TCP transport
    bound_port: list[int] = []
    shutdown_event = asyncio.Event()
    task = asyncio.create_task(serve(cfg, shutdown_event, in_main_thread=False, port=0, bound_port_out=bound_port))
    while not bound_port:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    tcp_port = bound_port[0]
    await _wait_for_server("localhost", tcp_port)
    return {"host": "localhost", "port": tcp_port, "task": task, "stop_event": shutdown_event}


@pytest_asyncio.fixture(scope="function")
async def sim_server_process(request: Any) -> Any:
    """Parameterized fixture to start a server with a specific simulation config."""
    config = request.getfixturevalue(request.param)
    details = await _start_edge_server(config)
    yield {"host": details["host"], "port": details["port"]}
    details["stop_event"].set()
    try:
        await asyncio.wait_for(details["task"], timeout=5.0)
    except TimeoutError:
        details["task"].cancel()
        await asyncio.gather(details["task"], return_exceptions=True)


@pytest_asyncio.fixture(scope="function")
async def default_server_process(uds_sim_server_config: dict[str, Any]) -> Any:
    """A non-parameterized fixture that runs a standard RPC simulation server."""
    assert os.name == "posix", "Only supported on POSIX systems."

    with tempfile.TemporaryDirectory() as td:
        config = copy.deepcopy(uds_sim_server_config)
        uds_cfg = config["acquisition_methods"]["uds"]
        template_basename = Path(uds_cfg["socket_path_template"]).name
        uds_cfg["socket_path_template"] = str(Path(td) / template_basename)

        details = await _start_edge_server(config)
        try:
            yield {
                "host": details["host"],
                "port": details["port"],
                "data_dir": "daq_data/simulated_data_dir",
                "stop_event": details["stop_event"],
            }
        finally:
            details["stop_event"].set()
            try:
                await asyncio.wait_for(details["task"], timeout=5.0)
            except TimeoutError:
                details["task"].cancel()
                await asyncio.gather(details["task"], return_exceptions=True)


@pytest_asyncio.fixture(scope="function")
async def n_sim_servers_fixture_factory(server_config_base: dict[str, Any]) -> Any:
    """
    Fixture factory that starts N sandboxed simulation server instances.
    Each server runs in its own temporary directory.
    """
    all_server_details: list[dict[str, Any]] = []
    temp_dirs: list[tempfile.TemporaryDirectory] = []

    async def _factory(num_servers: int, uds_paths: list[str] | None = None) -> list[dict[str, Any]]:
        for _ in range(num_servers):
            temp_dir = tempfile.TemporaryDirectory()
            temp_dirs.append(temp_dir)
            temp_dir_path = Path(temp_dir.name)

            config = copy.deepcopy(server_config_base)
            module_id = 200 + len(all_server_details)

            uds_cfg = config["acquisition_methods"]["uds"]
            template_basename = Path(uds_cfg["socket_path_template"]).name
            uds_cfg["socket_path_template"] = str(temp_dir_path / template_basename)
            config["simulate_daq_cfg"]["sim_module_ids"] = [module_id]
            config["simulate_daq_cfg"]["simulation_mode"] = "uds"
            config["acquisition_methods"]["uds"]["enabled"] = True

            details = await _start_edge_server(config)
            all_server_details.append({**details, "module_id": module_id})

        return list(all_server_details)

    try:
        yield _factory
    finally:
        for sd in all_server_details:
            if sd.get("stop_event"):
                sd["stop_event"].set()
        tasks = [sd["task"] for sd in all_server_details if sd.get("task")]
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
            except TimeoutError:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        for td in temp_dirs:
            td.cleanup()


@pytest_asyncio.fixture(scope="function")
async def gateway_factory(server_config_base: dict[str, Any]) -> Any:
    """
    Fixture factory for gateway E2E tests.
    ``await gateway_factory(num_edges)`` starts N edge servers + a gateway
    and returns ``{"host": ..., "port": ..., "edge_details": [...]}``.
    """
    all_edge_details: list[dict[str, Any]] = []
    temp_dirs: list[tempfile.TemporaryDirectory] = []
    gw_server: grpc.aio.Server | None = None
    gw_servicer: Any = None

    async def _make(num_edges: int) -> dict[str, Any]:
        nonlocal gw_server, gw_servicer

        for _ in range(num_edges):
            temp_dir = tempfile.TemporaryDirectory()
            temp_dirs.append(temp_dir)
            temp_dir_path = Path(temp_dir.name)

            config = copy.deepcopy(server_config_base)
            module_id = 200 + len(all_edge_details)

            uds_cfg = config["acquisition_methods"]["uds"]
            template_basename = Path(uds_cfg["socket_path_template"]).name
            uds_cfg["socket_path_template"] = str(temp_dir_path / template_basename)
            config["simulate_daq_cfg"]["sim_module_ids"] = [module_id]
            config["simulate_daq_cfg"]["simulation_mode"] = "uds"
            config["acquisition_methods"]["uds"]["enabled"] = True

            details = await _start_edge_server(config)
            all_edge_details.append({**details, "module_id": module_id})

        # Write daq/network configs for the gateway so it knows the edge endpoints
        gw_temp_dir = tempfile.TemporaryDirectory()
        temp_dirs.append(gw_temp_dir)
        gw_tmp = Path(gw_temp_dir.name)

        daq_cfg_data = {
            "daq_nodes": [
                {"ip_addr": f"10.0.100.{i + 1}", "data_dir": "/tmp", "username": "test"} for i in range(num_edges)
            ]
        }
        net_cfg_data = {
            "daq_nodes": [
                {
                    "ip_addr": f"10.0.100.{i + 1}",
                    "port_forwarding": {
                        "status": True,
                        "gw_ip": d["host"],
                        "grpc_port": d["port"],
                    },
                }
                for i, d in enumerate(all_edge_details[-num_edges:])
            ]
        }
        daq_cfg_path = gw_tmp / "daq_config.json"
        net_cfg_path = gw_tmp / "network_config.json"
        daq_cfg_path.write_text(json.dumps(daq_cfg_data))
        net_cfg_path.write_text(json.dumps(net_cfg_data))

        from panoseti_grpc.daq_data.aggregator import DaqDataGatewayServicer
        from panoseti_grpc.daq_data.config import DaqDataGatewayConfig, DaqDataServerConfig
        from panoseti_grpc.generated import daq_data_pb2
        from panoseti_grpc.grpc_utils.health import register_health

        gw_cfg = DaqDataServerConfig(
            role="gateway",
            gateway=DaqDataGatewayConfig(
                daq_config_path=str(daq_cfg_path),
                network_config_path=str(net_cfg_path),
            ),
        )
        gw_servicer = DaqDataGatewayServicer(gw_cfg)
        gw_server = grpc.aio.server()
        daq_data_pb2_grpc.add_DaqDataServicer_to_server(gw_servicer, gw_server)
        register_health(gw_server, [daq_data_pb2.DESCRIPTOR.services_by_name["DaqData"].full_name])

        gw_port = gw_server.add_insecure_port("[::]:0")
        await gw_server.start()
        await gw_servicer.startup()
        await _wait_for_server("localhost", gw_port)

        return {"host": "localhost", "port": gw_port, "edge_details": list(all_edge_details)}

    try:
        yield _make
    finally:
        if gw_servicer is not None:
            await gw_servicer.shutdown()
        if gw_server is not None:
            await gw_server.stop(0)
        for sd in all_edge_details:
            if sd.get("stop_event"):
                sd["stop_event"].set()
        tasks = [sd["task"] for sd in all_edge_details if sd.get("task")]
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
            except TimeoutError:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        for td in temp_dirs:
            td.cleanup()


@pytest_asyncio.fixture
async def async_client(default_server_process: Any) -> Any:
    """Provides a connected AioDaqDataClient for API tests."""
    async with AioDaqDataClient(
        default_server_process["host"],
        default_server_process["port"],
        log_level=logging.DEBUG,
    ) as client:
        yield client


@pytest.fixture
def sync_client(default_server_process: Any) -> Any:
    """Provides a connected DaqDataClient for API tests."""
    with DaqDataClient(
        default_server_process["host"],
        default_server_process["port"],
        log_level=logging.DEBUG,
    ) as client:
        yield client


@pytest.fixture
def sample_pano_image() -> PanoImage:
    header_dict = {"test_field": "test_value"}
    return PanoImage(
        type=PanoImage.Type.MOVIE,
        header=ParseDict(header_dict, Struct()),
        image_array=[i for i in range(256)],
        shape=[16, 16],
        bytes_per_pixel=1,
        file="test_upload.pff",
        module_id=101,
    )
