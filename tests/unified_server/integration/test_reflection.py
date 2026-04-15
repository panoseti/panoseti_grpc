"""
Integration tests: gRPC server reflection for all deployment profiles.

Uses the grpc-reflection v1alpha client to verify that each profile
exposes the correct set of service names and no others.
"""

from typing import Any
from __future__ import annotations

import grpc
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

from tests.unified_server.conftest import (
    DAQ_NODE_PORT,
    GRPC_PORT,
    HEADNODE_PORT,
)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def get_reflected_services(host: str, port: int, timeout: float = 10.0)-> set[str]:
    """Query gRPC reflection and return the set of advertised service names."""
    with grpc.insecure_channel(f"{host}:{port}") as channel:
        stub = reflection_pb2_grpc.ServerReflectionStub(channel)
        request = reflection_pb2.ServerReflectionRequest(list_services="")
        response_iter = stub.ServerReflectionInfo(iter([request]))
        services: set[str] = set()
        for resp in response_iter:
            for svc in resp.list_services_response.service:
                services.add(svc.name)
        return services


# ---------------------------------------------------------------------------
# All-services unified server
# ---------------------------------------------------------------------------


def test_unified_reflection_includes_all_services( start_unified_server: Any) -> None:
    """The all-services server advertises all three services via reflection."""
    services = get_reflected_services("localhost", GRPC_PORT)
    assert any("Telemetry" in s for s in services), f"Telemetry not in reflection: {services}"
    assert any("DaqData" in s for s in services), f"DaqData not in reflection: {services}"
    assert any("DaqControl" in s for s in services), f"DaqControl not in reflection: {services}"


def test_unified_reflection_includes_reflection_service( start_unified_server: Any) -> None:
    """The reflection service itself is included in the reflection response."""
    services = get_reflected_services("localhost", GRPC_PORT)
    assert any("reflection" in s.lower() for s in services), f"Reflection service not advertised: {services}"


# ---------------------------------------------------------------------------
# Headnode profile: telemetry only
# ---------------------------------------------------------------------------


def test_headnode_reflection_includes_only_telemetry( start_headnode_server: Any) -> None:
    """Headnode profile advertises only the Telemetry service."""
    services = get_reflected_services("localhost", HEADNODE_PORT)
    assert any("Telemetry" in s for s in services), f"Telemetry not in headnode reflection: {services}"
    assert not any("DaqData" in s for s in services), f"DaqData should not appear in headnode reflection: {services}"
    assert not any("DaqControl" in s for s in services), (
        f"DaqControl should not appear in headnode reflection: {services}"
    )


# ---------------------------------------------------------------------------
# DAQ node profile: daq_data + daq_control
# ---------------------------------------------------------------------------


def test_daq_node_reflection_excludes_telemetry( start_daq_node_server: Any) -> None:
    """DAQ node profile does not advertise the Telemetry service."""
    services = get_reflected_services("localhost", DAQ_NODE_PORT)
    assert not any("Telemetry" in s for s in services), (
        f"Telemetry should not appear in daq_node reflection: {services}"
    )


def test_daq_node_reflection_includes_daq_services( start_daq_node_server: Any) -> None:
    """DAQ node profile advertises DaqData and DaqControl via reflection."""
    services = get_reflected_services("localhost", DAQ_NODE_PORT)
    assert any("DaqData" in s for s in services), f"DaqData not in daq_node reflection: {services}"
    assert any("DaqControl" in s for s in services), f"DaqControl not in daq_node reflection: {services}"
