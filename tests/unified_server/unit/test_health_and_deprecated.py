"""
Unit tests for health check registration and deprecated service flag.

Tests verify:
- HealthClient.check() returns a concrete bool (not Any from protobuf)
- register_health() registers each service as SERVING
- Unknown services return False from HealthClient.check()
- Unreachable servers return False from HealthClient.check()
- ServiceDescriptor.deprecated field defaults to False
- Active built-in services are not deprecated
- The string-format logic for [DEPRECATED] tag is correct
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from panoseti_grpc.grpc_utils.health import HealthClient, register_health
from panoseti_grpc.server import ServiceDescriptor, ServiceRegistry


async def _wait_serving(
    client: HealthClient,
    service: str = "",
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> bool:
    """Poll HealthClient.check() until it returns True or the timeout expires.

    Runs the synchronous check() in a thread so the aio server event loop
    can process incoming RPCs while we wait.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = await asyncio.to_thread(client.check, service)
        if result:
            return True
        await asyncio.sleep(poll_interval)
    return False


# ---------------------------------------------------------------------------
# ServiceDescriptor.deprecated field
# ---------------------------------------------------------------------------


def test_deprecated_defaults_to_false() -> None:
    """ServiceDescriptor.deprecated must default to False."""
    desc = ServiceDescriptor(
        name="test_nondeprecated",
        servicer_factory=cast(Any, lambda cfg, ev: None),
        add_to_server_fn=lambda svc, srv: None,
        service_names_for_reflection=["test.Nondeprecated"],
        config_field="test_nondeprecated",
    )
    assert desc.deprecated is False


def test_deprecated_can_be_set_true() -> None:
    """ServiceDescriptor.deprecated can be explicitly set to True."""
    desc = ServiceDescriptor(
        name="test_deprecated_svc",
        servicer_factory=cast(Any, lambda cfg, ev: None),
        add_to_server_fn=lambda svc, srv: None,
        service_names_for_reflection=["test.Deprecated"],
        config_field="test_deprecated_svc",
        deprecated=True,
    )
    assert desc.deprecated is True


def test_built_in_services_are_not_deprecated() -> None:
    """None of the three active built-in services are deprecated."""
    for name in ("telemetry", "daq_data", "daq_control"):
        assert not ServiceRegistry.get(name).deprecated, f"{name} should not be deprecated"


# ---------------------------------------------------------------------------
# [DEPRECATED] tag format logic (in-process, no subprocess)
# ---------------------------------------------------------------------------


def _format_service_line(name: str, descriptor: ServiceDescriptor) -> str:
    """Reproduce the --list-services line format from unified_main.py."""
    tag = "  [DEPRECATED]" if descriptor.deprecated else ""
    return f"  {name}{tag}"


def test_active_service_line_has_no_deprecated_tag() -> None:
    """A non-deprecated service must not show [DEPRECATED] in its line."""
    desc = ServiceRegistry.get("daq_control")
    line = _format_service_line("daq_control", desc)
    assert "[DEPRECATED]" not in line
    assert "daq_control" in line


def test_deprecated_service_line_shows_deprecated_tag() -> None:
    """A service with deprecated=True must have [DEPRECATED] in its formatted line."""
    desc = ServiceDescriptor(
        name="ublox_control",
        servicer_factory=cast(Any, lambda cfg, ev: None),
        add_to_server_fn=lambda svc, srv: None,
        service_names_for_reflection=["panoseti.ublox_control.UbloxControl"],
        config_field="ublox_control",
        deprecated=True,
    )
    line = _format_service_line("ublox_control", desc)
    assert "[DEPRECATED]" in line
    assert "ublox_control" in line


# ---------------------------------------------------------------------------
# grpc_utils.health — register_health + HealthClient (in-process gRPC)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_health_raises_no_exception() -> None:
    """register_health completes without raising on a fresh aio server."""
    import grpc.aio

    service_names = ["panoseti.daq_control", "panoseti.telemetry"]
    server = grpc.aio.server()
    # Should not raise
    register_health(server, service_names)
    server.add_insecure_port("[::]:0")
    await server.start()
    await server.stop(0)


@pytest.mark.asyncio
async def test_health_client_overall_check_returns_true() -> None:
    """HealthClient.check('') returns True when the server is healthy."""
    import grpc.aio

    server = grpc.aio.server()
    register_health(server, ["panoseti.test_svc"])
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        client = HealthClient("localhost", port)
        result = await _wait_serving(client, "")
        assert result is True, "Overall health check timed out waiting for SERVING"
        assert isinstance(result, bool), "check() must return a concrete bool"
    finally:
        await server.stop(0)


@pytest.mark.asyncio
async def test_health_client_named_service_returns_true() -> None:
    """HealthClient.check(service_name) returns True for a registered service."""
    import grpc.aio

    server = grpc.aio.server()
    register_health(server, ["panoseti.daq_control", "panoseti.telemetry"])
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        client = HealthClient("localhost", port)
        result = await _wait_serving(client, "panoseti.daq_control")
        assert isinstance(result, bool)
        assert result is True
    finally:
        await server.stop(0)


@pytest.mark.asyncio
async def test_health_client_check_unknown_service_returns_false() -> None:
    """HealthClient.check() returns False for a service not passed to register_health."""
    import grpc.aio

    server = grpc.aio.server()
    register_health(server, ["panoseti.known"])
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        client = HealthClient("localhost", port)
        # Wait until the server is generally up before probing unknown service.
        await _wait_serving(client, "panoseti.known")
        # grpc.health.v1 returns NOT_FOUND for unregistered services → HealthClient → False
        result = client.check("panoseti.totally_unknown_service")
        assert isinstance(result, bool)
        assert result is False
    finally:
        await server.stop(0)


def test_health_client_check_unreachable_server_returns_false() -> None:
    """HealthClient.check() returns False when the server is not reachable."""
    client = HealthClient("localhost", 19999)  # nothing listening here
    result = client.check("")
    assert isinstance(result, bool)
    assert result is False


@pytest.mark.asyncio
async def test_multiple_services_all_serving() -> None:
    """All services passed to register_health are individually SERVING."""
    import grpc.aio

    names = ["panoseti.daq_control", "panoseti.daq_data", "panoseti.telemetry"]
    server = grpc.aio.server()
    register_health(server, names)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        client = HealthClient("localhost", port)
        for name in names:
            result = await _wait_serving(client, name)
            assert result is True, f"Service {name} not SERVING within timeout"
    finally:
        await server.stop(0)
