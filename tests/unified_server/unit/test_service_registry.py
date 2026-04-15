"""
Unit tests for ServiceRegistry, ServiceDescriptor, and ServiceToggles.

Verifies the extensibility contract of the unified server's service registry.
"""

import pytest

from panoseti_grpc.server import (
    PanosetiServer,
    ServiceDescriptor,
    ServiceRegistry,
    ServiceToggles,
)

# ---------------------------------------------------------------------------
# Registry contents
# ---------------------------------------------------------------------------


def test_all_three_services_registered():
    """All three built-in services are registered at module import time."""
    names = set(ServiceRegistry.all().keys())
    assert names == {"telemetry", "daq_data", "daq_control"}


def test_init_order_telemetry_first():
    """Telemetry must be the first service in INIT_ORDER (live before others log)."""
    assert PanosetiServer.INIT_ORDER[0] == "telemetry"


def test_init_order_contains_all_registered_services():
    """Every registered service appears in INIT_ORDER."""
    registered = set(ServiceRegistry.all().keys())
    in_order = set(PanosetiServer.INIT_ORDER)
    assert registered == in_order


def test_init_order_has_no_duplicates():
    """INIT_ORDER must not contain duplicate entries."""
    assert len(PanosetiServer.INIT_ORDER) == len(set(PanosetiServer.INIT_ORDER))


# ---------------------------------------------------------------------------
# ServiceDescriptor fields
# ---------------------------------------------------------------------------


def test_registry_get_daq_data():
    """daq_data descriptor has correct name and config_field."""
    desc = ServiceRegistry.get("daq_data")
    assert desc.name == "daq_data"
    assert desc.config_field == "daq_data"
    assert callable(desc.servicer_factory)
    assert callable(desc.add_to_server_fn)


def test_registry_get_telemetry():
    """telemetry descriptor has correct name and config_field."""
    desc = ServiceRegistry.get("telemetry")
    assert desc.name == "telemetry"
    assert desc.config_field == "telemetry"
    assert callable(desc.servicer_factory)
    assert callable(desc.add_to_server_fn)


def test_registry_get_daq_control():
    """daq_control descriptor has correct name and config_field."""
    desc = ServiceRegistry.get("daq_control")
    assert desc.name == "daq_control"
    assert desc.config_field == "daq_control"


def test_registry_service_names_for_reflection_telemetry():
    """Telemetry reflection names contain the fully-qualified service name."""
    desc = ServiceRegistry.get("telemetry")
    assert any("Telemetry" in n for n in desc.service_names_for_reflection)


def test_registry_service_names_for_reflection_daq_data():
    """DaqData reflection names contain the fully-qualified service name."""
    desc = ServiceRegistry.get("daq_data")
    assert any("DaqData" in n for n in desc.service_names_for_reflection)


def test_registry_service_names_for_reflection_daq_control():
    """DaqControl reflection names contain the fully-qualified service name."""
    desc = ServiceRegistry.get("daq_control")
    assert any("DaqControl" in n for n in desc.service_names_for_reflection)


def test_registry_all_returns_copy():
    """ServiceRegistry.all() returns a copy, not the internal dict."""
    d1 = ServiceRegistry.all()
    d2 = ServiceRegistry.all()
    assert d1 is not d2
    assert d1 == d2


def test_registry_get_unknown_service():
    """ServiceRegistry.get() on an unknown name raises KeyError."""
    with pytest.raises(KeyError):
        ServiceRegistry.get("nonexistent_service")


# ---------------------------------------------------------------------------
# Custom registration (extension point)
# ---------------------------------------------------------------------------


def test_registry_register_custom_and_retrieve():
    """A new ServiceDescriptor can be registered and retrieved by name."""
    dummy_descriptor = ServiceDescriptor(
        name="dummy_test_svc",
        servicer_factory=lambda cfg, ev: None,
        add_to_server_fn=lambda svc, srv: None,
        service_names_for_reflection=["dummy.test.Dummy"],
        config_field="dummy_test_svc",
    )
    ServiceRegistry.register(dummy_descriptor)
    try:
        retrieved = ServiceRegistry.get("dummy_test_svc")
        assert retrieved.name == "dummy_test_svc"
        assert "dummy.test.Dummy" in retrieved.service_names_for_reflection
    finally:
        # Clean up: remove the test registration to avoid polluting other tests
        ServiceRegistry._registry.pop("dummy_test_svc", None)


def test_registry_register_overwrites_existing():
    """Re-registering a name replaces the previous descriptor."""
    original = ServiceRegistry.get("daq_control")
    replacement = ServiceDescriptor(
        name="daq_control",
        servicer_factory=lambda cfg, ev: None,
        add_to_server_fn=lambda svc, srv: None,
        service_names_for_reflection=["replaced"],
        config_field="daq_control",
    )
    ServiceRegistry.register(replacement)
    try:
        assert ServiceRegistry.get("daq_control").service_names_for_reflection == ["replaced"]
    finally:
        # Restore original
        ServiceRegistry.register(original)


# ---------------------------------------------------------------------------
# ServiceToggles
# ---------------------------------------------------------------------------


def test_service_toggles_all_true_by_default():
    """Default ServiceToggles enables all three services."""
    t = ServiceToggles()
    assert t.telemetry and t.daq_data and t.daq_control


def test_service_toggles_partial():
    """ServiceToggles can selectively disable services."""
    t = ServiceToggles(telemetry=False, daq_data=True, daq_control=False)
    assert not t.telemetry
    assert t.daq_data
    assert not t.daq_control


def test_service_toggles_all_false():
    """All-false ServiceToggles is a valid model (server will raise at start time)."""
    t = ServiceToggles(telemetry=False, daq_data=False, daq_control=False)
    assert not t.telemetry
    assert not t.daq_data
    assert not t.daq_control


def test_service_toggles_equality():
    """Two ServiceToggles with identical fields compare as equal."""
    t1 = ServiceToggles(telemetry=True, daq_data=False, daq_control=True)
    t2 = ServiceToggles(telemetry=True, daq_data=False, daq_control=True)
    assert t1 == t2
