"""
Unit tests for @grpc_call decorator and typed exception mapping on AsyncDaqControlClient.

Verifies:
- UnavailableError raised for UNAVAILABLE RPCs
- FailedPreconditionError raised for FAILED_PRECONDITION RPCs
- DeadlineExceededError raised for DEADLINE_EXCEEDED RPCs
- CancelledError is never suppressed
- client_models Pydantic validation fires before hitting the network
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from panoseti_grpc.daq_control.client import AsyncDaqControlClient
from panoseti_grpc.grpc_utils.exceptions import (
    DeadlineExceededError,
    FailedPreconditionError,
    PanosetiRpcError,
    UnavailableError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeRpcError(grpc.RpcError, Exception):
    """A minimal grpc.RpcError that is also a real Python exception."""

    def __init__(self, code: grpc.StatusCode, details: str = "test error") -> None:
        super().__init__(details)
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def make_rpc_error(code: grpc.StatusCode, details: str = "test") -> _FakeRpcError:
    """Create a concrete grpc.RpcError instance with the given status code."""
    return _FakeRpcError(code, details)


def mock_client_with_stub(stub_mock: Any) -> AsyncDaqControlClient:
    """Return an AsyncDaqControlClient whose stub is pre-set."""
    client = AsyncDaqControlClient.__new__(AsyncDaqControlClient)
    client.target = "localhost:50051"
    client._channel = MagicMock()
    client._stub = stub_mock
    return client


_START_DAQ_PARAMS: dict[str, Any] = {
    "data_dir": "/data",
    "daq_ip_addr": "192.168.0.10",
    "bindhost": "0.0.0.0",
    "max_file_size_mb": 1024,
    "group_ph_frames": False,
    "run_dir": "run.pffd",
    "obs": "test_obs",
    "module_id": [250],
}

# ---------------------------------------------------------------------------
# @grpc_call — UNAVAILABLE → UnavailableError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startdaq_unavailable_raises_unavailable_error() -> None:
    """StartDaq raising UNAVAILABLE gRPC error is mapped to UnavailableError."""
    stub = MagicMock()
    stub.StartDaq = AsyncMock(side_effect=make_rpc_error(grpc.StatusCode.UNAVAILABLE))
    client = mock_client_with_stub(stub)

    with pytest.raises(UnavailableError):
        await client.StartDaq(_START_DAQ_PARAMS)


@pytest.mark.asyncio
async def test_stopdaqs_unavailable_raises_unavailable_error() -> None:
    """StopDaq raising UNAVAILABLE is mapped to UnavailableError."""
    stub = MagicMock()
    stub.StopDaq = AsyncMock(side_effect=make_rpc_error(grpc.StatusCode.UNAVAILABLE))
    client = mock_client_with_stub(stub)

    with pytest.raises(UnavailableError):
        await client.StopDaq({"data_dir": "/data", "run_dir": "run.pffd"})


# ---------------------------------------------------------------------------
# @grpc_call — FAILED_PRECONDITION → FailedPreconditionError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanupdata_failed_precondition_raises_typed_error() -> None:
    """CleanupData with wrong manifest_digest raises FailedPreconditionError."""
    stub = MagicMock()
    stub.CleanupData = AsyncMock(
        side_effect=make_rpc_error(grpc.StatusCode.FAILED_PRECONDITION, "manifest digest mismatch")
    )
    client = mock_client_with_stub(stub)

    with pytest.raises(FailedPreconditionError) as exc_info:
        await client.CleanupData({
            "data_dir": "/data",
            "run_dir": "run.pffd",
            "module_id": [250],
            "mode": "CLEANUP_SELECTIVE",
            "delete_patterns": ["*.pff"],
            "preserve_patterns": ["*.json"],
            "manifest_digest": "wrong_digest",
        })

    assert "FAILED_PRECONDITION" in str(exc_info.value)


# ---------------------------------------------------------------------------
# @grpc_call — DEADLINE_EXCEEDED → DeadlineExceededError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generatemanifest_deadline_raises_deadline_error() -> None:
    """GenerateManifest timeout raises DeadlineExceededError."""
    stub = MagicMock()
    stub.GenerateManifest = AsyncMock(
        side_effect=make_rpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)
    )
    client = mock_client_with_stub(stub)

    with pytest.raises(DeadlineExceededError):
        await client.GenerateManifest({
            "data_dir": "/data",
            "run_dir": "run.pffd",
            "module_id": 250,
            "algorithm": "blake3",
        })


# ---------------------------------------------------------------------------
# @grpc_call — CancelledError is not suppressed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_grpc_call_does_not_suppress_cancelled_error() -> None:
    """asyncio.CancelledError must propagate through @grpc_call unwrapped."""
    stub = MagicMock()
    stub.StartDaq = AsyncMock(side_effect=asyncio.CancelledError())
    client = mock_client_with_stub(stub)

    with pytest.raises(asyncio.CancelledError):
        await client.StartDaq(_START_DAQ_PARAMS)


# ---------------------------------------------------------------------------
# PanosetiRpcError carries structured fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unavailable_error_carries_code_and_details() -> None:
    """UnavailableError.code and .details are populated from the original RPC error."""
    stub = MagicMock()
    stub.StopDaq = AsyncMock(
        side_effect=make_rpc_error(grpc.StatusCode.UNAVAILABLE, "connection refused")
    )
    client = mock_client_with_stub(stub)

    with pytest.raises(UnavailableError) as exc_info:
        await client.StopDaq({"data_dir": "/data", "run_dir": "run.pffd"})

    err = exc_info.value
    assert err.code == grpc.StatusCode.UNAVAILABLE
    assert "connection refused" in err.details
    assert isinstance(err, PanosetiRpcError)


# ---------------------------------------------------------------------------
# client_models Pydantic validation (fires before network)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startdaq_invalid_module_id_raises_validation_error() -> None:
    """StartDaq with an invalid module_id raises Pydantic ValidationError before RPC."""
    from pydantic import ValidationError

    stub = MagicMock()
    stub.StartDaq = AsyncMock()
    client = mock_client_with_stub(stub)

    with pytest.raises(ValidationError):
        await client.StartDaq({
            **_START_DAQ_PARAMS,
            "module_id": "not_a_list",  # wrong type
        })

    stub.StartDaq.assert_not_called()


@pytest.mark.asyncio
async def test_cleanupdata_missing_required_field_raises_validation_error() -> None:
    """CleanupData without data_dir raises Pydantic ValidationError before RPC."""
    from pydantic import ValidationError

    stub = MagicMock()
    stub.CleanupData = AsyncMock()
    client = mock_client_with_stub(stub)

    with pytest.raises(ValidationError):
        await client.CleanupData({
            # missing data_dir
            "run_dir": "run.pffd",
            "module_id": [250],
        })

    stub.CleanupData.assert_not_called()
