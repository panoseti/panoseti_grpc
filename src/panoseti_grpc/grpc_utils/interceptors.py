"""gRPC interceptor stubs for PANOSETI services.

These are lightweight placeholders. Full implementation (logging, deadline
injection, metadata propagation) can be added incrementally without changing
the registration call sites.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import grpc
import grpc.aio


class LoggingClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):  # type: ignore[misc]
    """Logs outgoing RPCs (method, target) at DEBUG level."""

    async def intercept_unary_unary(  # type: ignore[override]
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        return await continuation(client_call_details, request)


class ExceptionServerInterceptor(grpc.aio.ServerInterceptor):  # type: ignore[misc]
    """Catches unhandled server exceptions and aborts with INTERNAL status.

    Mirrors the existing per-RPC @grpc_error_handler decorator in
    util/error_handling.py but applied globally at the server level.
    """

    async def intercept_service(  # type: ignore[override]
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        return await continuation(handler_call_details)
