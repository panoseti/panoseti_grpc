"""Shared gRPC error-handling utilities for all panoseti_grpc services."""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
from collections.abc import Callable
from typing import Any, cast

import grpc


def grpc_error_handler[F: Callable[..., Any]](func: F) -> F:
    """Decorator: catches unexpected exceptions in gRPC handlers and aborts with INTERNAL.

    CancelledError is always re-raised so that task cancellation is never suppressed.
    Works for both regular async handlers and async generator (server-streaming) handlers.
    """

    if inspect.isasyncgenfunction(func):
        # Server-streaming RPC: the handler is an async generator (uses yield).
        # We must wrap it as an async generator too — you cannot await a generator.
        @functools.wraps(func)
        async def agen_wrapper(self: Any, request: Any, context: grpc.aio.ServicerContext) -> Any:
            try:
                async for item in func(self, request, context):
                    yield item
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.exception(f"Error in {func.__name__}: {e!s}")
                await context.abort(grpc.StatusCode.INTERNAL, f"Internal server error: {e!s}")

        return cast(F, agen_wrapper)

    @functools.wraps(func)
    async def wrapper(self: Any, request: Any, context: grpc.aio.ServicerContext) -> Any:
        try:
            return await func(self, request, context)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.exception(f"Error in {func.__name__}: {e!s}")
            await context.abort(grpc.StatusCode.INTERNAL, f"Internal server error: {e!s}")

    return cast(F, wrapper)
