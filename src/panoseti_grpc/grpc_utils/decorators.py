from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any, TypeVar

import grpc

from .exceptions import from_rpc_error

F = TypeVar("F", bound=Callable[..., Any])


def grpc_call(fn: F) -> F:
    """Decorator that maps grpc.RpcError → PanosetiRpcError on any client method.

    Handles three call shapes:
    - async generator methods (e.g. GetManifest that yields entries)
    - regular async coroutine methods
    - synchronous methods

    CancelledError is never caught — cooperative cancellation is preserved.
    The decorated method must be a bound method whose first arg is self;
    self.target (str) is used as the error target label.
    """
    if inspect.isasyncgenfunction(fn):

        @functools.wraps(fn)
        async def _agen_wrapper(self: Any, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
            target: str = getattr(self, "target", "unknown")
            try:
                async for item in fn(self, *args, **kwargs):
                    yield item
            except grpc.RpcError as e:
                raise from_rpc_error(e, target) from e

        return _agen_wrapper  # type: ignore[return-value]

    elif asyncio.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def _async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            target: str = getattr(self, "target", "unknown")
            try:
                return await fn(self, *args, **kwargs)
            except grpc.RpcError as e:
                raise from_rpc_error(e, target) from e

        return _async_wrapper  # type: ignore[return-value]

    else:

        @functools.wraps(fn)
        def _sync_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            target: str = getattr(self, "target", "unknown")
            try:
                return fn(self, *args, **kwargs)
            except grpc.RpcError as e:
                raise from_rpc_error(e, target) from e

        return _sync_wrapper  # type: ignore[return-value]
