"""Shared gRPC error-handling utilities for all panoseti_grpc services."""
from __future__ import annotations
import asyncio
import functools
import logging

import grpc


def grpc_error_handler(func):
    """Decorator: catches unexpected exceptions in gRPC handlers and aborts with INTERNAL.

    CancelledError is always re-raised so that task cancellation is never suppressed.
    """
    @functools.wraps(func)
    async def wrapper(self, request, context):
        try:
            return await func(self, request, context)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.exception(f"Error in {func.__name__}: {str(e)}")
            await context.abort(grpc.StatusCode.INTERNAL, f"Internal server error: {str(e)}")
    return wrapper
