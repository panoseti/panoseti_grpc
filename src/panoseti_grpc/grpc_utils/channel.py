from __future__ import annotations

from typing import Any

import grpc
import grpc.aio


def keepalive_options(
    time_ms: int = 30_000,
    timeout_ms: int = 10_000,
    permit_without_calls: bool = True,
) -> list[tuple[str, Any]]:
    """Return standard gRPC keepalive channel options."""
    return [
        ("grpc.keepalive_time_ms", time_ms),
        ("grpc.keepalive_timeout_ms", timeout_ms),
        ("grpc.keepalive_permit_without_calls", int(permit_without_calls)),
        ("grpc.http2.max_pings_without_data", 0),
    ]


class AsyncChannelManager:
    """Async context manager that owns a single insecure gRPC channel.

    Usage::

        async with AsyncChannelManager("localhost", 50051) as mgr:
            stub = MyServiceStub(mgr.channel)
            resp = await stub.MyRpc(request)
    """

    def __init__(
        self,
        host: str,
        port: int,
        options: list[tuple[str, Any]] | None = None,
    ) -> None:
        self.target = f"{host}:{port}"
        self._options = options if options is not None else keepalive_options()
        self._channel: grpc.aio.Channel | None = None

    @property
    def channel(self) -> grpc.aio.Channel:
        if self._channel is None:
            raise RuntimeError("AsyncChannelManager must be used as an async context manager.")
        return self._channel

    async def __aenter__(self) -> AsyncChannelManager:
        self._channel = grpc.aio.insecure_channel(self.target, options=self._options)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
