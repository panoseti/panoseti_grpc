"""Thin wrappers around grpc.health.v1 for PANOSETI services.

grpcio-health-checking is an optional dependency. Import errors are surfaced
at call time rather than module import so that services that don't use health
checks can still import grpc_utils without the extra package installed.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, cast

import grpc
import grpc.aio

if TYPE_CHECKING:
    pass


class HealthToggle:
    """Per-service NOT_SERVING / SERVING toggle returned by :func:`register_health`.

    Servicers hold an optional reference to this object and call
    :meth:`not_serving` / :meth:`serving` (or use :meth:`reconfiguring` as a
    context manager) to signal transient unavailability during disruptive
    operations such as writer-lock acquisition or Hashpipe restart.
    """

    def __init__(self, servicer: object, service_names: list[str]) -> None:
        self._servicer = servicer
        self._service_names = service_names

    def not_serving(self, service: str) -> None:
        """Mark *service* as NOT_SERVING."""
        try:
            from grpc_health.v1 import health_pb2

            self._servicer.set(service, health_pb2.HealthCheckResponse.NOT_SERVING)  # type: ignore[attr-defined]
        except Exception:
            pass

    def serving(self, service: str) -> None:
        """Mark *service* as SERVING."""
        try:
            from grpc_health.v1 import health_pb2

            self._servicer.set(service, health_pb2.HealthCheckResponse.SERVING)  # type: ignore[attr-defined]
        except Exception:
            pass

    @contextlib.contextmanager
    def reconfiguring(self, service: str) -> Iterator[None]:
        """Context manager: NOT_SERVING for the duration, restored to SERVING on exit."""
        self.not_serving(service)
        try:
            yield
        finally:
            self.serving(service)


def register_health(server: grpc.aio.Server, service_names: list[str]) -> HealthToggle:
    """Register a HealthServicer on *server*, mark all services SERVING, and
    return a :class:`HealthToggle` for per-service liveness transitions.

    Args:
        server: The running grpc.aio.Server instance.
        service_names: Proto service names to register (e.g. "panoseti.daq_control").

    Returns:
        A :class:`HealthToggle` that servicers can use to flip NOT_SERVING /
        SERVING during disruptive reconfiguration windows.

    Raises:
        ImportError: If grpcio-health-checking is not installed.
    """
    try:
        from grpc_health.v1 import health, health_pb2
        from grpc_health.v1 import health_pb2_grpc as hpb2_grpc
    except ImportError as exc:
        raise ImportError(
            "grpcio-health-checking is required for health check support. "
            "Install it with: pip install grpcio-health-checking"
        ) from exc

    servicer = health.HealthServicer()
    hpb2_grpc.add_HealthServicer_to_server(servicer, server)
    for name in service_names:
        servicer.set(name, health_pb2.HealthCheckResponse.SERVING)
    servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    return HealthToggle(servicer, service_names)


class HealthClient:
    """Client-side wrapper for the standard gRPC health protocol.

    Args:
        host: Server hostname or IP.
        port: Server gRPC port.

    Raises:
        ImportError: If grpcio-health-checking is not installed.
    """

    def __init__(self, host: str = "localhost", port: int = 50051) -> None:
        try:
            from grpc_health.v1 import health_pb2, health_pb2_grpc
        except ImportError as exc:
            raise ImportError(
                "grpcio-health-checking is required. Install with: pip install grpcio-health-checking"
            ) from exc
        self._health_pb2 = health_pb2
        self._health_pb2_grpc = health_pb2_grpc
        self.target = f"{host}:{port}"

    def check(self, service: str = "", timeout: float = 5.0) -> bool:
        """Return True if the named service reports SERVING.

        Args:
            service: Proto service name (empty string for overall server health).
            timeout: RPC timeout in seconds.
        """
        with grpc.insecure_channel(self.target) as channel:
            stub = self._health_pb2_grpc.HealthStub(channel)
            try:
                resp = stub.Check(
                    self._health_pb2.HealthCheckRequest(service=service),
                    timeout=timeout,
                )
                return cast(bool, resp.status == self._health_pb2.HealthCheckResponse.SERVING)
            except grpc.RpcError:
                return False
