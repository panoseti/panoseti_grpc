"""Thin wrappers around grpc.health.v1 for PANOSETI services.

grpcio-health-checking is an optional dependency. Import errors are surfaced
at call time rather than module import so that services that don't use health
checks can still import grpc_utils without the extra package installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import grpc
import grpc.aio

if TYPE_CHECKING:
    pass


def register_health(server: grpc.aio.Server, service_names: list[str]) -> None:
    """Register a HealthServicer on *server* and mark all services SERVING.

    Args:
        server: The running grpc.aio.Server instance.
        service_names: Proto service names to register (e.g. "panoseti.daq_control").

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
