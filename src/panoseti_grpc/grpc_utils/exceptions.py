from __future__ import annotations

import grpc


class PanosetiRpcError(Exception):
    """Base exception for all gRPC errors in PANOSETI services.

    Wraps grpc.RpcError with structured fields so callers don't need to
    import grpc to inspect the status code.
    """

    def __init__(self, message: str, code: grpc.StatusCode, details: str, target: str) -> None:
        super().__init__(message)
        self.code = code
        self.details = details
        self.target = target

    def __repr__(self) -> str:
        return f"{type(self).__name__}(code={self.code.name}, target={self.target!r}, details={self.details!r})"


class UnavailableError(PanosetiRpcError):
    """Server is unreachable or temporarily unavailable (UNAVAILABLE)."""


class DeadlineExceededError(PanosetiRpcError):
    """RPC timed out before completing (DEADLINE_EXCEEDED)."""


class ResourceExhaustedError(PanosetiRpcError):
    """Server resource limit reached (RESOURCE_EXHAUSTED)."""


class FailedPreconditionError(PanosetiRpcError):
    """Operation rejected due to system state (FAILED_PRECONDITION)."""


class NotFoundError(PanosetiRpcError):
    """Requested resource does not exist (NOT_FOUND)."""


class AlreadyExistsError(PanosetiRpcError):
    """Resource already exists (ALREADY_EXISTS)."""


class InvalidArgumentError(PanosetiRpcError):
    """Client sent an invalid argument (INVALID_ARGUMENT)."""


class InternalError(PanosetiRpcError):
    """Server-side internal error (INTERNAL)."""


_CODE_TO_EXCEPTION: dict[grpc.StatusCode, type[PanosetiRpcError]] = {
    grpc.StatusCode.UNAVAILABLE: UnavailableError,
    grpc.StatusCode.DEADLINE_EXCEEDED: DeadlineExceededError,
    grpc.StatusCode.RESOURCE_EXHAUSTED: ResourceExhaustedError,
    grpc.StatusCode.FAILED_PRECONDITION: FailedPreconditionError,
    grpc.StatusCode.NOT_FOUND: NotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: AlreadyExistsError,
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentError,
    grpc.StatusCode.INTERNAL: InternalError,
}


def from_rpc_error(e: grpc.RpcError, target: str) -> PanosetiRpcError:
    """Convert a raw grpc.RpcError into the appropriate PanosetiRpcError subclass."""
    code: grpc.StatusCode = e.code()
    details: str = e.details() or ""
    cls = _CODE_TO_EXCEPTION.get(code, PanosetiRpcError)
    return cls(f"gRPC {code.name} from {target}: {details}", code, details, target)
