from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import grpc
import grpc.aio
from google.protobuf.json_format import MessageToDict
from pydantic import IPvAnyAddress

from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc
from panoseti_grpc.grpc_utils import grpc_call
from panoseti_grpc.grpc_utils.channel import AsyncChannelManager, keepalive_options
from panoseti_grpc.grpc_utils.retries import build_retry_service_config

from .client_models import (
    CleanupDataParameters,
    GenerateManifestParameters,
    GetManifestDigestParameters,
    GetManifestParameters,
    GetTransferStatusParameters,
    RetryFailedTransferParameters,
    StartDaqParameters,
    StatusDaqParameters,
    StopDaqParameters,
)


class AsyncDaqControlClient:
    """Async client for the PANOSETI Daq Control Service.

    Uses `grpc.aio` for non-blocking I/O. Supports async context management
    for automatic channel lifecycle management.

    Args:
        host: Server hostname or IP address. Defaults to "localhost".
        port: Server gRPC port. Defaults to 50051.
    """

    def __init__(self, host: str | IPvAnyAddress = "localhost", port: int = 50051) -> None:
        self.target = f"{host}:{port}"
        _options = [*keepalive_options(), ("grpc.service_config", build_retry_service_config())]
        self._channel_mgr = AsyncChannelManager(str(host), port, options=_options)
        self._stub: daq_control_pb2_grpc.DaqControlStub | None = None

    async def __aenter__(self) -> AsyncDaqControlClient:
        """Initialize the async gRPC channel."""
        await self._channel_mgr.__aenter__()
        self._stub = daq_control_pb2_grpc.DaqControlStub(self._channel_mgr.channel)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the async gRPC channel."""
        await self._channel_mgr.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def stub(self) -> daq_control_pb2_grpc.DaqControlStub:
        """The initialized gRPC stub.

        Raises:
            RuntimeError: If accessed outside of an async context block.
        """
        if self._stub is None:
            raise RuntimeError("AsyncDaqControlClient must be used as an async context manager.")
        return self._stub

    @grpc_call
    async def StartDaq(self, parameters: dict[str, Any], timeout: float | None = None) -> bool:  # noqa: ASYNC109
        """Start hashpipe on the remote DAQ node.

        Args:
            parameters: Start parameters including `data_dir`, `daq_ip_addr`, `module_id`, etc.
            timeout: Optional gRPC timeout in seconds.

        Returns:
            True if the command was accepted by the server.

        Raises:
            ValueError: If the server rejects the configuration.
            ConnectionError: If the RPC call fails.
        """
        v_params = StartDaqParameters(**parameters)
        request = daq_control_pb2.StartDaqRequest()
        request.data_dir = v_params.data_dir
        request.daq_ip_addr = str(v_params.daq_ip_addr)
        request.bindhost = v_params.bindhost
        request.max_file_size_mb = int(v_params.max_file_size_mb)
        request.group_ph_frames = v_params.group_ph_frames
        request.run_dir = v_params.run_dir
        request.obs = v_params.obs
        request.module_id.extend(v_params.module_id)
        resp: daq_control_pb2.StartDaqResponse = await self.stub.StartDaq(request, timeout=timeout)
        if not resp.success:
            raise ValueError(f"Server rejected data: {resp.message}")
        return bool(resp.success)

    @grpc_call
    async def StopDaq(self, parameters: dict[str, Any], timeout: float = 30.0) -> bool:  # noqa: ASYNC109
        """Stop hashpipe on the remote DAQ node.

        Args:
            parameters: Stop parameters including `data_dir` and `run_dir`.
            timeout: gRPC timeout in seconds. Defaults to 30.0.

        Returns:
            True if the command was accepted by the server.

        Raises:
            ValueError: If the server rejects the command.
            ConnectionError: If the RPC call fails.
        """
        v_params = StopDaqParameters(**parameters)
        request = daq_control_pb2.StopDaqRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        resp: daq_control_pb2.StopDaqResponse = await self.stub.StopDaq(request, timeout=timeout)
        if not resp.success:
            raise ValueError(f"Server rejected data: {resp.message}")
        return bool(resp.success)

    @grpc_call
    async def StatusDaq(self, parameters: dict[str, Any], timeout: float | None = None) -> tuple[bool, dict[str, Any]]:  # noqa: ASYNC109
        """Retrieve the current status of hashpipe and disk usage on the node.

        Args:
            parameters: Status query parameters.
            timeout: Optional gRPC timeout in seconds.

        Returns:
            A tuple of (success_bool, status_dict).

        Raises:
            ValueError: If the query parameters are invalid.
            ConnectionError: If the RPC call fails.
        """
        v_params = StatusDaqParameters(**parameters)
        request = daq_control_pb2.DaqStatusRequest()
        request.data_dir = v_params.data_dir
        request.check_hashpipe_running = v_params.check_hashpipe_running
        request.check_disk_usage = v_params.check_disk_usage
        request.check_run_dirs = v_params.check_run_dirs
        resp: daq_control_pb2.DaqStatusResponse = await self.stub.StatusDaq(request, timeout=timeout)
        if not resp.success:
            raise ValueError(f"Server rejected data: {resp.message}")
        status: dict[str, Any] = {}
        status["hashpipe_running"] = bool(resp.hashpipe_running)
        status["disk_usage"] = dict(resp.disk_usage)
        status["run_dirs"] = list(resp.run_dirs)
        status["hashpipe_pid"] = int(resp.hashpipe_pid) if resp.hashpipe_pid > 0 else None
        return bool(resp.success), status

    @grpc_call
    async def CleanupData(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:  # noqa: ASYNC109
        """Trigger data cleanup on the remote DAQ node.

        Args:
            parameters: Cleanup configuration.
            timeout: Optional gRPC timeout in seconds.

        Returns:
            A dictionary containing the cleanup results summary.

        Raises:
            ConnectionError: If the RPC call fails.
        """
        v_params = CleanupDataParameters(**parameters)
        request = daq_control_pb2.CleanupDataRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        request.force = v_params.force
        request.mode = daq_control_pb2.CleanupMode.Value(v_params.mode)
        request.delete_patterns.extend(v_params.delete_patterns)
        request.preserve_patterns.extend(v_params.preserve_patterns)
        request.manifest_digest = v_params.manifest_digest
        request.dry_run = v_params.dry_run
        resp: daq_control_pb2.CleanupDataResponse = await self.stub.CleanupData(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    async def GenerateManifest(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:  # noqa: ASYNC109
        """Generate a checksum manifest for run data.

        Args:
            parameters: Manifest generation parameters.
            timeout: Optional gRPC timeout in seconds.

        Returns:
            A dictionary summary of the manifest generation task.

        Raises:
            ConnectionError: If the RPC call fails.
        """
        v_params = GenerateManifestParameters(**parameters)
        request = daq_control_pb2.GenerateManifestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        request.algorithm = v_params.algorithm
        request.include_patterns.extend(v_params.include_patterns)
        resp: daq_control_pb2.GenerateManifestResponse = await self.stub.GenerateManifest(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    async def GetManifest(
        self,
        parameters: dict[str, Any],
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream manifest entries for a specific module/run.

        Args:
            parameters: Manifest query parameters.
            timeout: Optional gRPC timeout in seconds.

        Yields:
            Manifest entry dictionaries.

        Raises:
            ConnectionError: If the RPC call fails.
        """
        v_params = GetManifestParameters(**parameters)
        request = daq_control_pb2.GetManifestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        async for entry in self.stub.GetManifest(request, timeout=timeout):
            yield MessageToDict(entry, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    async def GetTransferStatus(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:  # noqa: ASYNC109
        """Return transfer readiness for a DAQ node.

        Args:
            parameters: Query parameters including ``data_dir`` and optional ``run_dir``.
            timeout: Optional gRPC timeout in seconds.

        Returns:
            Dictionary with ``hashpipe_running``, ``free_bytes``, ``total_bytes``,
            ``run_dirs``, and ``manifest_files``.

        Raises:
            ConnectionError: If the RPC call fails.
        """
        v_params = GetTransferStatusParameters(**parameters)
        request = daq_control_pb2.GetTransferStatusRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        resp: daq_control_pb2.GetTransferStatusResponse = await self.stub.GetTransferStatus(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    async def GetManifestDigest(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:  # noqa: ASYNC109
        """Return the SHA-256 hex digest of the on-disk manifest for a module/run.

        The returned ``digest_hex`` is used to populate ``manifest_digest`` in
        ``CleanupDataRequest`` to satisfy the CLEANUP_SELECTIVE integrity precondition.

        Args:
            parameters: Query parameters including ``data_dir``, ``run_dir``, ``module_id``.
            timeout: Optional gRPC timeout in seconds.

        Returns:
            Dictionary with ``digest_hex``, ``algo_suffix``, and ``manifest_path``.

        Raises:
            ConnectionError: If the RPC call fails.
        """
        v_params = GetManifestDigestParameters(**parameters)
        request = daq_control_pb2.GetManifestDigestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        resp: daq_control_pb2.GetManifestDigestResponse = await self.stub.GetManifestDigest(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    async def RetryFailedTransfer(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:  # noqa: ASYNC109
        """Re-emit a single file's digest so the head node can reconcile without a full re-rsync.

        Args:
            parameters: Parameters including ``data_dir``, ``run_dir``, ``module_id``, ``file_path``.
            timeout: Optional gRPC timeout in seconds.

        Returns:
            Dictionary with ``size_bytes``, ``digest_hex``, and ``algorithm``.

        Raises:
            ConnectionError: If the RPC call fails.
        """
        v_params = RetryFailedTransferParameters(**parameters)
        request = daq_control_pb2.RetryFailedTransferRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        request.file_path = v_params.file_path
        resp: daq_control_pb2.RetryFailedTransferResponse = await self.stub.RetryFailedTransfer(
            request, timeout=timeout
        )
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)


class DaqControlClient:
    """Synchronous client for the PANOSETI Daq Control Service.

    Args:
        host: Server hostname or IP address. Defaults to "localhost".
        port: Server gRPC port. Defaults to 50051.
    """

    def __init__(self, host: str = "localhost", port: int = 50051) -> None:
        self.target = f"{host}:{port}"
        self.channel = grpc.insecure_channel(self.target)
        self.stub: daq_control_pb2_grpc.DaqControlStub = daq_control_pb2_grpc.DaqControlStub(self.channel)

    def close(self) -> None:
        """Close the synchronous gRPC channel."""
        self.channel.close()

    @grpc_call
    def StartDaq(self, parameters: dict[str, Any], timeout: float | None = None) -> bool:
        """Start hashpipe on the remote DAQ node. (Sync)"""
        v_params = StartDaqParameters(**parameters)
        request = daq_control_pb2.StartDaqRequest()
        request.data_dir = v_params.data_dir
        request.daq_ip_addr = str(v_params.daq_ip_addr)
        request.bindhost = v_params.bindhost
        request.max_file_size_mb = int(v_params.max_file_size_mb)
        request.group_ph_frames = v_params.group_ph_frames
        request.run_dir = v_params.run_dir
        request.obs = v_params.obs
        request.module_id.extend(v_params.module_id)
        resp: daq_control_pb2.StartDaqResponse = self.stub.StartDaq(request, timeout=timeout)
        if not resp.success:
            raise ValueError(f"Server rejected data: {resp.message}")
        return bool(resp.success)

    @grpc_call
    def StopDaq(self, parameters: dict[str, Any], timeout: float = 30.0) -> bool:
        """Stop hashpipe on the remote DAQ node. (Sync)"""
        v_params = StopDaqParameters(**parameters)
        request = daq_control_pb2.StopDaqRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        resp: daq_control_pb2.StopDaqResponse = self.stub.StopDaq(request, timeout=timeout)
        if not resp.success:
            raise ValueError(f"Server rejected data: {resp.message}")
        return bool(resp.success)

    @grpc_call
    def StatusDaq(self, parameters: dict[str, Any], timeout: float | None = None) -> tuple[bool, dict[str, Any]]:
        """Retrieve the current status of hashpipe and disk usage. (Sync)"""
        v_params = StatusDaqParameters(**parameters)
        request = daq_control_pb2.DaqStatusRequest()
        request.data_dir = v_params.data_dir
        request.check_hashpipe_running = v_params.check_hashpipe_running
        request.check_disk_usage = v_params.check_disk_usage
        request.check_run_dirs = v_params.check_run_dirs
        resp: daq_control_pb2.DaqStatusResponse = self.stub.StatusDaq(request, timeout=timeout)
        if not resp.success:
            raise ValueError(f"Server rejected data: {resp.message}")
        status: dict[str, Any] = {}
        status["hashpipe_running"] = bool(resp.hashpipe_running)
        status["disk_usage"] = dict(resp.disk_usage)
        status["run_dirs"] = list(resp.run_dirs)
        status["hashpipe_pid"] = int(resp.hashpipe_pid) if resp.hashpipe_pid > 0 else None
        return bool(resp.success), status

    @grpc_call
    def CleanupData(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """Trigger data cleanup on the remote DAQ node. (Sync)"""
        v_params = CleanupDataParameters(**parameters)
        request = daq_control_pb2.CleanupDataRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        request.force = v_params.force
        request.mode = daq_control_pb2.CleanupMode.Value(v_params.mode)
        request.delete_patterns.extend(v_params.delete_patterns)
        request.preserve_patterns.extend(v_params.preserve_patterns)
        request.manifest_digest = v_params.manifest_digest
        request.dry_run = v_params.dry_run
        resp: daq_control_pb2.CleanupDataResponse = self.stub.CleanupData(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    def GenerateManifest(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """Generate a checksum manifest for run data. (Sync)"""
        v_params = GenerateManifestParameters(**parameters)
        request = daq_control_pb2.GenerateManifestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        request.algorithm = v_params.algorithm
        request.include_patterns.extend(v_params.include_patterns)
        resp: daq_control_pb2.GenerateManifestResponse = self.stub.GenerateManifest(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    def GetManifest(self, parameters: dict[str, Any], timeout: float | None = None) -> list[dict[str, Any]]:
        """Retrieve manifest entries for a specific module/run. (Sync)"""
        v_params = GetManifestParameters(**parameters)
        request = daq_control_pb2.GetManifestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        entries: list[dict[str, Any]] = []
        for entry in self.stub.GetManifest(request, timeout=timeout):
            entries.append(
                MessageToDict(entry, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)
            )
        return entries

    @grpc_call
    def GetTransferStatus(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """Return transfer readiness for a DAQ node. (Sync)"""
        v_params = GetTransferStatusParameters(**parameters)
        request = daq_control_pb2.GetTransferStatusRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        resp: daq_control_pb2.GetTransferStatusResponse = self.stub.GetTransferStatus(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    def GetManifestDigest(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """Return the SHA-256 hex digest of the on-disk manifest for a module/run. (Sync)"""
        v_params = GetManifestDigestParameters(**parameters)
        request = daq_control_pb2.GetManifestDigestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        resp: daq_control_pb2.GetManifestDigestResponse = self.stub.GetManifestDigest(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)

    @grpc_call
    def RetryFailedTransfer(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """Re-emit a single file's digest for reconciliation. (Sync)"""
        v_params = RetryFailedTransferParameters(**parameters)
        request = daq_control_pb2.RetryFailedTransferRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id.extend(v_params.module_id)
        request.file_path = v_params.file_path
        resp: daq_control_pb2.RetryFailedTransferResponse = self.stub.RetryFailedTransfer(request, timeout=timeout)
        return MessageToDict(resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True)
