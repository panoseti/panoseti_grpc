from typing import Any

import grpc
from google.protobuf.json_format import MessageToDict

from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc

from .client_models import (
    CleanupDataParameters,
    GenerateManifestParameters,
    GetManifestParameters,
    StartDaqParameters,
    StatusDaqParameters,
    StopDaqParameters,
)


class DaqControlClient:
    """
    Client for the PANOSETI Daq Control Service.
    Supports both Strict (Production) and Flexible (Experimental) logging.
    """

    def __init__(self, host: str = "localhost", port: int = 50051) -> None:
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub: daq_control_pb2_grpc.DaqControlStub = daq_control_pb2_grpc.DaqControlStub(self.channel)

    def StartDaq(self, parameters: dict[str, Any], timeout: float | None = None) -> bool:
        """
        Docstring for StartDaq

        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * daq_ip_addr(str) - ip address
                    * bindhost(str) - ethernet port for receiving packets
                    * max_file_size_mb(uint32) - max file size in MB
                    * group_ph_frames(bool) - set if group ph frames from 4 Qubaos
                    * run_dir(str) - the new directory for this run,
                                    which should be under data_dir
                    * obs(str) - obs name
                    * module_id (list) - modules for this daq node
        :param timeout: Optional gRPC timeout in seconds.
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
        try:
            resp: daq_control_pb2.StartDaqResponse = self.stub.StartDaq(request, timeout=timeout)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
            # this return may not be necessary
            return bool(resp.success)
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}") from e

    def StopDaq(self, parameters: dict[str, Any], timeout: float = 30.0) -> bool:
        """
        Docstring for StopDaq

        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * run_dir(str) - the new directory for this run,
                                    which should be under data_dir
        :param timeout: gRPC timeout in seconds. Defaults to 30.0.
        """
        v_params = StopDaqParameters(**parameters)
        request = daq_control_pb2.StopDaqRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        try:
            resp: daq_control_pb2.StopDaqResponse = self.stub.StopDaq(request, timeout=timeout)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
            # this return may not be necessary
            return bool(resp.success)
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}") from e

    def StatusDaq(self, parameters: dict[str, Any], timeout: float | None = None) -> tuple[bool, dict[str, Any]]:
        """
        Docstring for StatusDaq

        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * check_hashpipe_running(bool) - check if hashpipe is running
                    * check_disk_usage(bool) - check the disk usage
                    * check_run_dirs(bool) - check the run dirs on daq node
        :param timeout: Optional gRPC timeout in seconds.
        """
        v_params = StatusDaqParameters(**parameters)
        request = daq_control_pb2.DaqStatusRequest()
        request.data_dir = v_params.data_dir
        request.check_hashpipe_running = v_params.check_hashpipe_running
        request.check_disk_usage = v_params.check_disk_usage
        request.check_run_dirs = v_params.check_run_dirs
        try:
            resp: daq_control_pb2.DaqStatusResponse = self.stub.StatusDaq(request, timeout=timeout)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
            status: dict[str, Any] = {}
            status["hashpipe_running"] = bool(resp.hashpipe_running)
            status["disk_usage"] = dict(resp.disk_usage)
            status["run_dirs"] = list(resp.run_dirs)
            return bool(resp.success), status
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}") from e

    def CleanupData(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """
        Clean up run data on the DAQ node.

        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * run_dir(str) - the new directory for this run,
                                    which should be under data_dir
                    * module_id (list) - modules for this daq node
                    * force (bool) - force cleanup
                    * mode (str) - "CLEANUP_FULL" or "CLEANUP_SELECTIVE"
                    * delete_patterns (list[str]) - glob patterns to delete (selective mode)
                    * preserve_patterns (list[str]) - glob patterns to preserve (selective mode)
        :param timeout: Optional gRPC timeout in seconds.
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
        try:
            resp: daq_control_pb2.CleanupDataResponse = self.stub.CleanupData(request, timeout=timeout)
            resp_dict: dict[str, Any] = MessageToDict(
                resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True
            )
            return resp_dict
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}") from e

    def GenerateManifest(self, parameters: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        """
        Generate a checksum manifest for run data on the DAQ node.

        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * run_dir(str) - the run directory
                    * module_id (int) - module ID
                    * algorithm (str) - "blake3" or "xxh3_128"
                    * include_patterns (list[str]) - glob patterns to include
        :param timeout: Optional gRPC timeout in seconds.
        """
        v_params = GenerateManifestParameters(**parameters)
        request = daq_control_pb2.GenerateManifestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id = v_params.module_id
        request.algorithm = v_params.algorithm
        request.include_patterns.extend(v_params.include_patterns)
        try:
            resp: daq_control_pb2.GenerateManifestResponse = self.stub.GenerateManifest(request, timeout=timeout)
            resp_dict: dict[str, Any] = MessageToDict(
                resp, always_print_fields_with_no_presence=True, preserving_proto_field_name=True
            )
            return resp_dict
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}") from e

    def GetManifest(self, parameters: dict[str, Any], timeout: float | None = None) -> list[dict[str, Any]]:
        """
        Stream manifest entries for a module's run data.

        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * run_dir(str) - the run directory
                    * module_id (int) - module ID
        :param timeout: Optional gRPC timeout in seconds.
        """
        v_params = GetManifestParameters(**parameters)
        request = daq_control_pb2.GetManifestRequest()
        request.data_dir = v_params.data_dir
        request.run_dir = v_params.run_dir
        request.module_id = v_params.module_id
        try:
            entries: list[dict[str, Any]] = []
            for entry in self.stub.GetManifest(request, timeout=timeout):
                entry_dict: dict[str, Any] = MessageToDict(
                    entry, always_print_fields_with_no_presence=True, preserving_proto_field_name=True
                )
                entries.append(entry_dict)
            return entries
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}") from e
