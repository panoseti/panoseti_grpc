from typing import Any

import grpc
from google.protobuf.json_format import MessageToDict

from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc


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
        # TODO: check if all of the parameters are reasonable
        #       We could use Pydantic for this.
        request = daq_control_pb2.StartDaqRequest()
        request.data_dir = parameters["data_dir"]
        request.daq_ip_addr = parameters["daq_ip_addr"]
        request.bindhost = parameters["bindhost"]
        request.max_file_size_mb = parameters["max_file_size_mb"]
        request.group_ph_frames = parameters["group_ph_frames"]
        request.run_dir = parameters["run_dir"]
        request.obs = parameters["obs"]
        request.module_id.extend(parameters["module_id"])
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
        # TODO: check if all of the parameters are reasonable
        #       We could use Pydantic for this.
        request = daq_control_pb2.StopDaqRequest()
        request.data_dir = parameters["data_dir"]
        request.run_dir = parameters["run_dir"]
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
        # TODO: check if all of the parameters are reasonable
        #       We could use Pydantic for this.
        request = daq_control_pb2.DaqStatusRequest()
        request.data_dir = parameters["data_dir"]
        request.check_hashpipe_running = parameters["check_hashpipe_running"]
        request.check_disk_usage = parameters["check_disk_usage"]
        request.check_run_dirs = parameters["check_run_dirs"]
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
        request = daq_control_pb2.CleanupDataRequest()
        required_params = {"data_dir", "run_dir", "module_id"}
        if required_params.issubset(set(parameters)):
            request.data_dir = parameters["data_dir"]
            request.run_dir = parameters["run_dir"]
            request.module_id.extend(parameters["module_id"])
        else:
            missing_params = required_params.difference(set(parameters))
            raise ValueError(f"Missing required parameter(s): {missing_params}")
        if "force" in parameters:
            request.force = parameters["force"]
        if "mode" in parameters:
            request.mode = daq_control_pb2.CleanupMode.Value(parameters["mode"])
        if "delete_patterns" in parameters:
            request.delete_patterns.extend(parameters["delete_patterns"])
        if "preserve_patterns" in parameters:
            request.preserve_patterns.extend(parameters["preserve_patterns"])
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
        request = daq_control_pb2.GenerateManifestRequest()
        request.data_dir = parameters["data_dir"]
        request.run_dir = parameters["run_dir"]
        request.module_id = parameters["module_id"]
        if "algorithm" in parameters:
            request.algorithm = parameters["algorithm"]
        if "include_patterns" in parameters:
            request.include_patterns.extend(parameters["include_patterns"])
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
        request = daq_control_pb2.GetManifestRequest()
        request.data_dir = parameters["data_dir"]
        request.run_dir = parameters["run_dir"]
        request.module_id = parameters["module_id"]
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
