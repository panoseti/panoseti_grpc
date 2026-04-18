#!/usr/bin/env python3
"""
The Python implementation of the PANOSETI Daq Control gRPC Server.
Features:
- Start Daq (HASHPIPE instance) on daq nodes
- Stop Daq (HASHPIPE instance) on daq nodes
- Status Daq on daq nodes
    * check if HASHPIPE is running
    * check if there is run in progress
    * check the free space on disk
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
from collections.abc import Callable
from glob import glob
from pathlib import Path
from typing import Any

import psutil
from google.protobuf.message import Message
from google.protobuf.struct_pb2 import Struct
from pydantic import ValidationError

# gRPC Imports
os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "0"
import contextlib

import grpc
from google.protobuf.json_format import MessageToDict

from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc
from panoseti_grpc.telemetry.logger import get_logger
from panoseti_grpc.util.error_handling import grpc_error_handler

from .config import CleanupDataModel, StartDaqModel, StatusDaqModel

# Local Imports
from .util import is_hashpipe_running

PROCESS = "hashpipe"
SERVER_LOG_DIR = "/var/log/panoseti"


async def _read_stream(stream: asyncio.StreamReader, log_method: Callable[[str], None]) -> None:
    """Read lines from a subprocess stream and forward each line to a logger method."""
    while True:
        line = await stream.readline()
        if not line:
            break
        message = line.decode("utf-8", errors="replace").strip()
        if message:
            log_method(message)


async def _monitor_hashpipe(
    proc: asyncio.subprocess.Process, stdout_logger: logging.Logger, stderr_logger: logging.Logger
) -> None:
    """Pipe hashpipe stdout/stderr to their respective loggers (runs as background task)."""
    if proc.stdout is None or proc.stderr is None:
        return
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_read_stream(proc.stdout, stdout_logger.info))
        tg.create_task(_read_stream(proc.stderr, stderr_logger.error))


class DaqControlServicer(daq_control_pb2_grpc.DaqControlServicer):
    """
    Implements the Daq Control gRPC service.
    Handles start daq, stop daq and status daq.
    """

    def __init__(self, level: int = logging.INFO) -> None:
        self.logger = get_logger(
            "daq_control_server",
            level=level,
            console=True,
            log_dir=SERVER_LOG_DIR,
            grpc_enabled=True,
        )
        self.logger.info("DaqControlServicer initialized")
        self.logger.info("DaqControl Server Online")
        # This is used for recording the hashpipe pid
        n, hashpipe_pids = self._get_pids_by_name(PROCESS)
        if n == 0:
            self.hashpipe_pid = -1
        elif n == 1:
            self.hashpipe_pid = hashpipe_pids[0]
            self.logger.warning(f"Found 1 HASHPIPE instance is already running, pid:{self.hashpipe_pid}")
        else:
            self.hashpipe_pid = -1
            self.logger.warning(f"Found {n} HASHPIPE instances are running, pids: {hashpipe_pids}")
            self.logger.warning("All of these HASHPIPE instances have been killed.")
            self.kill_processes(hashpipe_pids)

    def _get_pids_by_name(self, name: str) -> tuple[int, list[int]]:
        pids = []
        for proc in psutil.process_iter(["pid", "name"]):
            if proc.info["name"] == name:
                pids.append(proc.info["pid"])
        return len(pids), pids

    def kill_processes(self, pids: list[int]) -> None:
        for pid in pids:
            p = psutil.Process(pid)
            p.send_signal(signal.SIGINT)

    def _create_module_config(self, datadir: str | Path, module_id: list[int]) -> None:
        mconfig = f"{datadir}/module.config"
        self.logger.info(f"Create {mconfig}")
        with open(mconfig, "w") as f:
            for id in module_id:
                f.write(f"{id} ")

    def _setup_data_directories(self, datadir: str | Path, rundir: str | Path, module_id: list[int]) -> None:
        # create directory for config files
        cdirname = f"{datadir}/{rundir}"
        self.logger.info(f"Setup rundir for configs: {cdirname}")
        Path(cdirname).mkdir(parents=True, exist_ok=True)
        # create directory for data
        for m in module_id:
            dirname = f"{datadir}/module_{m}/{rundir}"
            self.logger.info(f"Setup rundir for data: {dirname}")
            Path(dirname).mkdir(parents=True, exist_ok=True)

    def _check_disk_usage(self, datadir: str | Path) -> dict[str, int]:
        usage = shutil.disk_usage(datadir)
        disk_usage = {
            "total_disk_space": usage.total,
            "used_disk_space": usage.used,
            "free_disk_space": usage.free,
        }
        return disk_usage

    def _check_run_dirs(self, datadir: str | Path) -> list[str]:
        return glob(f"{datadir}/*.pffd")

    def _cleanup_dir(self, rundir: str | Path) -> bool:
        path = Path(rundir)
        if not path.is_dir():
            self.logger.warning(f"Data Directory not exist: {rundir}")
            return False
        else:
            self.logger.debug(f"Cleaning up {rundir}")
            shutil.rmtree(path)
            if not path.is_dir():
                self.logger.debug("Cleanup successful")
                return True
            else:
                self.logger.debug("Cleanup failed")
                return False

    def _request_to_dict(self, request: Message) -> dict[str, Any]:
        request_dict: dict[str, Any] = MessageToDict(
            request, always_print_fields_with_no_presence=True, preserving_proto_field_name=True
        )
        return request_dict

    @grpc_error_handler
    async def StartDaq(
        self, request: daq_control_pb2.StartDaqRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.StartDaqResponse:
        self.logger.info("Starting HASHPIPE instance...")
        # 1. check if we already have HASHPIPE running
        n, pids = self._get_pids_by_name(PROCESS)
        if n > 0:
            msg = f"Found {n} HASHPIPE instances running. pids: {pids}"
            self.logger.warning(msg)
            return daq_control_pb2.StartDaqResponse(success=False, message=msg)
        # 2. check the parameters
        dreq = self._request_to_dict(request)
        vreq = StartDaqModel(**dreq)
        vreq_dict = vreq.model_dump(mode="json", exclude_unset=True)
        # 3. get the parameters
        datadir = vreq.data_dir
        run_dir = vreq.run_dir
        bindhost = vreq.bindhost
        max_file_size_mb = vreq.max_file_size_mb
        group_ph_frames = vreq.group_ph_frames
        obs = vreq.obs
        module_id = vreq.module_id
        # get the full path for hashpipe.so, rundir and module.config
        hashpipe_so = f"{datadir}/hashpipe.so"
        configfn = f"{datadir}/module.config"
        # create module.config
        self._create_module_config(datadir, module_id)
        # setup data directories
        self._setup_data_directories(datadir, run_dir, module_id)
        # create per-run loggers for hashpipe stdout and stderr
        run_dir_path = str(Path(datadir) / run_dir)
        hp_stdout_logger = get_logger(
            "hp_stdout",
            log_dir=run_dir_path,
            grpc_enabled=True,
            console=False,
        )
        hp_stderr_logger = get_logger(
            "hp_stderr",
            log_dir=run_dir_path,
            grpc_enabled=True,
            console=False,
        )
        # create cmdline for start HASHPIPE
        cmd = [
            "hashpipe",
            "-p",
            hashpipe_so,
            "-I",
            "0",
            "-o",
            f"BINDHOST={bindhost}",
            "-o",
            f"MAXFILESIZE={max_file_size_mb}",
            "-o",
            f"GROUPPHFRAMES={group_ph_frames}",
            "-o",
            f"RUNDIR={run_dir}",
            "-o",
            f"CONFIG={configfn}",
            "-o",
            f"OBS={obs}",
            "net_thread",
            "compute_thread",
            "output_thread",
        ]
        # log the cmd
        cmdstr = " ".join(cmd)
        self.logger.debug("Create subprocess...")
        self.logger.debug(f"cmd: {cmdstr}")
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=datadir, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
        )
        self.logger.debug("Subprocess created...")
        # monitor stdout/stderr in background — routes to run_dir log files and gRPC
        self._monitor_task = asyncio.create_task(_monitor_hashpipe(proc, hp_stdout_logger, hp_stderr_logger))
        # get the hashpipe pid
        self.hashpipe_pid = proc.pid

        WAIT_TIMEOUT = 5  # seconds
        POLL_INTERVAL = 0.05  # seconds
        success = False
        for _ in range(int(WAIT_TIMEOUT / POLL_INTERVAL)):
            success = is_hashpipe_running(self.hashpipe_pid)
            if success:
                break
            await asyncio.sleep(POLL_INTERVAL)
        self.logger.info(f"HASHPIPE instance status: {success}; PID: {self.hashpipe_pid}")
        msg = f"HASHPIPE start failed. \n{vreq_dict=} \n{cmd=}" if not success else ""
        return daq_control_pb2.StartDaqResponse(success=success, message=msg)

    @grpc_error_handler
    async def StopDaq(
        self, request: daq_control_pb2.StopDaqRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.StopDaqResponse:
        self.logger.info("Stop HASHPIPE instance...")
        # data dir and run dir is not used in this method
        # dreq = self._request_to_dict(request)
        # vreq = StopDaqModel(**dreq)
        if self.hashpipe_pid == -1:
            self.logger.info("No HASHPIPE instance is running.")
            return daq_control_pb2.StopDaqResponse(success=True)
        try:
            p = psutil.Process(self.hashpipe_pid)
            p.send_signal(signal.SIGINT)
            await asyncio.get_running_loop().run_in_executor(None, p.wait)
        except psutil.NoSuchProcess:
            # Process already gone (e.g. killed externally) — treat as stopped.
            self.logger.info(f"HASHPIPE process (pid={self.hashpipe_pid}) no longer exists; treating as stopped.")
            self.hashpipe_pid = -1
            return daq_control_pb2.StopDaqResponse(success=True)
        success = is_hashpipe_running(self.hashpipe_pid)
        if success:
            self.logger.warning("HASHPIPE is still running...")
            return daq_control_pb2.StopDaqResponse(success=False)
        else:
            self.hashpipe_pid = -1
            self.logger.info("HASHPIPE instance stopped successfully.")
            return daq_control_pb2.StopDaqResponse(success=True)

    @grpc_error_handler
    async def StatusDaq(
        self, request: daq_control_pb2.DaqStatusRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.DaqStatusResponse:
        self.logger.info("Checking Daq Node status...")
        creq = self._request_to_dict(request)
        vreq = StatusDaqModel(**creq)
        datadir = vreq.data_dir
        # check hashpipe status
        if vreq.check_hashpipe_running:
            self.logger.debug("Checking HASHPIPE status...")
            hashpipe_running = False if self.hashpipe_pid == -1 else is_hashpipe_running(self.hashpipe_pid)
        else:
            hashpipe_running = False
        # check free space
        disk_usage_dict: dict[str, Any] = {}
        if vreq.check_disk_usage:
            self.logger.debug("Checking disk usage...")
            disk_usage_dict = self._check_disk_usage(datadir)
        else:
            disk_usage_dict = {
                "total_disk_space": -1,
                "used_disk_space": -1,
                "free_disk_space": -1,
            }
        disk_usage_struct = Struct()
        disk_usage_struct.update(disk_usage_dict)

        # check run dirs
        run_dirs: list[str] = []
        if vreq.check_run_dirs:
            self.logger.debug("Checking run dirs")
            run_dirs = self._check_run_dirs(datadir)
        # return
        return daq_control_pb2.DaqStatusResponse(
            success=True, hashpipe_running=hashpipe_running, disk_usage=disk_usage_struct, run_dirs=run_dirs
        )

    @grpc_error_handler
    async def CleanupData(
        self, request: daq_control_pb2.CleanupDataRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.CleanupDataResponse:
        self.logger.info("Cleanning up Data...")
        creq = self._request_to_dict(request)
        try:
            vreq = CleanupDataModel(**creq)
        except ValidationError as e:
            msg = f"Validation Error: {e}"
            self.logger.error(msg)
            return daq_control_pb2.CleanupDataResponse(success=False, message=msg)

        datadir = vreq.data_dir
        rundir = vreq.run_dir
        module_id = vreq.module_id
        force = vreq.force

        if self.hashpipe_pid > 0:
            msg = f"HASHPIPE is running, pid[{self.hashpipe_pid}]. "
            if not force:
                msg += "Cleaning up data dir is not allowed."
                self.logger.warning(msg)
                return daq_control_pb2.CleanupDataResponse(success=False, message=msg)
            else:
                msg += f" {force=}: Forcing cleanup..."
                self.logger.warning(msg)

        # clean up the run dir in data dir
        run_dir_path = f"{datadir}/{rundir}"
        module_dir_paths = [f"{datadir}/module_{id}/{rundir}" for id in module_id]
        cleanup_paths = [run_dir_path, *module_dir_paths]

        # clean up the run dir in module_x dir
        msg = ""
        all_cleaned = True
        for cleanup_path in cleanup_paths:
            if not self._cleanup_dir(cleanup_path):
                msg += f"_cleanup_dir failed for {cleanup_path}"
            all_cleaned &= not os.path.exists(cleanup_path)  # noqa: ASYNC240
        if msg:
            self.logger.warning(msg)

        return daq_control_pb2.CleanupDataResponse(success=all_cleaned, message=msg)


async def serve(grpc_port: int = 50051, level: int = logging.DEBUG) -> None:
    """
    Main entry point for running the server.
    Args:
        level: logging level.
                logging.DEBUG, logging.INFO, logging.WARNING or logging.ERROR.
    """
    # 0. create logger
    logger = get_logger(
        "daq_control_server",
        level=level,
        console=True,
        log_dir=SERVER_LOG_DIR,
        grpc_enabled=True,
    )

    # 1. setup gRPC server
    server = grpc.aio.server()

    # 2. add servicer to the server
    servicer = DaqControlServicer(level)
    daq_control_pb2_grpc.add_DaqControlServicer_to_server(servicer, server)

    # 2b. enable gRPC reflection for service discovery
    from grpc_reflection.v1alpha import reflection as grpc_reflection

    SERVICE_NAMES = (
        daq_control_pb2.DESCRIPTOR.services_by_name["DaqControl"].full_name,
        grpc_reflection.SERVICE_NAME,
    )
    grpc_reflection.enable_server_reflection(SERVICE_NAMES, server)

    # 3. bind Ports
    server.add_insecure_port(f"[::]:{grpc_port}")

    logger.info(f"gRPC Server listening on TCP port {grpc_port}")

    # 4. graceful shutdown setup
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Signal received. Initiating shutdown...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, _handle_signal)
    loop.add_signal_handler(signal.SIGTERM, _handle_signal)

    # 5. start serving
    await server.start()
    logger.info("Server started. Press Ctrl+C to stop.")

    await shutdown_event.wait()

    # 6. shutdown sequence
    logger.info("Stopping gRPC server (allowing 5s grace period)...")
    await server.stop(5)

    logger.info("Cleaning up servicer resources...")
    logger.info("Goodbye.")


def main() -> None:
    """Console script entry point (``panoseti-daq-control``)."""
    GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(GRPC_PORT, logging.DEBUG))


if __name__ == "__main__":
    main()
