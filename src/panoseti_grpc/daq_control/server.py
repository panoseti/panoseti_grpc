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
import fnmatch
import logging
import os
import shutil
import signal
import socket
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any, Literal, cast

import anyio
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
from panoseti_grpc.panoseti_util.control_utils import kill_hk_recorder
from panoseti_grpc.telemetry.logger import get_logger
from panoseti_grpc.util.error_handling import grpc_error_handler

from .config import CleanupDataModel, CleanupMode, GenerateManifestModel, StartDaqModel, StatusDaqModel
from .manifest import compute_manifest

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
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_read_stream(proc.stdout, stdout_logger.info))
            tg.create_task(_read_stream(proc.stderr, stderr_logger.error))
    finally:
        # Ensure process is reaped by the event loop
        await proc.wait()


class DaqControlServicer(daq_control_pb2_grpc.DaqControlServicer):
    """
    Implements the Daq Control gRPC service.
    Handles start daq, stop daq and status daq.
    """

    def __init__(
        self,
        level: int = logging.INFO,
        grpc_enabled: bool = True,
        hashpipe_path: str = "hashpipe",
        hashpipe_name: str = "hashpipe",
    ) -> None:
        self.logger = get_logger(
            "daq_control_server",
            level=level,
            console=True,
            log_dir=SERVER_LOG_DIR,
            grpc_enabled=grpc_enabled,
        )
        self.logger.info("DaqControlServicer initialized")
        self.logger.info("DaqControl Server Online")
        self.hashpipe_path = hashpipe_path
        self.hashpipe_name = hashpipe_name
        # This is used for recording the hashpipe pid
        n, hashpipe_pids = self._get_pids_by_name(self.hashpipe_name)
        if n == 0:
            self.hashpipe_pid = -1
        elif n == 1:
            self.hashpipe_pid = hashpipe_pids[0]
            self.logger.warning(f"Found 1 {self.hashpipe_name} instance is already running, pid:{self.hashpipe_pid}")
        else:
            self.hashpipe_pid = -1
            self.logger.warning(f"Found {n} {self.hashpipe_name} instances are running, pids: {hashpipe_pids}")
            self.logger.warning(f"All of these {self.hashpipe_name} instances have been killed.")
            self.kill_processes(hashpipe_pids)
        self._lock = asyncio.Lock()

    def _get_pids_by_name(self, name: str) -> tuple[int, list[int]]:
        pids = []
        my_pid = os.getpid()
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                pid = proc.info["pid"]
                if pid == my_pid:
                    continue

                p_name = proc.info["name"] or ""
                p_cmdline = proc.info["cmdline"] or []

                # 1. Direct executable name match (binary)
                if name.lower() == p_name.lower():
                    pids.append(pid)
                    continue

                # 2. Command line match (script or shebang)
                # We match if 'name' is exactly any of the arguments (ignoring paths)
                if any(name.lower() == arg.split("/")[-1].lower() for arg in p_cmdline):
                    pids.append(pid)
                    continue

            except psutil.NoSuchProcess, psutil.AccessDenied:
                continue
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
        # 1. Create and chmod root run directory (for configs)
        cdirname = f"{datadir}/{rundir}"
        self.logger.info(f"Setup rundir for configs: {cdirname}")
        p = Path(cdirname)
        p.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            os.chmod(p, 0o777)

        # 2. Create and chmod module-specific run directories
        for m in module_id:
            mod_dir = Path(datadir) / f"module_{m}"
            run_dir = mod_dir / rundir
            self.logger.info(f"Setup rundir for data: {run_dir}")

            # Ensure parent module_X directory exists and is accessible
            mod_dir.mkdir(parents=True, exist_ok=True)
            with contextlib.suppress(OSError):
                os.chmod(mod_dir, 0o777)

            # Create and chmod the actual run directory
            run_dir.mkdir(parents=True, exist_ok=True)
            with contextlib.suppress(OSError):
                os.chmod(run_dir, 0o777)

    def _check_disk_usage(self, datadir: str | Path) -> dict[str, int]:
        usage = shutil.disk_usage(datadir)
        disk_usage = {
            "total_disk_space": usage.total,
            "used_disk_space": usage.used,
            "free_disk_space": usage.free,
        }
        return disk_usage

    def _check_run_dirs(self, datadir: str | Path) -> list[str]:
        """Returns a list of .pffd directories sorted by modification time, newest first."""
        paths = list(Path(datadir).glob("*.pffd"))
        # Sort by modification time, newest first
        paths.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
        return [str(p) for p in paths]

    def _cleanup_dir(self, rundir: str | Path) -> bool:
        path = Path(rundir)
        if not path.is_dir():
            self.logger.info(f"Data Directory already cleaned (noop): {rundir}")
            return True
        else:
            self.logger.debug(f"Cleaning up {rundir}")
            shutil.rmtree(path)
            if not path.is_dir():
                self.logger.debug("Cleanup successful")
                return True
            else:
                self.logger.debug("Cleanup failed")
                return False

    def _cleanup_dir_selective(
        self,
        run_dir: Path,
        delete_patterns: list[str],
        preserve_patterns: list[str],
    ) -> tuple[int, int, list[str]]:
        """Selective cleanup: delete files matching delete_patterns unless preserved.

        Returns (deleted_count, freed_bytes, preserved_paths).
        Empty directories are left in place.
        """
        deleted_count = 0
        freed_bytes = 0
        preserved_paths: list[str] = []

        if not run_dir.is_dir():
            self.logger.warning(f"Directory does not exist for selective cleanup: {run_dir}")
            return deleted_count, freed_bytes, preserved_paths

        for dirpath, _dirnames, filenames in os.walk(run_dir):
            for filename in filenames:
                filepath = Path(dirpath) / filename
                matches_delete = any(fnmatch.fnmatch(filename, pat) for pat in delete_patterns)
                matches_preserve = any(fnmatch.fnmatch(filename, pat) for pat in preserve_patterns)
                if matches_delete and matches_preserve:
                    preserved_paths.append(str(filepath.relative_to(run_dir)))
                    self.logger.debug(f"Preserved (matches preserve pattern): {filepath}")
                elif matches_delete and not matches_preserve:
                    try:
                        size = filepath.stat().st_size
                        filepath.unlink()
                        deleted_count += 1
                        freed_bytes += size
                        self.logger.debug(f"Deleted: {filepath} ({size} bytes)")
                    except FileNotFoundError:
                        self.logger.debug(f"File already gone (concurrent deletion): {filepath}")

        return deleted_count, freed_bytes, preserved_paths

    def _cleanup_dir_selective_dry_run(
        self,
        run_dir: Path,
        delete_patterns: list[str],
        preserve_patterns: list[str],
    ) -> tuple[int, int, list[str]]:
        """Dry-run counterpart of _cleanup_dir_selective: counts what *would* be deleted.

        Returns (would_delete_count, would_free_bytes, preserved_paths).
        No files are actually removed.
        """
        would_delete = 0
        would_free = 0
        preserved_paths: list[str] = []

        if not run_dir.is_dir():
            return would_delete, would_free, preserved_paths

        for dirpath, _dirnames, filenames in os.walk(run_dir):
            for filename in filenames:
                filepath = Path(dirpath) / filename
                matches_delete = any(fnmatch.fnmatch(filename, pat) for pat in delete_patterns)
                matches_preserve = any(fnmatch.fnmatch(filename, pat) for pat in preserve_patterns)
                if matches_delete and matches_preserve:
                    preserved_paths.append(str(filepath.relative_to(run_dir)))
                elif matches_delete and not matches_preserve:
                    try:
                        would_free += filepath.stat().st_size
                        would_delete += 1
                    except FileNotFoundError:
                        pass

        return would_delete, would_free, preserved_paths

    def _request_to_dict(self, request: Message) -> dict[str, Any]:
        request_dict: dict[str, Any] = MessageToDict(
            request, always_print_fields_with_no_presence=True, preserving_proto_field_name=True
        )
        return request_dict

    @grpc_error_handler
    async def StartDaq(
        self, request: daq_control_pb2.StartDaqRequest, context: grpc.aio.ServicerContext
    ) -> daq_control_pb2.StartDaqResponse:
        self.logger.info(f"Starting {self.hashpipe_name} instance...")
        async with self._lock:
            # 1. check if we already have HASHPIPE running
            n, pids = self._get_pids_by_name(self.hashpipe_name)
            if n > 0:
                msg = f"Found {n} {self.hashpipe_name} instances running. pids: {pids}"
                self.logger.warning(msg)
                return daq_control_pb2.StartDaqResponse(success=False, message=msg)
            # 2. validate request
            try:
                dreq = self._request_to_dict(request)
                vreq = StartDaqModel(**dreq)
            except ValidationError as e:
                msg = f"Validation Error: {e}"
                self.logger.error(msg)
                return daq_control_pb2.StartDaqResponse(success=False, message=msg)

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
            baked_in_so = "/usr/local/lib/panoseti_hashpipe.so"

            # Async check for file existence
            hp_so_exists = await asyncio.to_thread(os.path.exists, hashpipe_so)
            baked_so_exists = await asyncio.to_thread(os.path.exists, baked_in_so)

            if not hp_so_exists and baked_so_exists:
                hashpipe_so = baked_in_so
                self.logger.info(f"Using baked-in Hashpipe plugin: {hashpipe_so}")

            hostname = socket.gethostname()
            configfn = f"{datadir}/module.config"
            # create module.config
            self._create_module_config(datadir, module_id)
            # setup data directories
            self._setup_data_directories(datadir, run_dir, module_id)
            # create per-run loggers for hashpipe stdout and stderr
            run_dir_path = str(Path(datadir) / run_dir)
            hp_stdout_logger = get_logger(
                f"hp_stdout_{hostname}",
                log_dir=run_dir_path,
                grpc_enabled=True,
                console=False,
            )
            hp_stderr_logger = get_logger(
                f"hp_stderr_{hostname}",
                log_dir=run_dir_path,
                grpc_enabled=True,
                console=False,
            )
            # Force file creation by logging an initial message
            hp_stdout_logger.info(f"--- {self.hashpipe_name.upper()} STDOUT LOG STARTED for run: {run_dir} ---")
            hp_stderr_logger.info(f"--- {self.hashpipe_name.upper()} STDERR LOG STARTED for run: {run_dir} ---")

            # create cmdline for start HASHPIPE
            self.logger.info(f"Starting {self.hashpipe_name} for run_dir: {run_dir}")

            # Use python3 if it's a python script
            hp_bin = [self.hashpipe_path]
            if self.hashpipe_path.endswith(".py"):
                hp_bin = ["python3", self.hashpipe_path]

            cmd = [
                *hp_bin,
                "-p",
                hashpipe_so,
                "-I",
                "0",
                "-o",
                f"BINDHOST={bindhost}",
                "-o",
                f"MAXFILESIZE={max_file_size_mb}",
                "-o",
                f"GROUPPHFRAMES={int(group_ph_frames)}",
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
            self.logger.info("Create subprocess...")
            self.logger.info(f"cmd: {cmdstr}")
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=datadir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
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
            success = is_hashpipe_running(self.hashpipe_pid, name=self.hashpipe_name)
            if success:
                break
            await asyncio.sleep(POLL_INTERVAL)
        self.logger.info(f"{self.hashpipe_name} instance status: {success}; PID: {self.hashpipe_pid}")
        msg = f"{self.hashpipe_name} start failed. \n{vreq_dict=} \n{cmd=}" if not success else ""
        return daq_control_pb2.StartDaqResponse(success=success, message=msg)

    @grpc_error_handler
    async def StopDaq(
        self, request: daq_control_pb2.StopDaqRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.StopDaqResponse:
        self.logger.info(f"Stop {self.hashpipe_name} instance(s)...")

        # 1. Identify all hashpipe processes (Non-blocking)
        n_initial, pids = await asyncio.to_thread(self._get_pids_by_name, self.hashpipe_name)
        if n_initial == 0:
            self.logger.info(f"No {self.hashpipe_name} instance is running.")
            self.hashpipe_pid = -1
            return daq_control_pb2.StopDaqResponse(success=True, message="No processes found.")

        self.logger.info(f"Found {n_initial} {self.hashpipe_name} process(es): {pids}")

        # 2. Tier 1: SIGINT to all detected instances
        for pid in pids:
            try:
                p = psutil.Process(pid)
                p.send_signal(signal.SIGINT)
            except psutil.NoSuchProcess:
                continue

        # 3. Graceful Wait Loop (up to 10s)
        WAIT_TIMEOUT = 10.0
        POLL_INTERVAL = 1.0
        elapsed = 0.0
        while elapsed < WAIT_TIMEOUT:
            _, remaining_pids = await asyncio.to_thread(self._get_pids_by_name, self.hashpipe_name)
            if not remaining_pids:
                break
            self.logger.info(
                f"Waiting for {len(remaining_pids)} {self.hashpipe_name} "
                f"process(es) to exit gracefully... ({int(elapsed)}s)"
            )
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        # 4. Tier 2: Escalation to SIGKILL for survivors
        _, final_pids = await asyncio.to_thread(self._get_pids_by_name, self.hashpipe_name)
        killed_count = 0
        if final_pids:
            self.logger.warning(f"{len(final_pids)} process(es) refused SIGINT. Escalating to SIGKILL: {final_pids}")
            for pid in final_pids:
                try:
                    p = psutil.Process(pid)
                    p.kill()
                    killed_count += 1
                except psutil.NoSuchProcess:
                    continue
            # Brief wait for OS to reap
            await asyncio.sleep(1.0)

        # 5. Cleanup sidecars (HK recorder)
        await asyncio.to_thread(kill_hk_recorder)

        # 6. Final verification
        n_remaining, _ = await asyncio.to_thread(self._get_pids_by_name, self.hashpipe_name)
        self.hashpipe_pid = -1

        success = n_remaining == 0
        status_msg = (
            f"Successfully stopped {n_initial} processes."
            if success
            else f"Failed to stop all processes. {n_remaining} still active."
        )
        if killed_count > 0:
            status_msg += f" ({killed_count} required SIGKILL escalation)."

        self.logger.info(status_msg)
        return daq_control_pb2.StopDaqResponse(success=success, message=status_msg)

    @grpc_error_handler
    async def StatusDaq(
        self, request: daq_control_pb2.DaqStatusRequest, context: grpc.aio.ServicerContext
    ) -> daq_control_pb2.DaqStatusResponse:
        self.logger.info("Checking Daq Node status...")
        try:
            creq = self._request_to_dict(request)
            vreq = StatusDaqModel(**creq)
        except ValidationError as e:
            msg = f"Validation Error: {e}"
            self.logger.error(msg)
            return daq_control_pb2.DaqStatusResponse(success=False)  # StatusResponse doesn't have message field

        datadir = vreq.data_dir
        hashpipe_pid = -1
        # check hashpipe status
        if vreq.check_hashpipe_running:
            self.logger.debug(f"Checking {self.hashpipe_name} status...")
            # Consistency Fix: check for ANY running hashpipe process, not just the tracked one.
            n, pids = await asyncio.to_thread(self._get_pids_by_name, self.hashpipe_name)
            hashpipe_running = n > 0
            if n > 0:
                hashpipe_pid = pids[0]
            # Update tracked pid if exactly one found and we didn't have one
            if n == 1 and self.hashpipe_pid == -1:
                self.hashpipe_pid = pids[0]
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
            success=True,
            hashpipe_running=hashpipe_running,
            disk_usage=disk_usage_struct,
            run_dirs=run_dirs,
            hashpipe_pid=hashpipe_pid,
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

        # Defensive liveness check (SC-010 resolution)
        pid_alive = False
        uncertain = False
        parsed_pid = -1

        try:
            parsed_pid = int(self.hashpipe_pid)
        except ValueError, TypeError:
            if self.hashpipe_pid != -1:
                uncertain = True

        if not uncertain and parsed_pid > 0:
            try:
                # os.kill(pid, 0) verifies process existence and access
                os.kill(parsed_pid, 0)
                pid_alive = True
            except ProcessLookupError:
                # PID does not exist — safe to clean up
                pid_alive = False
            except PermissionError:
                # Process alive but owned by another user — treat as alive (unsafe)
                pid_alive = True
            except Exception as e:
                self.logger.error(f"Unexpected error during liveness check: {e}")
                uncertain = True

        if not pid_alive and not uncertain:
            # Fallback: check for ANY running hashpipe process matching the name.
            # This handles cases where the tracked PID is lost due to server restart.
            n, pids = await asyncio.to_thread(self._get_pids_by_name, self.hashpipe_name)
            if n > 0:
                pid_alive = True
                parsed_pid = pids[0]
                # Consistency Fix: Update tracked pid if exactly one found
                if n == 1:
                    self.hashpipe_pid = pids[0]

        if pid_alive:
            msg = f"HASHPIPE is still alive, pid[{parsed_pid}]. Cleanup refused."
            self.logger.warning(msg)
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
        elif uncertain:
            msg = f"HASHPIPE status uncertain for pid[{self.hashpipe_pid}]. Refusing cleanup without force=True."
            if not force:
                self.logger.warning(msg)
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
            else:
                self.logger.warning(f"{msg} (Force cleanup enabled)")
                self.hashpipe_pid = -1
        elif parsed_pid > 0 and not force:
            # pid_alive is False and not uncertain: process is dead (orphaned)
            # but caller did not pass force=True.
            msg = f"Orphaned HASHPIPE pid[{parsed_pid}] (process dead). Use force=True to override and clean up."
            self.logger.warning(msg)
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
        elif parsed_pid > 0:
            # Process is dead and force=True — allowed; reset tracked PID.
            self.logger.info(f"Orphaned HASHPIPE pid[{parsed_pid}] (dead). Force cleanup allowed.")
            self.hashpipe_pid = -1

        # clean up the run dir in data dir
        run_dir_path = f"{datadir}/{rundir}"
        module_dir_paths = [f"{datadir}/module_{id}/{rundir}" for id in module_id]
        cleanup_paths = [run_dir_path, *module_dir_paths]
        self.logger.info(f"CleanupData: Validating paths: {cleanup_paths}")

        # Manifest-digest precondition: when CLEANUP_SELECTIVE is requested with a
        # non-empty manifest_digest, the server re-reads each manifest file, hashes
        # it, and refuses the RPC if the digest doesn't match.  This enforces the
        # "no deletion without verified integrity" invariant (plan §3.2 step 6).
        if vreq.mode == CleanupMode.CLEANUP_SELECTIVE and request.manifest_digest:
            import hashlib as _hashlib

            provided_digest = request.manifest_digest.hex()
            hostname = socket.gethostname()

            for cp in cleanup_paths:
                cp_path = Path(cp)
                # 1. Check new format: dp_manifest.node_<hostname>.algo_<algo>.txt
                pattern = f"dp_manifest.node_{hostname}.algo_*.txt"

                def _glob_manifests(p: Path = cp_path, pat: str = pattern) -> list[Path]:
                    return list(p.glob(pat))

                manifest_files = await asyncio.to_thread(_glob_manifests)

                # 2. Fall back to legacy format: manifest.<algo>
                if not manifest_files:
                    for suffix in ("blake3", "xxh3_128", "sha256"):
                        candidate = cp_path / f"manifest.{suffix}"
                        if candidate.exists():
                            manifest_files.append(candidate)
                            break

                for mf in manifest_files:
                    raw = mf.read_bytes()
                    actual = _hashlib.sha256(raw).hexdigest()
                    if actual != provided_digest:
                        self.logger.error(
                            "Manifest digest mismatch for %s: provided=%s..., actual=%s...",
                            mf,
                            provided_digest[:16],
                            actual[:16],
                        )
                        await context.abort(
                            grpc.StatusCode.FAILED_PRECONDITION,
                            f"Manifest digest mismatch for {mf.name}: "
                            f"expected {provided_digest[:16]}…, got {actual[:16]}…. "
                            "Cleanup refused — verify the transfer before retrying.",
                        )
                    # If we found a manifest and it matched, we are satisfied for this path
                    break

        if vreq.mode == CleanupMode.CLEANUP_SELECTIVE:
            dry_run: bool = bool(request.dry_run)
            total_deleted = 0
            total_freed = 0
            all_preserved: list[str] = []
            msg = ""
            for cleanup_path in cleanup_paths:
                if dry_run:
                    deleted, freed, preserved = self._cleanup_dir_selective_dry_run(
                        Path(cleanup_path),
                        vreq.delete_patterns,
                        vreq.preserve_patterns,
                    )
                else:
                    deleted, freed, preserved = self._cleanup_dir_selective(
                        Path(cleanup_path),
                        vreq.delete_patterns,
                        vreq.preserve_patterns,
                    )
                total_deleted += deleted
                total_freed += freed
                all_preserved.extend(preserved)
            if dry_run:
                msg = "dry_run=True: no files deleted"
            return daq_control_pb2.CleanupDataResponse(
                success=True,
                message=msg,
                deleted_count=total_deleted,
                freed_bytes=total_freed,
                preserved_paths=all_preserved,
            )

        # CLEANUP_FULL: existing rmtree logic
        msg = ""
        all_cleaned = True
        for cleanup_path in cleanup_paths:
            if not self._cleanup_dir(cleanup_path):
                msg += f"_cleanup_dir failed for {cleanup_path}"
            # ASYNC240: Use anyio.Path for non-blocking file existence checks in async method
            path_obj = anyio.Path(cleanup_path)
            all_cleaned &= not await path_obj.exists()
        if msg:
            self.logger.warning(msg)

        return daq_control_pb2.CleanupDataResponse(success=all_cleaned, message=msg)

    @grpc_error_handler
    async def GenerateManifest(
        self, request: daq_control_pb2.GenerateManifestRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.GenerateManifestResponse:
        self.logger.info("GenerateManifest called...")
        dreq = self._request_to_dict(request)
        # Proto string fields default to "" when not set; strip it so the
        # Pydantic Literal default ("blake3") takes effect instead of
        # failing validation with an empty string.
        if not dreq.get("algorithm"):
            dreq.pop("algorithm", None)

        # Pop empty include_patterns so Pydantic default takes over.
        # Check explicitly for empty list (what gRPC yields for unset repeated field).
        if "include_patterns" in dreq and not dreq["include_patterns"]:
            dreq.pop("include_patterns")

        try:
            vreq = GenerateManifestModel(**dreq)
        except Exception as e:
            msg = f"Validation Error: {e}"
            self.logger.error(msg)
            return daq_control_pb2.GenerateManifestResponse(success=False, message=msg)

        all_entries = []
        t0 = asyncio.get_event_loop().time()

        # Identify all directories to be hashed
        # 1. Root run directory (configuration files)
        root_run_dir = Path(vreq.data_dir) / vreq.run_dir

        # 2. Module directories (science files)
        source_dirs = [root_run_dir]
        for mid in vreq.module_id:
            source_dirs.append(Path(vreq.data_dir) / f"module_{mid}" / vreq.run_dir)

        # Resilience: Highly persistent retry (5s total) to outlast any VirtioFS lag.
        # We wait for the root directory as a proxy for the whole run setup.
        root_run_dir_async = anyio.Path(root_run_dir)
        for attempt in range(10):
            try:
                # Resolve dynamically inside the loop to catch late-arriving symlinks/mounts
                resolved_root = await root_run_dir_async.resolve()
                if await resolved_root.is_dir():
                    break
            except OSError, FileNotFoundError:
                pass

            if attempt < 9:
                self.logger.debug(
                    f"GenerateManifest: Root path {root_run_dir} not found/resolved, "
                    f"retrying in 500ms... (attempt {attempt + 1}/10)"
                )
                await asyncio.sleep(0.5)
        else:
            msg = f"GenerateManifest: Root run directory not found after 5s: {root_run_dir}"
            self.logger.warning(msg)
            return daq_control_pb2.GenerateManifestResponse(
                success=False,
                message=msg,
            )

        # Compute a single manifest for all source directories.
        # compute_manifest handles list of source dirs and saves to output_dir.
        result = await compute_manifest(source_dirs, root_run_dir, vreq.include_patterns, vreq.algorithm)
        all_entries.extend(result.entries)
        manifest_path = str(result.manifest_path)
        # Type narrowing for MyPy literal compatibility
        algorithm = cast(Literal["blake3", "xxh3_128"], result.algorithm)
        elapsed = asyncio.get_event_loop().time() - t0
        total_bytes = sum(e.size_bytes for e in all_entries)
        self.logger.info(
            f"Manifest generated: {len(all_entries)} files, {total_bytes} bytes, algo={algorithm}, path={manifest_path}"
        )
        return daq_control_pb2.GenerateManifestResponse(
            success=True,
            message="",
            manifest_path=manifest_path,
            file_count=len(all_entries),
            total_bytes=total_bytes,
            elapsed_seconds=elapsed,
            algorithm=algorithm,
        )

    @grpc_error_handler
    async def GetManifest(
        self, request: daq_control_pb2.GetManifestRequest, context: grpc.aio.ServicerContext
    ) -> AsyncGenerator[daq_control_pb2.ManifestEntry]:
        self.logger.info("GetManifest called...")

        # ASYNC240: Use anyio.Path for non-blocking path resolution in async generator
        data_dir = await anyio.Path(request.data_dir).resolve()

        # Manifest can now be in the root run dir or module run dir.
        root_run_dir = data_dir / request.run_dir
        module_run_dirs = [data_dir / f"module_{mid}" / request.run_dir for mid in request.module_id]

        # Resilience: Highly persistent retry (5s total) to outlast any VirtioFS lag.
        manifest_path: anyio.Path | None = None
        for attempt in range(10):
            # Check for new format in root run dir: dp_manifest.node_<hostname>.algo_<algo>.txt
            if await root_run_dir.is_dir():
                async for entry in root_run_dir.glob("dp_manifest.node_*.algo_*.txt"):
                    if await entry.is_file():
                        manifest_path = entry
                        break
                if manifest_path:
                    break

            # Fallback: check for legacy format in module run dirs: manifest.<algo>
            for mdir in module_run_dirs:
                if await mdir.is_dir():
                    for suffix in ("blake3", "xxh3_128", "sha256"):
                        candidate = mdir / f"manifest.{suffix}"
                        if await candidate.is_file():
                            manifest_path = candidate
                            break
                if manifest_path:
                    break

            if attempt < 9:
                await asyncio.sleep(0.5)

        if manifest_path is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"No manifest file found for run {request.run_dir}")
            return

        async with await manifest_path.open() as f:
            async for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                # Parse: digest  size  mtime_ns  relpath (4 columns)
                parts = line.split("  ", 3)
                if len(parts) < 3:
                    continue

                if len(parts) == 4:
                    digest_hex, size_str, mtime_str, rel_path = parts
                    mtime_ns = int(mtime_str)
                else:
                    digest_hex, size_str, rel_path = parts
                    mtime_ns = 0

                yield daq_control_pb2.ManifestEntry(
                    relative_path=rel_path,
                    digest_hex=digest_hex,
                    size_bytes=int(size_str),
                    mtime_ns=mtime_ns,
                )

    @grpc_error_handler
    async def GetTransferStatus(
        self, request: daq_control_pb2.GetTransferStatusRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.GetTransferStatusResponse:
        """Return transfer readiness: hashpipe state, run dirs, free disk, manifest presence."""
        self.logger.info("GetTransferStatus called for run_dir=%s", request.run_dir)
        data_dir = Path(request.data_dir)
        data_dir_is_dir = await asyncio.to_thread(data_dir.is_dir)
        if not data_dir_is_dir:
            return daq_control_pb2.GetTransferStatusResponse(success=False, message=f"data_dir not found: {data_dir}")

        hashpipe_running = self.hashpipe_pid > 0 and await asyncio.to_thread(
            is_hashpipe_running, self.hashpipe_pid, name=self.hashpipe_name
        )

        disk = await asyncio.to_thread(shutil.disk_usage, str(data_dir))
        free_bytes = disk.free
        total_bytes = disk.total

        run_dirs: list[str] = []
        manifest_files: list[str] = []
        if request.run_dir:
            # Check root run dir for new format
            root_path = data_dir / request.run_dir
            root_path_is_dir = await asyncio.to_thread(root_path.is_dir)
            if root_path_is_dir:
                run_dirs.append(str(root_path))
                pattern = "dp_manifest.node_*.algo_*.txt"

                def _glob_root_manifests(p: Path = root_path, pat: str = pattern) -> list[Path]:
                    return list(p.glob(pat))

                root_manifests = await asyncio.to_thread(_glob_root_manifests)
                for mf in root_manifests:
                    manifest_files.append(str(mf))

            # Check module dirs for legacy or new format
            pattern = "module_*"

            def _glob_module_dirs(p: Path = data_dir, pat: str = pattern) -> list[Path]:
                return sorted(p.glob(pat))

            module_dirs = await asyncio.to_thread(_glob_module_dirs)
            for mod_dir in module_dirs:
                run_path = mod_dir / request.run_dir
                run_path_is_dir = await asyncio.to_thread(run_path.is_dir)
                if run_path_is_dir:
                    run_dirs.append(str(run_path))
                    for suffix in ("blake3", "xxh3_128", "sha256"):
                        mf = run_path / f"manifest.{suffix}"
                        mf_exists = await asyncio.to_thread(mf.exists)
                        if mf_exists:
                            manifest_files.append(str(mf))
                            break

        return daq_control_pb2.GetTransferStatusResponse(
            success=True,
            message="",
            hashpipe_running=hashpipe_running,
            free_bytes=free_bytes,
            total_bytes=total_bytes,
            run_dirs=run_dirs,
            manifest_files=manifest_files,
        )

    @grpc_error_handler
    async def GetManifestDigest(
        self, request: daq_control_pb2.GetManifestDigestRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.GetManifestDigestResponse:
        """Return the SHA-256 hex digest of the on-disk manifest file for a module/run.

        The digest is used by the head node to populate ``manifest_digest`` in
        ``CleanupDataRequest`` and satisfy the CLEANUP_SELECTIVE integrity precondition.
        """
        import hashlib as _hashlib

        self.logger.info("GetManifestDigest called...")

        data_dir = await anyio.Path(request.data_dir).resolve()
        root_run_dir = data_dir / request.run_dir
        module_run_dirs = [data_dir / f"module_{mid}" / request.run_dir for mid in request.module_id]

        manifest_path: anyio.Path | None = None
        # Check new format
        if await root_run_dir.is_dir():
            async for entry in root_run_dir.glob("dp_manifest.node_*.algo_*.txt"):
                if await entry.is_file():
                    manifest_path = entry
                    break

        # Check legacy format
        if not manifest_path:
            for mdir in module_run_dirs:
                if await mdir.is_dir():
                    for suffix in ("blake3", "xxh3_128", "sha256"):
                        candidate = mdir / f"manifest.{suffix}"
                        if await candidate.is_file():
                            manifest_path = candidate
                            break
                if manifest_path:
                    break

        if manifest_path is None:
            msg = f"No manifest file found for run {request.run_dir}"
            self.logger.warning(msg)
            return daq_control_pb2.GetManifestDigestResponse(success=False, message=msg)

        try:
            raw = await manifest_path.read_bytes()
            digest_hex = _hashlib.sha256(raw).hexdigest()
            # Infer algo suffix for legacy or new format
            name = manifest_path.name
            if ".algo_" in name:
                algo_suffix = name.split(".algo_")[1].split(".")[0]
            else:
                algo_suffix = manifest_path.suffix.lstrip(".")

            return daq_control_pb2.GetManifestDigestResponse(
                success=True,
                digest_hex=digest_hex,
                algo_suffix=algo_suffix,
                manifest_path=str(manifest_path),
            )
        except Exception as e:
            msg = f"Error reading manifest: {e}"
            self.logger.error(msg)
            return daq_control_pb2.GetManifestDigestResponse(success=False, message=msg)

    @grpc_error_handler
    async def RetryFailedTransfer(
        self, request: daq_control_pb2.RetryFailedTransferRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.RetryFailedTransferResponse:
        """Re-emit a single file's digest so the head node can verify it without a full re-rsync.

        The file must reside under ``data_dir/module_{module_id}/run_dir/`` to prevent
        path traversal.  Returns the file size and blake3/sha256 digest.
        """
        import hashlib as _hashlib

        module_id = request.module_id[0] if request.module_id else 0
        self.logger.info(
            "RetryFailedTransfer called: data_dir=%s run_dir=%s module_id=%d file_path=%s",
            request.data_dir,
            request.run_dir,
            module_id,
            request.file_path,
        )

        def _resolve_data_dir(d: str = request.data_dir) -> Path:
            return Path(d).resolve()

        data_dir = await asyncio.to_thread(_resolve_data_dir)

        def _resolve_module_run_dir(d: Path = data_dir, mid: int = module_id, rd: str = request.run_dir) -> Path:
            return (d / f"module_{mid}" / rd).resolve()

        module_run_dir = await asyncio.to_thread(_resolve_module_run_dir)

        file_path = Path(request.file_path)

        def _check_absolute(p: Path = file_path) -> bool:
            return p.is_absolute()

        file_path_is_absolute = await asyncio.to_thread(_check_absolute)

        if not file_path_is_absolute:

            def _resolve_relative_file_path(mrd: Path = module_run_dir, fp: str = request.file_path) -> Path:
                return (mrd / fp).resolve()

            file_path = await asyncio.to_thread(_resolve_relative_file_path)
        else:

            def _resolve_absolute_file_path(p: Path = file_path) -> Path:
                return p.resolve()

            file_path = await asyncio.to_thread(_resolve_absolute_file_path)

        if not file_path.is_relative_to(module_run_dir):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"file_path escapes module run dir: {request.file_path}",
            )
            return daq_control_pb2.RetryFailedTransferResponse(success=False, message="")

        if not file_path.is_file():
            return daq_control_pb2.RetryFailedTransferResponse(success=False, message=f"File not found: {file_path}")

        raw = await asyncio.to_thread(file_path.read_bytes)
        digest_hex = _hashlib.sha256(raw).hexdigest()
        return daq_control_pb2.RetryFailedTransferResponse(
            success=True,
            message="",
            size_bytes=len(raw),
            digest_hex=digest_hex,
            algorithm="sha256",
        )


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
