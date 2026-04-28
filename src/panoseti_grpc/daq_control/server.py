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
from collections.abc import AsyncGenerator, Callable
from glob import glob
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
from panoseti_grpc.telemetry.logger import get_logger
from panoseti_grpc.util.error_handling import grpc_error_handler
from panoseti_grpc.panoseti_util.control_utils import kill_hk_recorder

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
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_read_stream(proc.stdout, stdout_logger.info))
        tg.create_task(_read_stream(proc.stderr, stderr_logger.error))


class DaqControlServicer(daq_control_pb2_grpc.DaqControlServicer):
    """
    Implements the Daq Control gRPC service.
    Handles start daq, stop daq and status daq.
    """

    def __init__(self, level: int = logging.INFO, grpc_enabled: bool = True) -> None:
        self.logger = get_logger(
            "daq_control_server",
            level=level,
            console=True,
            log_dir=SERVER_LOG_DIR,
            grpc_enabled=grpc_enabled,
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
        self.logger.info("Starting HASHPIPE instance...")
        # 1. check if we already have HASHPIPE running
        n, pids = self._get_pids_by_name(PROCESS)
        if n > 0:
            msg = f"Found {n} HASHPIPE instances running. pids: {pids}"
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
        if not os.path.exists(hashpipe_so) and os.path.exists("/usr/local/lib/panoseti_hashpipe.so"):
            hashpipe_so = "/usr/local/lib/panoseti_hashpipe.so"
            self.logger.info(f"Using baked-in Hashpipe plugin: {hashpipe_so}")

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
        self.logger.info(f"Starting HASHPIPE for run_dir: {run_dir}")
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
        self.logger.info("Create subprocess...")
        self.logger.info(f"cmd: {cmdstr}")
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
        self.logger.info("Stop HASHPIPE instance(s)...")

        # 1. Identify all hashpipe processes (Non-blocking)
        n_initial, pids = await asyncio.to_thread(self._get_pids_by_name, PROCESS)
        if n_initial == 0:
            self.logger.info("No HASHPIPE instance is running.")
            self.hashpipe_pid = -1
            return daq_control_pb2.StopDaqResponse(success=True, message="No processes found.")

        self.logger.info(f"Found {n_initial} HASHPIPE process(es): {pids}")

        # 2. Tier 1: SIGINT to all detected instances
        for pid in pids:
            try:
                p = psutil.Process(pid)
                p.send_signal(signal.SIGINT)
            except psutil.NoSuchProcess:
                continue

        # 3. Graceful Wait Loop (up to 60s)
        WAIT_TIMEOUT = 60.0
        POLL_INTERVAL = 2.0
        elapsed = 0.0
        while elapsed < WAIT_TIMEOUT:
            _, remaining_pids = await asyncio.to_thread(self._get_pids_by_name, PROCESS)
            if not remaining_pids:
                break
            self.logger.info(f"Waiting for {len(remaining_pids)} HASHPIPE process(es) to exit gracefully... ({int(elapsed)}s)")
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        # 4. Tier 2: Escalation to SIGKILL for survivors
        _, final_pids = await asyncio.to_thread(self._get_pids_by_name, PROCESS)
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
        n_remaining, _ = await asyncio.to_thread(self._get_pids_by_name, PROCESS)
        self.hashpipe_pid = -1
        
        success = (n_remaining == 0)
        status_msg = (
            f"Successfully stopped {n_initial} processes." if success else
            f"Failed to stop all processes. {n_remaining} still active."
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
            self.logger.debug("Checking HASHPIPE status...")
            # Consistency Fix: check for ANY running hashpipe process, not just the tracked one.
            n, pids = await asyncio.to_thread(self._get_pids_by_name, PROCESS)
            hashpipe_running = (n > 0)
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
            hashpipe_pid=hashpipe_pid
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
        except (ValueError, TypeError):
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

        if pid_alive:
            msg = f"HASHPIPE is still alive, pid[{parsed_pid}]. Cleanup refused."
            self.logger.warning(msg)
            return daq_control_pb2.CleanupDataResponse(success=False, message=msg)
        elif uncertain:
            msg = f"HASHPIPE status uncertain for pid[{self.hashpipe_pid}]. Refusing cleanup without force=True."
            if not force:
                self.logger.warning(msg)
                return daq_control_pb2.CleanupDataResponse(success=False, message=msg)
            else:
                self.logger.warning(f"{msg} (Force cleanup enabled)")
                self.hashpipe_pid = -1
        elif parsed_pid > 0 and not force:
            # pid_alive is False and not uncertain: process is dead (orphaned)
            # but caller did not pass force=True.
            msg = (
                f"Orphaned HASHPIPE pid[{parsed_pid}] (process dead). "
                "Use force=True to override and clean up."
            )
            self.logger.warning(msg)
            return daq_control_pb2.CleanupDataResponse(success=False, message=msg)
        elif parsed_pid > 0:
            # Process is dead and force=True — allowed; reset tracked PID.
            self.logger.info(f"Orphaned HASHPIPE pid[{parsed_pid}] (dead). Force cleanup allowed.")
            self.hashpipe_pid = -1

        # clean up the run dir in data dir
        run_dir_path = f"{datadir}/{rundir}"
        module_dir_paths = [f"{datadir}/module_{id}/{rundir}" for id in module_id]
        cleanup_paths = [run_dir_path, *module_dir_paths]
        self.logger.info(f"CleanupData: Validating paths: {cleanup_paths}")

        # Async Path Validation: Ensure all paths exist and are directories
        for cp in cleanup_paths:
            cp_async = anyio.Path(cp)
            if not await cp_async.is_dir():
                msg = f"Cleanup path not found or not a directory: {cp}"
                self.logger.warning(msg)
                return daq_control_pb2.CleanupDataResponse(success=False, message=msg)

        # Manifest-digest precondition: when CLEANUP_SELECTIVE is requested with a
        # non-empty manifest_digest, the server re-reads each manifest file, hashes
        # it, and refuses the RPC if the digest doesn't match.  This enforces the
        # "no deletion without verified integrity" invariant (plan §3.2 step 6).
        if vreq.mode == CleanupMode.CLEANUP_SELECTIVE and request.manifest_digest:
            import hashlib as _hashlib

            provided_digest = request.manifest_digest.hex()
            for cp in cleanup_paths:
                cp_path = Path(cp)
                for algo_suffix in ("blake3", "xxh3_128", "sha256"):
                    mf = cp_path / f"manifest.{algo_suffix}"
                    if mf.exists():
                        raw = mf.read_bytes()
                        actual = _hashlib.sha256(raw).hexdigest()
                        if actual != provided_digest:
                            self.logger.error(
                                "Manifest digest mismatch for %s: provided=%s..., actual=%s...",
                                cp,
                                provided_digest[:16],
                                actual[:16],
                            )
                            context.abort(
                                grpc.StatusCode.FAILED_PRECONDITION,
                                f"Manifest digest mismatch for {cp}: "
                                f"expected {provided_digest[:16]}…, got {actual[:16]}…. "
                                "Cleanup refused — verify the transfer before retrying.",
                            )
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
        try:
            vreq = GenerateManifestModel(**dreq)
        except Exception as e:
            msg = f"Validation Error: {e}"
            self.logger.error(msg)
            return daq_control_pb2.GenerateManifestResponse(success=False, message=msg)

        all_entries = []
        t0 = asyncio.get_event_loop().time()
        # Ensure we use absolute resolution to avoid any relative path ambiguity.
        # ASYNC240: Use anyio.Path for non-blocking resolution and checks in async method.
        raw_module_run_dir = anyio.Path(vreq.data_dir / f"module_{vreq.module_id}" / vreq.run_dir)
        
        # Resilience: Highly persistent retry (5s total) to outlast any VirtioFS lag
        module_run_dir = raw_module_run_dir
        for attempt in range(10):
            try:
                # Resolve dynamically inside the loop to catch late-arriving symlinks/mounts
                module_run_dir = await raw_module_run_dir.resolve()
                if await module_run_dir.is_dir():
                    break
            except (OSError, FileNotFoundError):
                # Resolve can fail on missing paths in some environments
                pass

            if attempt < 9:
                self.logger.debug(
                    f"GenerateManifest: Path {raw_module_run_dir} not found/resolved, "
                    f"retrying in 500ms... (attempt {attempt+1}/10)"
                )
                await asyncio.sleep(0.5)
        else:
            msg = f"GenerateManifest: Module run directory not found after 5s: {raw_module_run_dir}"
            self.logger.warning(msg)
            return daq_control_pb2.GenerateManifestResponse(
                success=False,
                message=msg,
            )

        # compute_manifest takes a pathlib.Path
        result = await compute_manifest(Path(str(module_run_dir)), vreq.include_patterns, vreq.algorithm)
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
        self, request: daq_control_pb2.GetManifestRequest, context: grpc.ServicerContext
    ) -> AsyncGenerator[daq_control_pb2.ManifestEntry]:
        self.logger.info("GetManifest called...")

        # ASYNC240: Use anyio.Path for non-blocking path resolution in async generator
        data_dir = await anyio.Path(request.data_dir).resolve()
        run_dir_path = await (data_dir / f"module_{request.module_id}" / request.run_dir).resolve()

        if not run_dir_path.is_relative_to(data_dir):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "run_dir escapes data_dir")
            return

        # Auto-detect manifest file by trying known algorithm suffixes
        manifest_path: anyio.Path | None = None
        for suffix in ("blake3", "xxh3_128", "sha256"):
            candidate = run_dir_path / f"manifest.{suffix}"
            if await candidate.is_file():
                manifest_path = candidate
                break

        if manifest_path is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"No manifest file found in {run_dir_path}")
            return

        async def _read_manifest(path: anyio.Path) -> list[daq_control_pb2.ManifestEntry]:
            entries = []
            async with await path.open() as f:
                async for line in f:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    # Parse: digest  size  mtime_ns  relpath (4 columns)
                    # Backward compat: digest  size  relpath (3 columns, mtime_ns=0)
                    parts = line.split("  ", 3)
                    if len(parts) == 4:
                        digest_hex, size_str, mtime_str, rel_path = parts
                        mtime_ns = int(mtime_str)
                    elif len(parts) == 3:
                        digest_hex, size_str, rel_path = parts
                        mtime_ns = 0
                    else:
                        continue
                    entries.append(
                        daq_control_pb2.ManifestEntry(
                            relative_path=rel_path,
                            digest_hex=digest_hex,
                            size_bytes=int(size_str),
                            mtime_ns=mtime_ns,
                        )
                    )
            return entries

        entries = await _read_manifest(manifest_path)
        for entry in entries:
            yield entry

    @grpc_error_handler
    async def GetTransferStatus(
        self, request: daq_control_pb2.GetTransferStatusRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.GetTransferStatusResponse:
        """Return transfer readiness: hashpipe state, run dirs, free disk, manifest presence."""
        self.logger.info("GetTransferStatus called for run_dir=%s", request.run_dir)
        data_dir = Path(request.data_dir)
        if not data_dir.is_dir():
            return daq_control_pb2.GetTransferStatusResponse(
                success=False, message=f"data_dir not found: {data_dir}"
            )

        hashpipe_running = self.hashpipe_pid > 0 and is_hashpipe_running(self.hashpipe_pid)

        disk = shutil.disk_usage(str(data_dir))
        free_bytes = disk.free
        total_bytes = disk.total

        run_dirs: list[str] = []
        manifest_files: list[str] = []
        if request.run_dir:
            for mod_dir in sorted(data_dir.glob("module_*")):
                run_path = mod_dir / request.run_dir
                if run_path.is_dir():
                    run_dirs.append(str(run_path))
                    for suffix in ("blake3", "xxh3_128", "sha256"):
                        mf = run_path / f"manifest.{suffix}"
                        if mf.exists():
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

        self.logger.info(
            "GetManifestDigest called: data_dir=%s run_dir=%s module_id=%d",
            request.data_dir, request.run_dir, request.module_id,
        )
        module_run_dir = Path(request.data_dir) / f"module_{request.module_id}" / request.run_dir
        if not module_run_dir.is_dir():
            return daq_control_pb2.GetManifestDigestResponse(
                success=False, message=f"Module run dir not found: {module_run_dir}"
            )

        for suffix in ("blake3", "xxh3_128", "sha256"):
            mf = module_run_dir / f"manifest.{suffix}"
            if mf.exists():
                raw = await asyncio.to_thread(mf.read_bytes)
                digest_hex = _hashlib.sha256(raw).hexdigest()
                return daq_control_pb2.GetManifestDigestResponse(
                    success=True,
                    message="",
                    digest_hex=digest_hex,
                    algo_suffix=suffix,
                    manifest_path=str(mf),
                )

        return daq_control_pb2.GetManifestDigestResponse(
            success=False,
            message=f"No manifest file found in {module_run_dir}",
        )

    @grpc_error_handler
    async def RetryFailedTransfer(
        self, request: daq_control_pb2.RetryFailedTransferRequest, context: grpc.ServicerContext
    ) -> daq_control_pb2.RetryFailedTransferResponse:
        """Re-emit a single file's digest so the head node can verify it without a full re-rsync.

        The file must reside under ``data_dir/module_{module_id}/run_dir/`` to prevent
        path traversal.  Returns the file size and blake3/sha256 digest.
        """
        import hashlib as _hashlib

        self.logger.info(
            "RetryFailedTransfer called: data_dir=%s run_dir=%s module_id=%d file_path=%s",
            request.data_dir, request.run_dir, request.module_id, request.file_path,
        )
        data_dir = Path(request.data_dir).resolve()
        module_run_dir = (data_dir / f"module_{request.module_id}" / request.run_dir).resolve()

        file_path = Path(request.file_path)
        if not file_path.is_absolute():
            file_path = (module_run_dir / request.file_path).resolve()
        else:
            file_path = file_path.resolve()

        if not file_path.is_relative_to(module_run_dir):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"file_path escapes module run dir: {request.file_path}",
            )
            return daq_control_pb2.RetryFailedTransferResponse(success=False, message="")

        if not file_path.is_file():
            return daq_control_pb2.RetryFailedTransferResponse(
                success=False, message=f"File not found: {file_path}"
            )

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
