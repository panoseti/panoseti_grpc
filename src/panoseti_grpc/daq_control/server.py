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
import os
import subprocess
import logging
import psutil
import shutil
from pathlib import Path

import asyncio
import signal

from glob import glob

# gRPC Imports 
os.environ['GRPC_ENABLE_FORK_SUPPORT'] = '0'
import grpc
from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc

# Local Imports
from .resources import make_rich_logger
from .util import is_hashpipe_running

PROCESS = 'hashpipe'

class DaqControlServicer(daq_control_pb2_grpc.DaqControlServicer):
    """
    Implements the Daq Control gRPC service.
    Handles start daq, stop daq and status daq.
    """
    def __init__(self, level = logging.INFO):
        self.logger = make_rich_logger("daq_control_server", level)
        self.logger.info(f"DaqControlServicer initialized")
        self.logger.info(f"[bold green]DaqControl Server Online[/]", extra={"markup": True})
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
            self.logger.warning(f"All of these HASHPIPE instances have been killed.")
            self.kill_processes(hashpipe_pids)

    def _get_pids_by_name(self, name):
        pids = []
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] == name:
                pids.append(proc.info['pid'])
        return len(pids), pids
    
    def kill_processes(self, pids):
        for pid in pids:
            p = psutil.Process(pid)
            p.send_signal(signal.SIGINT)

    def _create_module_config(self, datadir,module_id):
        mconfig = f'{datadir}/module.config'
        self.logger.info(f'Create {mconfig}')
        with open(mconfig, 'w') as f:
            for id in module_id:
                f.write(f'{id} ')

    def _setup_data_directories(self, datadir, rundir, module_id):
        # create directory for config files
        cdirname = f"{datadir}/{rundir}"
        self.logger.info(f'Setup rundir for configs: {cdirname}')
        Path(cdirname).mkdir(parents=True, exist_ok=True)
        # create directory for data
        for m in module_id:
            dirname = f"{datadir}/module_{m}/{rundir}"
            self.logger.info(f'Setup rundir for data: {dirname}')
            Path(dirname).mkdir(parents=True, exist_ok=True)
    
    def _validate_data_dir(self, datadir):
        if not Path(datadir).is_dir():
            msg = f"data directory ({datadir}) not found."
            self.logger.error(msg)
            return False, msg
        else:
            msg = f"Data dir ({datadir}): validated."
            self.logger.debug(msg)
            return True, msg
    
    def _validate_ip_ethport(self, ip, ethport):
        netinfo = {}
        addrs = psutil.net_if_addrs()
        for iface, addr_list in addrs.items():
            for addr in addr_list:
                if addr.family.name == "AF_INET":
                    netinfo[iface] = addr.address
        if ethport not in netinfo:
            msg = f"eth port ({ethport} is not valid.)"
            self.logger.error(msg)
            return False, msg
        elif netinfo[ethport] != ip:
            msg = f"DAQ node IP ({ip}) is incorrect."
            self.logger.error(msg)
            return False, msg
        else:
            msg = f"{ethport} ({ip}): validated."
            self.logger.debug(msg)
            return True, msg
    
    def _check_disk_usage(self, datadir):
        usage = shutil.disk_usage(datadir)
        disk_usage = {
            'total_disk_space' : usage.total,
            'used_disk_space' : usage.used,
            'free_disk_space' : usage.free,
        }
        return disk_usage
    
    def _check_run_dirs(self, datadir):
        return glob(f"{datadir}/*.pffd")
    
    def _cleanup_dir(self, rundir):
        path = Path(rundir)
        if not path.is_dir():
            self.logger.warning(f'Data Directory not exist: {rundir}')
        else:
            self.logger.debug(f"Cleaning up {rundir}")
            shutil.rmtree(path)
            if not path.is_dir():
                self.logger.debug(f"Cleanup successful")
                return True
            else:
                self.logger.debug(f"Cleanup failed")
                return False
            
    async def StartDaq(self, request, context):
        self.logger.info("Starting HASHPIPE instance...")
        # 1. check if we already have HASHPIPE running
        n, pids = self._get_pids_by_name(PROCESS)
        if n > 0:
            msg = f"Found {n} HASHPIPE instances running. pids: {pids}"
            self.logger.warning(msg)
            return daq_control_pb2.StartDaqResponse(success=False, message=msg)
        # 2. check if the data dir exists
        datadir = request.data_dir
        s, msg = self._validate_data_dir(datadir)
        if not s:
            return daq_control_pb2.StartDaqResponse(success=s, message=msg)
        # 3. check if the IP and eth port are valid.
        #    Actually, IP is not important here
        daq_ip_addr = request.daq_ip_addr
        bindhost = request.bindhost
        s, msg = self._validate_ip_ethport(daq_ip_addr, bindhost)
        if not s:
            return daq_control_pb2.StartDaqResponse(success=s, message=msg)
        max_file_size_mb = request.max_file_size_mb
        self.logger.debug(f"max_file_size_mb: {max_file_size_mb}")
        group_ph_frames = request.group_ph_frames
        self.logger.debug(f"group_ph_frames: {group_ph_frames}")
        run_dir = request.run_dir
        self.logger.debug(f"run_dir: {run_dir}")
        obs = request.obs
        self.logger.debug(f"obs: {obs}")
        module_id = request.module_id
        # get the full path for hashpipe.so, rundir and module.config
        hashpipe_so = f"{datadir}/hashpipe.so"
        configfn = f"{datadir}/module.config"
        # create module.config
        self._create_module_config(datadir, module_id)
        # setup data directories
        self._setup_data_directories(datadir, run_dir, module_id)
        # create log files for stdout and stderr
        self.logger.debug('Create log files...')
        stdoutfd = open(f"{datadir}/{run_dir}/hp_stdout_{daq_ip_addr}", "w")
        stderrfd = open(f"{datadir}/{run_dir}/hp_stderr_{daq_ip_addr}", "w")
        # create cmdline for start HASHPIPE
        cmd = [
             'hashpipe',
             '-p', hashpipe_so, 
             '-I', '0',
             '-o', f'BINDHOST={bindhost}',
             '-o', f'MAXFILESIZE={max_file_size_mb}',
             '-o', f'GROUPPHFRAMES={group_ph_frames}',
             '-o', f'RUNDIR={run_dir}',
             '-o', f'CONFIG={configfn}',
             '-o', f'OBS={obs}',
             'net_thread',
             'compute_thread',
             'output_thread'
             ]
        # log the cmd
        cmdstr = " ".join(cmd)
        self.logger.debug('Create subprocess...')
        self.logger.debug(f"cmd: {cmdstr}")
        proc = await asyncio.create_subprocess_exec(
             *cmd,
             cwd = datadir,
             stdout=stdoutfd,
             stderr=stderrfd,
             start_new_session=True
        )

        self.logger.debug('Subprocess created...')
        stdoutfd.close()
        stderrfd.close()
        # get the hashpipe pid
        self.hashpipe_pid = proc.pid
        success = is_hashpipe_running(self.hashpipe_pid)
        self.logger.info(f"HASHPIPE instance status: {success}; PID: {self.hashpipe_pid}")
        return daq_control_pb2.StartDaqResponse(success=success)
    
    async def StopDaq(self, request, context):
        self.logger.info("Stop HASHPIPE instance...")
        # data dir and run dir is not used in this method
        datadir = request.data_dir
        run_dir = request.run_dir
        if self.hashpipe_pid == -1:
            self.logger.info('No HASHPIPE instance is running.')
            return daq_control_pb2.StopDaqResponse(success=True)
        p = psutil.Process(self.hashpipe_pid)
        p.send_signal(signal.SIGINT)
        await asyncio.get_running_loop().run_in_executor(None, p.wait)
        success = is_hashpipe_running(self.hashpipe_pid)
        if success:
            self.logger.warning("HASHPIPE is still running...")
            return daq_control_pb2.StopDaqResponse(success=False)
        else:
            self.hashpipe_pid = -1
            self.logger.info("HASHPIPE instance stopped successfully.")
            return daq_control_pb2.StopDaqResponse(success=True)
    
    async def StatusDaq(self, request, context):
        self.logger.info('Checking Daq Node status...')
        datadir = request.data_dir
        # check hashpipe status
        if request.check_hashpipe_running:
            self.logger.debug('Checking HASHPIPE status...')
            if self.hashpipe_pid == -1:
                hashpipe_running = False
            else:
                hashpipe_running = is_hashpipe_running(self.hashpipe_pid)
        else:
            hashpipe_running = False
        # check free space
        if request.check_disk_usage:
            self.logger.debug('Checking disk usage...')
            disk_usage = self._check_disk_usage(datadir)
        else:
            disk_usage = {
                'total_disk_space' : -1,
                'used_disk_space' : -1,
                'free_disk_space' : -1,
            }
        # check run dirs
        run_dirs = self._check_run_dirs(datadir)
        return daq_control_pb2.StatusDaqResponse(
            success = True,
            hashpipe_running = hashpipe_running,
            disk_usage = disk_usage,
            run_dirs = run_dirs
        )
    
    async def CleanupData(self, request, context):
        self.logger.info('Cleanning up Data...')
        if self.hashpipe_pid > 0:
            self.logger.warning(f'Cleaning up data dir is not allowed')
            msg = f'HASHPIPE is running, pid[{self.hashpipe_pid}]'
            self.logger.warning(msg)
            return daq_control_pb2.CleanupDataResponse(success=False, message=msg)
        datadir = request.data_dir
        rundir = request.run_dir
        module_ids = request.module_ids
        # clean up the run dir in data dir
        self._cleanup_dir(f"{datadir}/{rundir}")
        # clean up the run dir in module_x dir
        for id in module_ids:
            cleanupdir = f"{datadir}/module_{id}/{rundir}"
            if not self._cleanup_dir(cleanupdir):
                msg = f"Fail to cleanup {cleanupdir}"
                return daq_control_pb2.CleanupDataResponse(success=False, message=msg)
        return daq_control_pb2.CleanupDataResponse(success=True)

async def serve(grpc_port, level):
    """
    Main entry point for running the server.
    Args:
        level: logging level.
                logging.DEBUG, logging.INFO, logging.WARNING or logging.ERROR.
    """
    # 0. create logger
    logger = make_rich_logger("daq_control_server", level)

    # 1. setup gRPC server
    server = grpc.aio.server()

    # 2. add servicer to the server
    servicer = DaqControlServicer(level)
    daq_control_pb2_grpc.add_DaqControlServicer_to_server(servicer, server)

    # 3. bind Ports
    server.add_insecure_port(f'[::]:{grpc_port}')

    logger.info(f"gRPC Server listening on TCP port [bold]{grpc_port}[/]")

    # 4. graceful shutdown setup
    shutdown_event = asyncio.Event()
    def _handle_signal(*args):
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
    
if __name__ == "__main__":
    GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

    try:
        asyncio.run(serve(GRPC_PORT, logging.DEBUG))
    except KeyboardInterrupt:
        pass
