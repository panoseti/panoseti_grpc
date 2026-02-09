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

import asyncio
import signal

# gRPC Imports 
import grpc
from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc

# Local Imports
from .resources import make_rich_logger
from .util import is_hashpipe_running

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
        self.hashpipe_pid = -1
        self.rootdir = None

    def _create_module_config(self, module_id):
        with open(f'{self.rootdir}/module.config', 'w') as f:
            for id in module_id:
                f.write(f'{id} ')

    async def StartDaq(self, request, context):
        self.logger.info("Starting HASHPIPE instance...")
        # get the parameters from client
        self.rootdir = request.root_dir
        self.logger.debug(f"root_dir: {self.rootdir}")
        daq_ip_addr = request.daq_ip_addr
        self.logger.debug(f"daq_ip_addr: {daq_ip_addr}")
        bindhost = request.bindhost
        self.logger.debug(f"bindhost: {bindhost}")
        max_file_size_mb = request.max_file_size_mb
        self.logger.debug(f"max_file_size_mb: {max_file_size_mb}")
        group_ph_frames = request.group_ph_frames
        self.logger.debug(f"group_ph_frames: {group_ph_frames}")
        run_dir = request.run_dir
        self.logger.debug(f"run_dir: {run_dir}")
        obs = request.obs
        self.logger.debug(f"obs: {obs}")
        module_id = request.module_id
        # create module.config
        self._create_module_config(module_id)
        # create cmdline for start HASHPIPE
        hashpipe_so = f"{self.rootdir}/hashpipe.so"
        rundir = f"{self.rootdir}/{run_dir}"
        configfn = f"{self.rootdir}/module.config"
        # create log files for stdout and stderr
        stdoutfd = open(f"{self.rootdir}/{run_dir}/hp_stdout_/{daq_ip_addr}", "w")
        stderrfd = open(f"{self.rootdir}/{run_dir}/hp_stderr_/{daq_ip_addr}", "w")
        proc = subprocess.Popen(
            ['hashpipe', 
             '-p', hashpipe_so, 
             '-I', '0',
             '-o', f'BINDHOST={bindhost}',
             '-o', f'MAXFILESIZE={max_file_size_mb}',
             '-o', f'GROUPPHFRAMES={group_ph_frames}',
             '-o', f'RUNDIR={rundir}',
             '-o', f'CONFIG={configfn}',
             '-o', f'OBS={obs}',
             'net_thread',
             'compute_thread',
             'output_thread'
             ],
             stdout=stdoutfd,
             stderr=stderrfd,
             start_new_session=True
        )
        stdoutfd.close()
        stderrfd.close()
        # get the hashpipe pid
        self.hashpipe_pid = proc.pid
        status = is_hashpipe_running(self.hashpipe_pid)
        self.logger.info(f"HASHPIPE instance status: {status}; PID: {self.hashpipe_pid}")
        return daq_control_pb2.StartDaqResponse(status=status)
    
    async def StopDaq(self, request, context):
        self.logger.info("Stop HASHPIPE instance...")
        self.rootdir = request.root_dir
        run_dir = request.run_dir
        psutil.Process(self.hashpipe_pid).terminate()
        status = is_hashpipe_running(self.hashpipe_pid)
        if status:
            self.logger.warning("HASHPIPE is still running...")
            return daq_control_pb2.StopDaqResponse(status=False)
        else:
            self.hashpipe_pid = -1
            self.logger.info("HASHPIPE instance stopped successfully.")
            return daq_control_pb2.StopDaqResponse(status=True)
    
    async def StatusDaq(self, request, context):
        self.logger.info('Checking Daq Node status...')
        self.rootdir = request.root_dir
        # check hashpipe status
        if request.check_hashpipe_running:
            self.logger.debug('Checking HASHPIPE status...')
            if self.hashpipe_pid == -1:
                hashpipe_running = False
            else:
                hashpipe_running = is_hashpipe_running(self.hashpipe_pid)
        else:
            hashpipe_status = False
        # check free space
        if request.check_disk_usage:
            self.logger.debug('Checking disk usage...')
            usage = shutil.disk_usage(self.rootdir)
            total_disk_space = usage.total
            used_disk_space = usage.used
            free_disk_space = usage.free
        else:
            total_disk_space = -1
            used_disk_space = -1
            free_disk_space = -1
        return daq_control_pb2.StatusResponse(
            success = True,
            hashpipe_running = hashpipe_running,
            total_disk_space = total_disk_space,
            used_disk_space = used_disk_space,
            free_disk_space = free_disk_space
        )

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
