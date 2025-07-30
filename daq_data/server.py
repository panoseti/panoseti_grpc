#!/usr/bin/env python3

"""
The Python implementation of a gRPC DaqData server.

Requires following to function correctly:
    1. A POSIX-compliant operating system.
    2. All Python packages specified in requirements.txt.
    3. A connection to a panoseti module.
"""
from collections import defaultdict
from pathlib import Path
import os
import asyncio
import uuid
from threading import Event, Thread
from glob import glob
from contextlib import asynccontextmanager
from typing import List, Callable, Tuple, Any, Dict, AsyncIterator
from dataclasses import dataclass, field
import logging
import json
import sys
import time
import urllib.parse

# async libraries
from watchfiles import awatch, Change

## --- gRPC imports ---
import grpc

# gRPC reflection service: allows clients to discover available RPCs
from grpc_reflection.v1alpha import reflection

# standard gRPC protobuf types + utility functions
from google.protobuf.struct_pb2 import Struct
from google.protobuf.empty_pb2 import Empty
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf import timestamp_pb2

# protoc-generated marshalling / demarshalling code
from daq_data import daq_data_pb2, daq_data_pb2_grpc
from .daq_data_pb2 import PanoImage, StreamImagesResponse, StreamImagesRequest, InitHpIoRequest, InitHpIoResponse

## --- daq_data utils ---
from .resources import make_rich_logger, get_dp_cfg, CFG_DIR, get_daq_active_file, get_sim_pff_path, is_daq_active_sync, is_daq_active
from .hp_io_manager import HpIoManager
from .testing import is_os_posix

from panoseti_util import pff


def daq_sim_thread_fn(
    sim_cfg: Dict[str, Any],
    update_interval: float,
    stop_io: Event,
    sim_valid: Event,
    logger: logging.Logger,
) -> None:
    """Simulate hashpipe data stream: Read a real file and write to a fake file into the following file structure:
    Simulated directory structure:
        simulated_data_dir/
            ├── real_run_dir/
            │   └── obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd
            │       ├── real_movie_pff [seqno 0]
            │       └── real_pulse_height_pffs [seqno 0]
            │
            ├── module_1/
            │   └── obs_SIMULATE/
            │       ├── simulated_movie_pff [seqno 0]
            │       │   ...
            │       ├── simulated_movie_pff [seqno M1]
            │       ├── simulated_pulse_height_pff [seqno 0]
            │       │   ...
            │       └── simulated_pulse_height_pffs [seqno P1]
            │
            ├── module_2/
            │   └── obs_SIMULATE/
            │       ...
            │
            └── module_N/
                └── obs_SIMULATE/
                    ...

    To simulate the multi-file creation behavior of the daq software due to the max file size parameter,
    every [frames_per_pff] frames, create a new file of each type.
    """
    logger.info("hp_sim thread started")

    # unpack source file info from sim_cfg
    frames_per_pff = sim_cfg['frames_per_pff']
    movie_type = sim_cfg['movie_type']
    ph_type = sim_cfg['ph_type']
    do_ph = do_movie = False
    if sim_cfg['do_ph'] and sim_cfg['do_movie']:
        do_ph = do_movie = True
    elif sim_cfg['do_ph']:
        do_ph = True
    elif sim_cfg['do_movie']:
        do_movie = True
    else:
        raise ValueError("at least one of 'do_ph' and 'do_movie' must be True in sim_cfg['data_products']!")
    data_products = [ph_type, movie_type]
    dp_cfg = get_dp_cfg(data_products)

    simulated_data_files = []
    daq_active_files = []
    active_pff_files = dict()
    try:
        # prevent multiple server instances from running this thread
        daq_active_files = [get_daq_active_file(sim_cfg, module_id=mid) for mid in sim_cfg['sim_module_ids']]
        daq_active = is_daq_active_sync(simulate_daq=True, sim_cfg=sim_cfg)

        if daq_active:
            emsg = "hp_sim thread is already running on another server instance!"
            logger.critical(emsg)
            raise RuntimeError(emsg)

        # create files to signal daq is in progress
        for daq_active_file in daq_active_files:
            with open(daq_active_file, "w") as f:
                f.write("1")

        # open real pff files for reading
        movie_src_path = get_sim_pff_path(sim_cfg, module_id=sim_cfg['real_module_id'], seqno=0, is_ph=False, is_simulated=False)
        #ph_src_path = get_sim_pff_path(sim_cfg, module_id=sim_cfg['real_module_id'], seqno=0, is_ph=True, is_simulated=False)
        ph_src_path = get_sim_pff_path(sim_cfg, module_id=3, seqno=0, is_ph=True, is_simulated=False)
        with (open(movie_src_path, "rb") as movie_src, open(ph_src_path, "rb") as ph_src):
            # get file info, e.g. frame size from the ph and img source files
            (movie_frame_size, movie_nframes, first_t, last_t) = pff.img_info(movie_src, dp_cfg[movie_type]['bytes_per_image'])
            movie_src.seek(0, os.SEEK_SET)
            logger.info(f"movie src: {movie_frame_size=}, {movie_nframes=}")

            (ph_frame_size, ph_nframes, first_t, last_t) = pff.img_info(ph_src, dp_cfg[ph_type]['bytes_per_image'])
            logger.info(f"ph src: {ph_frame_size=}, {ph_nframes=}")
            ph_src.seek(0, os.SEEK_SET)

            # copy frames from [dp]_src to dp_dst to simulate data acquisition software
            # fnum = 0
            ph_fnum = movie_fnum = 0
            ph_seqno = movie_seqno = -1
            sim_valid.set()
            while not stop_io.is_set() and ph_fnum < ph_nframes and movie_fnum < movie_nframes:
                # check if new simulated files should be created
                if int(ph_fnum / frames_per_pff) > ph_seqno:
                    ph_seqno += 1
                    logger.debug(f"new ph_seqno={ph_seqno}")
                    for module_id in sim_cfg['sim_module_ids']:
                        if module_id not in active_pff_files:
                            active_pff_files[module_id] = {'movie': None, 'ph': None}
                        elif active_pff_files[module_id]['ph'] is not None:
                            active_pff_files[module_id]['ph'].close()
                        ph_dest_path = get_sim_pff_path(sim_cfg, module_id, seqno=ph_seqno, is_ph=True, is_simulated=True)
                        active_pff_files[module_id]['ph'] = open(ph_dest_path, 'ab')
                        simulated_data_files.append(ph_dest_path)
                        logger.debug(f"new {ph_dest_path=}")

                if int(movie_fnum / frames_per_pff) > movie_seqno:
                    movie_seqno += 1
                    logger.debug(f"new movie_seqno={movie_seqno}")
                    for module_id in sim_cfg['sim_module_ids']:
                        if module_id not in active_pff_files:
                            active_pff_files[module_id] = {'movie': None, 'ph': None}
                        elif active_pff_files[module_id]['movie'] is not None:
                            active_pff_files[module_id]['movie'].close()
                        movie_dest_path = get_sim_pff_path(sim_cfg, module_id, seqno=movie_seqno, is_ph=False, is_simulated=True)
                        active_pff_files[module_id]['movie'] = open(movie_dest_path, 'ab')
                        simulated_data_files.append(movie_dest_path)
                        logger.debug(f"new {movie_dest_path=}")

                # read data from real pff files and broadcast it to all simulated run directories
                if do_ph:
                    ph_data = ph_src.read(ph_frame_size)
                    # ph_data += np.random.poisson(lam=750, size=dp_cfg[ph_type]['shape'])
                    for module_id in sim_cfg['sim_module_ids']:
                        ph_dst = active_pff_files[module_id]['ph']
                        ph_dst.write(ph_data)
                        ph_dst.flush()
                    ph_fnum += 1

                if do_movie:
                    movie_data = movie_src.read(movie_frame_size)
                    # movie_data += np.random.poisson(lam=100, size=dp_cfg[movie_type]['shape'])
                    for module_id in sim_cfg['sim_module_ids']:
                        movie_dst = active_pff_files[module_id]['movie']
                        movie_dst.write(movie_data)
                        movie_dst.flush()
                    movie_fnum += 1

                # logger.debug( f"Creating new simulated data files: {movie_dest_file=}, {ph_dest_file=}, {seqno=}, {fnum=}" )
                # simulation rate limiting
                time.sleep(update_interval)
                if 'early_exit' in sim_cfg:
                    if sim_cfg['early_exit']['do_exit']:
                        sim_cfg['early_exit']['nframes_before_exit'] -= 1
                        if sim_cfg['early_exit']['nframes_before_exit'] <= 0:
                            raise TimeoutError("test hp_io task unexpected termination")
                if ph_fnum >= ph_nframes:
                    logger.warning(f"simulated ph data acquisition reached EOF: {ph_fnum=} >= {ph_nframes=}")
                    ph_src.seek(0, os.SEEK_SET)
                    ph_fnum = 0
                if movie_fnum >= movie_nframes:
                    logger.warning(f"simulated movie data acquisition reached EOF: {movie_fnum=} >= {movie_nframes=}")
                    movie_src.seek(0, os.SEEK_SET)
                    movie_fnum = 0
    finally:
        sim_valid.clear()
        logger.debug(f"{simulated_data_files=}")
        logger.debug(f"{daq_active_files=}")
        for module_id in active_pff_files:
            if active_pff_files[module_id]['ph'] is not None:
                active_pff_files[module_id]['ph'].close()
            if active_pff_files[module_id]['movie'] is not None:
                active_pff_files[module_id]['movie'].close()
        for daq_active_file in daq_active_files:
            if os.path.exists(daq_active_file):
                os.unlink(daq_active_file)
        for file in simulated_data_files:
            os.unlink(file)
        logger.info("hp_sim thread exited")




"""gRPC server implementing DaqData RPCs"""
class DaqDataServicer(daq_data_pb2_grpc.DaqDataServicer):
    """Provides implementations for DaqData RPCs."""

    def __init__(self, server_cfg):
        # verify the server is running on a POSIX-compliant system
        test_result, msg = is_os_posix()
        assert test_result, msg

        # Initialize mesa monitor for synchronizing access to the hp_io task
        #   "Writers" = tasks changing server state
        #   "Readers" = all other tasks
        self._rw_lock_state = {
            "wr": 0,  # waiting readers
            "ww": 0,  # waiting writers
            "ar": 0,  # active readers
            "aw": 0,  # active writers
        }
        self._hp_io_lock = asyncio.Lock() # threading.RLock()
        self._read_ok_condvar = asyncio.Condition(self._hp_io_lock) # threading.Condition(self._hp_io_lock)
        self._write_ok_condvar = asyncio.Condition(self._hp_io_lock) #threading.Condition(self._hp_io_lock)
        self._active_clients = dict()  # dict of uid : {"client_ip":context.peer()} for debugging

        self._server_cfg = server_cfg
        self.active_data_products = set()

        # Create the server's logger
        self.logger = make_rich_logger(__name__, level=logging.INFO)

        # Load default hahspipe_io configuration
        with open(CFG_DIR/self._server_cfg["default_hp_io_config_file"], "r") as f:
            self._hp_io_cfg = json.load(f)

        # State for single producer, multiple consumer hp_io access
        # A single IO task manages the dataflow between multiple concurrent RPC tasks and the hp_io task:
        #   [single RPC writer -> hp_io task] send messages to the hp_io task
        #   [hp_io task -> many RPC readers] broadcast image data to active read_queues
        self._hp_io_task: asyncio.Task = None
        self._daq_sim_thread: Thread = None

        # Initialize an array of reader_state dicts to support up to max_client concurrent reader RPCs
        self._reader_states: List[Dict[str, Any]] = []
        # _reader_states is a list of reader gRPC state dictionaries
        #   - "is_allocated": True iff corresponding queue is allocated to a reader
        #   - "queue": Queue implementing single producer (hp_io), multiple independent consumer model
        #   - "config": Keyword configuration options
        #   - "last_update_t": strictly monotonic timestamp for rate limiting
        #   - "enqueue_timeouts": number of consecutive timeouts from queue.put()
        #   - "timeouts": number of consecutive timeouts from queue.get()
        #   - "client_ip": info about an active client
        for _ in range(server_cfg['max_concurrent_rpcs']):
            default_config = {
                "stream_movie_data": True,
                "stream_pulse_height_data": True,
                "stream_hashpipe_status": False,
                "update_interval_seconds": 1,
                "module_ids": [],
            }
            default_reader_state = {
                "is_allocated": False,
                "queue": asyncio.Queue(maxsize=server_cfg['max_read_queue_size']),
                "config": default_config,
                "last_update_t": time.monotonic(),
                'client_ip': None,
                'enqueue_timeouts': 0,
                'dequeue_timeouts': 0,
            }
            self._reader_states.append(default_reader_state)
        self._stop_io = Event()  # Signals hp_io task to exit
        self._hp_io_valid = asyncio.Event()  # Set only if the hp_io task is active and collecting data
        self._shutdown_event = asyncio.Event()  # Set only at shutdown
        self._cancel_readers_event = asyncio.Event()  # Causes all waiting and active reader RPCs to abort
        self._daq_sim_thread_valid = Event()  # wait for daq simulation thread to be valid

        # Start the hp_io task if server_cfg points to a valid hp_io_cfg
        if self._server_cfg["init_from_default"]:
            self.logger.info(f"Creating the initial hp_io task from config: "
                             f"{self._server_cfg['init_from_default']=}")
            self._server_cfg['hp_io_init'] = True
            self._start_hp_io_task(self._hp_io_cfg)
        else:
            self.logger.info(f"An InitHpIo call is required to start the hp_io task: "
                                f"{self._server_cfg['init_from_default']=}")
            self._server_cfg['hp_io_init'] = False

    async def _cancel_all_readers(self):
        """Cancel all active and waiting reader RPCs."""
        self._cancel_readers_event.set()
        # signal any blocking readers to wake up and exit
        self._read_ok_condvar.notify_all()
        for rs in [rs for rs in self._reader_states if rs['is_allocated']]:
            try:
                rs['queue'].put_nowait("shutdown")
            except asyncio.QueueFull:
                pass

    async def shutdown(self):
        self._shutdown_event.set()
        self._stop_io.set() # signal hp_io task to exit gracefully
        shutdown_record = dict()
        async with self._hp_io_lock:
            self._server_cfg['hp_io_init'] = False
            await self._cancel_all_readers()
            # wait for the hp_io task to exit
            shutdown_record['stop_hp_io'] = await self._stop_hp_io_task()

        async def wait_for_all_exit():
            while self._rw_lock_state['ar'] + self._rw_lock_state['aw'] > 0:
                await asyncio.sleep(0.1)

        await asyncio.create_task(wait_for_all_exit())

        # check if state was updated properly
        lock_status_ok = True
        for task_state, num_tasks in self._rw_lock_state.items():
            if num_tasks != 0:
                self.logger.critical(f"[rw lock] unexpected tasks in state {task_state} at termination!\n"
                                     f"{self._rw_lock_state=}")
                lock_status_ok = False
        shutdown_record['lock_status_ok'] = lock_status_ok
        if all(shutdown_record.values()):
            self.logger.info("Successfully released all resources")
        else:
            self.logger.critical(f"Some server resources were not released: {shutdown_record=}")
        # for handler in self.logger.handlers:
        #     handler.flush()
        #     self.logger.removeHandler(handler)

    async def _start_hp_io_task(self, hp_io_cfg):
        """Creates a new hp_io task with the given hp_io_cfg.
        Requires: _hp_io_lock acquired in [writer] mode
        @return: True iff the hp_io task was created and attached to a valid active observing run.
        """
        hp_io_update_interval = max(
            hp_io_cfg['update_interval_seconds'],
            self._server_cfg['min_hp_io_update_interval_seconds']
        )
        simulate_daq_cfg = self._server_cfg['simulate_daq_cfg']

        # Terminate any currently alive hp_io task
        if not await self._stop_hp_io_task():
            emsg = f"Failed to stop hp_io task."
            self.logger.critical(emsg)
            raise grpc.RpcError(grpc.StatusCode.INTERNAL, emsg)
        self._stop_io.clear()
        self._server_cfg['hp_io_init'] = False

        # Toggle simulation task creation
        data_dir = hp_io_cfg['data_dir']
        if not hp_io_cfg['simulate_daq']:
            if not os.path.exists(data_dir):
                return False
            self._daq_sim_thread = None
        else:
            data_dir = simulate_daq_cfg['files']['data_dir']
            abs_data_dir = os.path.abspath(data_dir)
            self.logger.info(f"Starting simulated DAQ flow to {abs_data_dir=}")
            self._daq_sim_thread = Thread(
                target=daq_sim_thread_fn,
                args=(
                    simulate_daq_cfg.copy(),
                    hp_io_update_interval / 2,
                    self._stop_io,
                    self._daq_sim_thread_valid,
                    self.logger
                )
            )
            self._daq_sim_thread.start()
            await asyncio.to_thread(self._daq_sim_thread_valid.wait)

        # Create a new hp_io_task using the client's configuration
        active_data_products_queue = asyncio.Queue()
        self._hp_io_task = asyncio.create_task(
            HpIoManager(
                Path(data_dir),
                self._server_cfg['valid_data_products'],
                hp_io_update_interval,
                hp_io_cfg['module_ids'],
                hp_io_cfg['simulate_daq'],
                self._reader_states,
                self._stop_io,
                self._hp_io_valid,
                self._server_cfg['max_reader_enqueue_timeouts'],
                active_data_products_queue,
                self.logger,
                simulate_daq_cfg
            ).run()
        )
        try:
            init_tasks = [self._hp_io_valid.wait(), active_data_products_queue.get()]
            _, self.active_data_products  = await asyncio.wait_for(asyncio.gather(*init_tasks), timeout=10)
        except asyncio.TimeoutError:
            logging.error(f"Timeout waiting for hp_io task: {self._hp_io_task=}")

        if not self._is_hp_io_valid() or not self.active_data_products:
            await self._stop_hp_io_task()
            self._hp_io_task = None
            self._daq_sim_thread = None
            return False
        self.logger.info(f"hp_io task alive and valid with active_data_products={self.active_data_products}")
        self._server_cfg['hp_io_init'] = True
        return True

    def _is_hp_io_valid(self, verbose=True):
        if self._hp_io_task is not None and not self._hp_io_task.done() and self._hp_io_valid.is_set():
            return True
        elif self._hp_io_task is None:
            if verbose: self.logger.warning("hp_io task is uninitialized")
        elif self._hp_io_task.done():
            if verbose: self.logger.warning("hp_io task is not alive")
        elif not self._hp_io_valid.is_set():
            if verbose: self.logger.warning("hp_io task is alive but not valid")
        else:
            emsg = (f"unhandled is_hp_io_valid case: "
                    f"{self._hp_io_task=}, "
                    f"{self._hp_io_task.done()=},"
                    f"{self._hp_io_valid=}")
            if verbose: self.logger.critical(emsg)
            raise RuntimeError(emsg)  # SHOULD NEVER REACH HERE
        return False

    async def _stop_hp_io_task(self):
        """Stops the hp_io task. Idempotent behavior.
        Requires: _hp_io_lock acquired in [writer] mode
        @return: True iff the hp_io task is not alive.
        :param timeout: seconds to wait for hp_io task to exit gracefully"""
        self._stop_io.set()  # signal hp_io task to exit gracefully
        self._server_cfg['hp_io_init'] = False
        if self._hp_io_task is not None and not self._hp_io_task.done():
            try:
                await self._hp_io_task
                if self._daq_sim_thread is not None and self._daq_sim_thread.is_alive():
                    await asyncio.to_thread(self._daq_sim_thread.join)
            except RuntimeError as rerr:
                self.logger.critical(f"encountered runtime error while stopping hp_io task: {rerr}")
                return False
            finally:
                if self._hp_io_task.done():  # check if join succeeded or timeout happened while waiting
                    self.logger.info(f"Successfully terminated hp_io task")
                    return True
        else:
            self.logger.debug("no hp_io task to stop_io (doing nothing)")
            return True

    @asynccontextmanager
    async def _rw_lock_writer(self, context, force=False):
        uid: uuid.UUID = None
        active = False
        try:
            async with self._hp_io_lock:
                # BEGIN check-in critical section
                # All reader RPCs are long-lived server streaming operations.
                # The server's synchronization logic will prevent updates to _server_cfg while any reader RPCs are active,
                # so we should cancel any writer RPCs immediately

                if (not force) and self._rw_lock_state['ar'] > 0:
                    active_clients = str([c["client_ip"] for c in self._active_clients.values()])
                    emsg = (f"Cannot modify server state because there are {self._rw_lock_state['ar']} active "
                            f"streaming clients. Set force=True or stop the following active clients and try again: {active_clients}.")
                    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)
                elif force and self._rw_lock_state['ar'] > 0:
                    self.logger.warning(f"Forcing server state modification despite active reader RPCs. ")
                    await self._cancel_all_readers()


                self.logger.debug(f"(writer) check-in (start):\t{self._rw_lock_state=}")
                # Wait until no active readers or active writers
                while (not self._shutdown_event.is_set() and
                       not context.cancelled() and
                       (self._rw_lock_state['aw'] + self._rw_lock_state['ar']) > 0):
                    self._rw_lock_state['ww'] += 1
                    await self._write_ok_condvar.wait()
                    self._rw_lock_state['ww'] -= 1

                # check if the server is still active
                if self._shutdown_event.is_set():
                    emsg = "server shutdown initiated during writer lock acquisition [skipping to check-out]"
                    self.logger.error(emsg)
                    await context.abort(grpc.StatusCode.CANCELLED, emsg)

                # check if the client is still active
                if context.cancelled():
                    emsg = "client cancelled rpc during writer lock acquisition (skipping to check-out)"
                    self.logger.warning(emsg)
                    await context.abort(grpc.StatusCode.CANCELLED, emsg)

                # check if the hp_io task is valid
                if self._server_cfg['hp_io_init'] and not self._is_hp_io_valid():
                    emsg = (f"The hp_io task data stream is invalid. "
                            f" (skipping to check-out)")
                    self.logger.warning(emsg)
                    self._server_cfg['hp_io_init'] = False
                    await context.abort(grpc.StatusCode.INTERNAL, emsg)

                # activate the writer
                self._rw_lock_state['aw'] += 1
                active = True
                uid = uuid.uuid4()
                self._active_clients[uid] = {
                    "client_ip": urllib.parse.unquote(context.peer()),
                    "type": "writer",
                }
                self.logger.debug(f"(writer) check-in (end):\t\t{self._rw_lock_state=}")
                # END check-in critical section
            yield None
        except RuntimeError as err:
            pass
        finally:
            async with self._hp_io_lock:
                # BEGIN check-out critical section
                self.logger.debug(f"(writer) check-out (start):\t{self._rw_lock_state=}")
                if not self._is_hp_io_valid(verbose=False):
                    self._server_cfg['hp_io_init'] = False
                if active:  # handle edge cases where task is interrupted or has an error during lock acquire
                    self._rw_lock_state['aw'] = self._rw_lock_state['aw'] - 1  # no longer active
                    del self._active_clients[uid]
                # allow new readers to start waiting
                self._cancel_readers_event.clear()
                # Wake up waiting readers or a waiting writer (prioritize waiting writers).
                if self._rw_lock_state['ww'] > 0:  # Give lock priority to waiting writers
                    self._write_ok_condvar.notify()
                elif self._rw_lock_state['wr'] > 0:
                    self._read_ok_condvar.notify_all()
                self.logger.debug(f"(writer) check-out (end):\t{self._rw_lock_state=}")
                # END check-out critical section

    @asynccontextmanager
    async def _rw_lock_reader(self, context):
        reader_idx = -1  # remember which reader_states dict corresponds to this task
        uid: uuid.UUID = None
        active = False
        try:
            async with self._hp_io_lock:
                # BEGIN check-in critical section
                if (self._rw_lock_state['ar'] + self._rw_lock_state['wr']) >= self._server_cfg['max_concurrent_rpcs']:
                    emsg = (f"Cannot start a new reader RPC because the maximum number of active reader RPCs "
                            f"({self._server_cfg['max_concurrent_rpcs']}) has been reached. To change the max number of "
                            f" reader RPCs, increase the server configuration parameter for 'max_concurrent_rpcs'.")
                    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)
                self.logger.debug(f"(reader) check-in (start):\t{self._rw_lock_state=}"
                                  f"\n{[rs['is_allocated'] for rs in self._reader_states]=}")
                # Wait until no active writers or waiting writers
                while (not self._shutdown_event.is_set() and
                       not self._cancel_readers_event.is_set() and
                       not context.cancelled() and
                       (self._rw_lock_state['aw'] + self._rw_lock_state['ww']) > 0):
                    self._rw_lock_state['wr'] += 1
                    await self._read_ok_condvar.wait()
                    self._rw_lock_state['wr'] -= 1

                # check if the server is still active
                if self._shutdown_event.is_set():
                    emsg = "server shutdown initiated during reader lock acquisition [skipping to check-out]"
                    self.logger.error(emsg)
                    await context.abort(grpc.StatusCode.CANCELLED, emsg)

                # check if reader RPCs are cancelled
                elif self._cancel_readers_event.is_set():
                    emsg = ("cancel_all_readers called during reader lock acquisition. "
                            "another client is likely configuring the server right now. "
                            "try again soon [skipping to check-out]")
                    self.logger.warning(emsg)
                    await context.abort(grpc.StatusCode.CANCELLED, emsg)

                # check if the client is still active
                elif context.cancelled():
                    emsg = "client context terminated during reader lock acquisition [skipping to check-out]"
                    self.logger.error(emsg)
                    await context.cancel()

                # check if the hp_io task is valid
                elif self._server_cfg['hp_io_init'] and not self._is_hp_io_valid():
                    emsg = (f"The hp_io task data stream is invalid. "
                            f" (skipping to check-out)")
                    self.logger.warning(emsg)
                    self._server_cfg['hp_io_init'] = False
                    await context.abort(grpc.StatusCode.INTERNAL, emsg)

                # allocate reader resources
                for idx, rs in enumerate(self._reader_states):
                    if not rs['is_allocated']:
                        reader_idx = idx
                        self._reader_states[idx]['is_allocated'] = True
                        break

                # check if the allocation succeeded
                if reader_idx < 0:
                    emsg = "reader_states allocation failed during reader check-in! [SHOULD NEVER HAPPEN]"
                    self.logger.critical(emsg)
                    await context.abort(grpc.StatusCode.INTERNAL, emsg)

                # activate the reader
                self._rw_lock_state['ar'] += 1
                active = True
                uid = uuid.uuid4()
                client_ip = urllib.parse.unquote(context.peer())
                self._active_clients[uid] = {
                    "client_ip": client_ip,
                    "type": "reader",
                }
                self._reader_states[reader_idx]['client_ip'] = client_ip
                self.logger.debug(f"(reader) check-in (end):\t\t{self._rw_lock_state=}, fmap_idx={reader_idx}"
                                  f"\n{[rs['is_allocated'] for rs in self._reader_states]=}")
                # END check-in critical section
            yield self._reader_states[reader_idx]
        finally:
            async with self._hp_io_lock:
                # BEGIN check-out critical section
                self.logger.debug(f"(reader) check-out (start):\t{self._rw_lock_state=}")
                if not self._is_hp_io_valid(verbose=False):
                    self._server_cfg['hp_io_init'] = False
                if active:
                    self._rw_lock_state['ar'] = self._rw_lock_state['ar'] - 1  # no longer active
                    del self._active_clients[uid]
                    self._reader_states[reader_idx]['is_allocated'] = False # release reader resources
                # Wake up waiting readers or a waiting writer (prioritize waiting writers).
                if self._rw_lock_state['ar'] == 0 and self._rw_lock_state['ww'] > 0:
                    self._write_ok_condvar.notify()
                elif self._rw_lock_state['wr'] > 0:
                    self._read_ok_condvar.notify_all()
                self.logger.debug(f"(reader) check-out (end):\t\t{self._rw_lock_state=}")
                # END check-out critical section

    async def StreamImages(self, request: StreamImagesRequest, context) -> AsyncIterator[StreamImagesResponse]:
        """Forward sample panoseti movie and pulse-height images to the client. [reader]"""
        self.logger.info(f"new StreamImages rpc from {urllib.parse.unquote(context.peer())}: "
                         f"{MessageToDict(request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)}")
        # Validate request fields that don't require reading server state
        # check movie and pulse-height option params
        if not request.stream_movie_data and not request.stream_pulse_height_data:
            emsg = "At least one of the stream flags must be set to True"
            self.logger.warning(f"Rejecting request: '{emsg}'")
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)
        async with self._rw_lock_reader(context) as reader_state:  # reader_state = allocated reader resources
            # BEGIN reader critical section
            # Validate request fields that require reading protected server state
            if not self._server_cfg['hp_io_init']:
                emsg = "Uninitialized hp_io task. Run InitHpIo with a valid hp_io configuration to initialize it."
                self.logger.warning(f"Rejecting request: '{emsg}'")
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)

            # check if hp_io is currently streaming data of the requested type
            if request.stream_movie_data and not {'img8', 'img16'}.intersection(self.active_data_products):
                emsg = ("hp_io task is not streaming movie data. Set stream_movie_data=False to avoid this error or "
                        "restart the hp_io task to enable streaming movie data.")
                self.logger.warning(f"'{emsg}'")
                # await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)
            if request.stream_pulse_height_data and not {'ph256', 'ph1024'}.intersection(self.active_data_products):
                emsg = ("hp_io task is not streaming pulse-height data. Set stream_pulse_height_data=False to avoid this error or "
                        "restart the hp_io task to enable streaming pulse-height data.")
                self.logger.warning(f"'{emsg}'")
                # await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)

            # Set stream filter options
            if request.update_interval_seconds > self._server_cfg['max_client_update_interval_seconds']:
                emsg = (f"update_interval_seconds must be at most "
                        f"{self._server_cfg['max_client_update_interval_seconds']}"
                        f"seconds. Got {request.update_interval_seconds}")
                self.logger.warning(emsg)
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)
            elif request.update_interval_seconds < self._hp_io_cfg['update_interval_seconds']:
                reader_state['config']['update_interval_seconds'] = self._hp_io_cfg['update_interval_seconds']
            else:
                reader_state['config']['update_interval_seconds'] = request.update_interval_seconds
            reader_state['config']['stream_movie_data'] = request.stream_movie_data
            reader_state['config']['stream_pulse_height_data'] = request.stream_pulse_height_data
            reader_state['config']['module_ids'] = request.module_ids
            reader_state['enqueue_timeouts'] = 0
            reader_state['dequeue_timeouts'] = 0
            self.logger.debug(f"{reader_state=}")

            # Clear old data from the read_queue
            reader_queue: asyncio.Queue = reader_state['queue']
            while not reader_queue.empty():
                await reader_queue.get()

            def continue_streaming():
                res = not context.cancelled()
                res &= self._is_hp_io_valid()
                res &= not self._shutdown_event.is_set()
                res &= not self._cancel_readers_event.is_set()
                res &= reader_state['dequeue_timeouts'] < self._server_cfg['max_reader_dequeue_timeouts']
                res &= reader_state['enqueue_timeouts'] < self._server_cfg['max_reader_enqueue_timeouts']
                return res

            # Valid server state -> start streaming!
            while continue_streaming():
                # await the next PanoImage to broadcast from the hp_io task
                try:
                    pano_image = await asyncio.wait_for(reader_queue.get(), timeout=self._server_cfg['reader_timeout'])
                    reader_state['dequeue_timeouts'] = 0
                except asyncio.TimeoutError:
                    reader_state['dequeue_timeouts'] += 1
                    continue
                if not isinstance(pano_image, PanoImage):
                    break
                send_timestamp = timestamp_pb2.Timestamp()
                send_timestamp.GetCurrentTime()

                stream_images_response = StreamImagesResponse(
                    name=f"StreamImageResponse [Data]",
                    timestamp=send_timestamp,
                    pano_image=pano_image
                )
                yield stream_images_response

            # log reason why streaming stopped
            if self._shutdown_event.is_set():
                emsg = "server shutdown initiated"
                await context.abort(grpc.StatusCode.CANCELLED, emsg)
            elif self._cancel_readers_event.is_set():
                emsg = "cancel_all_readers: another client has likely forced a write to server state"
                self.logger.warning(emsg)
                await context.abort(grpc.StatusCode.CANCELLED, emsg)
            elif context.cancelled():
                emsg = "client context terminated"
                self.logger.info(emsg)
                await context.abort(grpc.StatusCode.CANCELLED, emsg)
            elif reader_state['dequeue_timeouts'] >= self._server_cfg['max_reader_dequeue_timeouts']:
                emsg = (f"Reader reached max number of consecutive read dequeue timeouts: "
                        f"({self._server_cfg['max_reader_dequeue_timeouts']}).")
                self.logger.warning(emsg)
                await context.abort(grpc.StatusCode.CANCELLED, emsg)
            elif reader_state['enqueue_timeouts'] >= self._server_cfg['max_reader_enqueue_timeouts']:
                emsg = (f"Reader reached max number of consecutive read enqueue timeouts: "
                        f"({self._server_cfg['max_reader_enqueue_timeouts']}). "
                        f"The reader process is likely too slow for the requested update interval "
                        f"{reader_state['config']['update_interval_seconds']}.")
                self.logger.warning(emsg)
                await context.abort(grpc.StatusCode.CANCELLED, emsg)
            elif not self._stop_io.is_set():
                emsg = (f"The hp_io task data stream became invalid! "
                        f"This is expected when hashpipe stops running. "
                        f"Otherwise, check the server logs to debug this issue")
                self.logger.warning(emsg)
                await context.abort(grpc.StatusCode.INTERNAL, emsg)
            else:
                emsg = "Unexpected termination case"
                self.logger.critical(emsg)
                await context.abort(grpc.StatusCode.INTERNAL, emsg)
            # END reader critical section

    async def InitHpIo(self, request: InitHpIoRequest, context) -> InitHpIoResponse:
        """Initialize the hp_io task with the given configuration. [writer]"""
        self.logger.info(f"new InitHpIo rpc from {urllib.parse.unquote(context.peer())}: "
                         f"{MessageToDict(request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)}")
        # Validate request fields that don't require reading server state:
        # if the daq target is a live observing run, check if the specified data_dir exists
        if (not request.simulate_daq) and (not os.path.exists(request.data_dir)):
            emsg = f"data_dir={request.data_dir} does not exist"
            self.logger.warning(f"Rejecting request: '{emsg}'")
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)

        # check if daq is active and real daq is being used. Note: simulated daq data flow always properly initialized
        elif (not request.simulate_daq) and (not is_daq_active(simulate_daq=False)):
            emsg = 'DAQ software is not active. Re-try hp_io task creation once the daq software has been started.'
            self.logger.warning(f"Rejecting request: '{emsg}'")
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)

        # read server configuration to validate parameters in init request
        async with self._rw_lock_reader(context) as reader_state:
            # check if given data products are valid
            # check if the requested update interval is not too short
            if request.update_interval_seconds < self._server_cfg['min_hp_io_update_interval_seconds']:
                emsg = (f"update_interval_seconds must be at least "
                        f"{self._server_cfg['min_hp_io_update_interval_seconds']} seconds. Got {request.update_interval_seconds}")
                self.logger.warning(f"Rejecting request: '{emsg}'")
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)
            self.logger.info(f"Request passed validation checks")

        # attempt to change server state: modify hp_io task
        async with self._rw_lock_writer(context, force=request.force):
            self._server_cfg['hp_io_init'] = False
            last_hp_io_valid = self._is_hp_io_valid()
            stop_success = await self._stop_hp_io_task()
            if not stop_success:
                emsg = "failed to stop hp_io task!"
                self.logger.critical(emsg)
                await context.abort(grpc.StatusCode.INTERNAL, emsg)
            self.logger.info("stopped existing hp_io task")
            hp_io_cfg = {
                "data_dir": request.data_dir,
                "simulate_daq": request.simulate_daq,
                "update_interval_seconds": request.update_interval_seconds,
                "module_ids": request.module_ids,
            }
            start_success = await self._start_hp_io_task(hp_io_cfg)
            if start_success:
                # commit client changes
                self.logger.info("InitHpIo transaction succeeded: new hp_io task initialized")
                self._hp_io_cfg = hp_io_cfg
                self._server_cfg['hp_io_init'] = True
            else:
                # attempt to restart previously valid hp_io task
                emsg = "failed to start hp_io task. "
                if last_hp_io_valid:
                    emsg += "Restarting hp_io with the previous configuration"
                    self._server_cfg['hp_io_init'] = await self._start_hp_io_task(self._hp_io_cfg)
                else:
                    emsg += "No previously valid hp_io task to restart."
                self.logger.warning(emsg)

            return InitHpIoResponse(success=start_success)


    async def Ping(self, request, context):
        """Returns the Empty message to verify client-server connection."""
        self.logger.info(f"Ping rpc from {urllib.parse.unquote(context.peer())}")
        return Empty()

async def serve(server_cfg):
    """Create the gRPC server and start providing the UbloxControl service."""
    server = grpc.aio.server(
        maximum_concurrent_rpcs=server_cfg['max_concurrent_rpcs'],
    )
    daq_data_servicer = DaqDataServicer(server_cfg)
    daq_data_pb2_grpc.add_DaqDataServicer_to_server(
        daq_data_servicer, server
    )

    # Add RPC reflection to show available commands to users
    SERVICE_NAMES = (
        daq_data_pb2.DESCRIPTOR.services_by_name["DaqData"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    # Start gRPC and configure to listen on port 50051
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    print(f"The gRPC services {SERVICE_NAMES} are running.\nEnter CTRL+C to stop_io them.")
    try:
        await server.start()
        await server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        grace = server_cfg["shutdown_grace_period"]
        print(f"'^C' received, shutting down the server with {grace=} seconds.")
        await daq_data_servicer.shutdown()
        await server.stop(grace=grace)


if __name__ == "__main__":

    # Load server configuration
    default_hp_io_config_file = 'default_hp_io_config.json'

    # Configuration
    server_cfg_file = "daq_data_server_config.json"
    with open(CFG_DIR / server_cfg_file, "r") as f:
        server_cfg = json.load(f)

    try:
        asyncio.run(serve(server_cfg))
    except KeyboardInterrupt as e:
        pass
    finally:
        time.sleep(0.1)
        print("Exiting the server.")
