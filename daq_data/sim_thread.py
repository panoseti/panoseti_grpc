"""Simulate hashpipe data stream: Read a real file and write to a fake file into the following file structure:"""

import os
import time
import logging
from threading import Event
from typing import Dict, Any


from .resources import get_dp_config, get_daq_active_file, get_sim_pff_path, is_daq_active_sync
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
    dp_cfg = get_dp_config(data_products)

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
            (movie_frame_size, movie_nframes, first_t, last_t) = pff.img_info(movie_src, dp_cfg[movie_type].bytes_per_image)
            movie_src.seek(0, os.SEEK_SET)
            logger.info(f"movie src: {movie_frame_size=}, {movie_nframes=}")

            (ph_frame_size, ph_nframes, first_t, last_t) = pff.img_info(ph_src, dp_cfg[ph_type].bytes_per_image)
            logger.info(f"ph src: {ph_frame_size=}, {ph_nframes=}")
            ph_src.seek(0, os.SEEK_SET)

            # copy frames from [dp]_src to dp_dst to simulate data acquisition software
            # fnum = 0
            ph_fnum = movie_fnum = 0
            ph_seqno = movie_seqno = -1
            sim_valid.set()
            last_sleep_t = 0
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
                curr_t = time.monotonic()
                sleep_t = max(update_interval - (curr_t - last_sleep_t), 0)
                time.sleep(sleep_t)
                last_sleep_t = curr_t
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
