"""
DAQ Data v2 Simulator.
Reads PFF frames from disk and pushes them into Hashpipe UDS sockets.
Acts as the Hashpipe role for testing.
"""

import asyncio
import logging
import signal
from pathlib import Path
from importlib import resources

from panoseti_grpc.panoseti_util import pff
from panoseti_grpc.telemetry.logger import get_logger

# Shared anchored package
ANCHOR_PACKAGE = "panoseti_grpc"

class Simulator:
    def __init__(
        self,
        socket_path_template: str,
        sim_configs: list[dict],
        logger: logging.Logger,
    ):
        self.socket_path_template = socket_path_template
        self.sim_configs = sim_configs
        self.logger = logger
        self.stop_event = asyncio.Event()

    async def _simulate_dp(self, dp_name: str, pff_path: str, module_id: int, bpp: int, shape: tuple[int, int]):
        """Reads PFF and pushes to UDS in a loop."""
        socket_path = self.socket_path_template.format(dp_name=dp_name)
        bytes_per_image = shape[0] * shape[1] * bpp
        
        # Load frames into memory
        frames = []
        try:
            # Resolve package path
            with resources.files(ANCHOR_PACKAGE).joinpath(pff_path).open("rb") as f:
                frame_size, nframes, _, _ = pff.img_info(f, bytes_per_image)
                f.seek(0)
                # Read all frames
                for _ in range(nframes):
                    frame = f.read(frame_size)
                    if not frame: break
                    frames.append(frame)
            self.logger.info(f"Loaded {len(frames)} frames for {dp_name}")
        except Exception as e:
            self.logger.error(f"Error loading PFF {pff_path}: {e}")
            return

        while not self.stop_event.is_set():
            try:
                self.logger.info(f"Simulator connecting to {socket_path}")
                _, writer = await asyncio.open_unix_connection(socket_path)
                self.logger.info(f"Simulator connected to {socket_path}")
                
                fnum = 0
                while not self.stop_event.is_set():
                    frame_data = frames[fnum % len(frames)]
                    
                    # Wire format: [2 bytes module_id][PFF frame]
                    module_id_bytes = module_id.to_bytes(2, "big")
                    writer.write(module_id_bytes)
                    writer.write(frame_data)
                    await writer.drain()
                    
                    fnum += 1
                    await asyncio.sleep(0.1) # 10Hz
                    
            except (ConnectionRefusedError, FileNotFoundError):
                await asyncio.sleep(1.0)
            except Exception as e:
                self.logger.error(f"Simulator error for {dp_name}: {e}")
                await asyncio.sleep(1.0)

    async def run(self):
        tasks = []
        for cfg in self.sim_configs:
            tasks.append(asyncio.create_task(
                self._simulate_dp(cfg["dp_name"], cfg["pff_path"], cfg["module_id"], cfg["bpp"], cfg["shape"])
            ))
        
        await self.stop_event.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket-template", default="/tmp/hashpipe_grpc.dp_{dp_name}.sock")
    args = parser.parse_args()

    logger = get_logger("daq_data_v2.simulator")
    
    # Hardcoded test config for now, can be moved to JSON
    sim_configs = [
        {
            "dp_name": "img16",
            "pff_path": "daq_data/simulated_data_dir/obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd/start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_0.debug_TRUNCATED.pff",
            "module_id": 1,
            "bpp": 2,
            "shape": (32, 32)
        },
        {
            "dp_name": "ph256",
            "pff_path": "daq_data/simulated_data_dir/obs_Lick.start_2024-07-25T04:34:06Z.runtype_sci-data.pffd/start_2024-07-25T04_34_46Z.dp_ph256.bpp_2.module_3.seqno_0.debug_TRUNCATED.pff",
            "module_id": 3,
            "bpp": 2,
            "shape": (16, 16)
        }
    ]

    simulator = Simulator(args.socket_template, sim_configs, logger)

    def stop():
        simulator.stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop)

    await simulator.run()

if __name__ == "__main__":
    asyncio.run(main())
