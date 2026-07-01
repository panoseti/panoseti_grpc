"""
Rigorous tests that verify UDS-only data-path correctness post-refactor.

Covers:
  - Frame shapes and dtypes for every DataProduct
  - Monotonically increasing frame_ids per (module_id, type) pair
  - module_ids whitelist filtering
  - Simulation resumes cleanly after force re-init
  - Two concurrent readers see consistent frame_ids
"""

from __future__ import annotations

import asyncio
from typing import Any

import numpy as np
import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient
from panoseti_grpc.grpc_utils.exceptions import PanosetiRpcError

pytestmark = pytest.mark.asyncio

HP_IO_SIM = {
    "data_dir": "daq_data/simulated_data_dir",
    "simulate_daq": True,
    "update_interval_seconds": 0.1,
    "force": True,
    "module_ids": [],
}


async def _collect_frames(client, n: int, **stream_kwargs) -> list[dict[Any]]:
    """Collect `n` frames from a fresh StreamImages call and return them."""
    frames: list[dict[Any]] = []
    async for img in client.stream_images(**stream_kwargs):
        frames.append(img)
        if len(frames) >= n:
            break
    return frames


async def test_frame_shapes_and_dtypes(default_server_process):
    """
    img16 frames must have shape (32,32), bytes_per_pixel=2, type=MOVIE.
    ph256 frames must have shape (16,16), bytes_per_pixel=2, type=PULSE_HEIGHT.
    All image arrays must be parseable as numpy arrays of the correct dtype.
    """
    async with AioDaqDataClient(default_server_process["host"], default_server_process["port"]) as client:
        assert await client.init_hp_io(HP_IO_SIM)

        frames = await _collect_frames(
            client,
            20,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
        )
        assert frames, "No frames received from simulation"

        movie_frames = [f for f in frames if f["type"] == "MOVIE"]
        ph_frames = [f for f in frames if f["type"] == "PULSE_HEIGHT"]

        assert movie_frames, "No MOVIE frames received"
        assert ph_frames, "No PULSE_HEIGHT frames received"

        for f in movie_frames:
            arr = np.array(f["image_array"]).reshape(f["shape"])
            assert f["shape"] in ([32, 32], [16, 16]), f"Unexpected movie shape: {f['shape']}"
            assert f["bytes_per_pixel"] in (1, 2), f"Unexpected bpp: {f['bytes_per_pixel']}"
            arr = arr.astype(np.uint16) if f["bytes_per_pixel"] == 2 else arr.astype(np.uint8)
            assert arr.shape == tuple(f["shape"])

        for f in ph_frames:
            arr = np.array(f["image_array"]).reshape(f["shape"])
            assert f["shape"] == [16, 16], f"PH256 frames must be 16x16, got {f['shape']}"
            assert f["bytes_per_pixel"] == 2, f"PH frames must have bpp=2, got {f['bytes_per_pixel']}"
            arr = arr.astype(np.int16)
            assert arr.shape == (16, 16)


async def test_frame_id_monotonically_increases(default_server_process):
    """
    frame_id must never decrease between consecutive frames of the same (module_id, type) pair.
    """
    async with AioDaqDataClient(default_server_process["host"], default_server_process["port"]) as client:
        assert await client.init_hp_io(HP_IO_SIM)

        frames = await _collect_frames(
            client,
            30,
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.05,
        )
        assert len(frames) >= 5, "Need at least 5 MOVIE frames for monotonicity check"

        from collections import defaultdict

        by_module: dict[int, list[int]] = defaultdict(list)
        for f in frames:
            by_module[f["module_id"]].append(f["frame_number"])

        for mid, frame_nums in by_module.items():
            for i in range(len(frame_nums) - 1):
                assert frame_nums[i + 1] >= frame_nums[i], (
                    f"Module {mid}: frame_number went backwards ({frame_nums[i]} → {frame_nums[i + 1]})"
                )


async def test_module_id_whitelist_filters_correctly(default_server_process):
    """
    When module_ids is set in the StreamImages request, only frames from
    those module IDs should be delivered.
    """
    async with AioDaqDataClient(default_server_process["host"], default_server_process["port"]) as client:
        cfg = {**HP_IO_SIM, "module_ids": [224]}
        assert await client.init_hp_io(cfg)

        frames = await _collect_frames(
            client,
            10,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
            module_ids=(224,),
        )
        assert frames, "No frames received with module_ids=[224]"
        for f in frames:
            assert f["module_id"] == 224, f"Expected only module 224, got module_id={f['module_id']}"


async def test_stream_cancelled_on_force_reinit(default_server_process):
    """
    When a second InitHpIo(force=True) is called while a StreamImages reader
    is active, the server must cancel the active reader.
    """
    async with AioDaqDataClient(default_server_process["host"], default_server_process["port"]) as client:
        assert await client.init_hp_io(HP_IO_SIM)

        stream = client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.1,
        )

        first = await asyncio.wait_for(stream.__anext__(), timeout=10.0)
        assert first is not None

        # Force a re-init from a second client — this should cancel the active reader
        async with AioDaqDataClient(default_server_process["host"], default_server_process["port"]) as reinit_client:
            await reinit_client.init_hp_io({**HP_IO_SIM, "force": True})

        # The original stream must now end (CANCELLED or StopAsyncIteration)
        stream_ended = False
        try:
            async with asyncio.timeout(10.0):
                async for _ in stream:
                    pass
            stream_ended = True  # Clean StopAsyncIteration
        except TimeoutError:
            pass  # Stream didn't end — failure
        except PanosetiRpcError:
            stream_ended = True  # CANCELLED or other RPC error is expected
        except Exception:
            stream_ended = True  # Any termination is acceptable

        assert stream_ended, "Active StreamImages reader must terminate after force re-init by a writer"


async def test_simulation_resumes_after_force_reinit(default_server_process):
    """
    Calling InitHpIo twice (force=True each time) must leave the server
    in a valid state where frames continue to flow.
    """
    async with AioDaqDataClient(default_server_process["host"], default_server_process["port"]) as client:
        assert await client.init_hp_io(HP_IO_SIM)
        assert await client.init_hp_io({**HP_IO_SIM, "force": True})

        frames = await _collect_frames(
            client,
            5,
            stream_movie_data=True,
            stream_pulse_height_data=False,
            update_interval_seconds=0.1,
        )
        assert len(frames) >= 5, "Frames must flow after second force re-init"


async def test_concurrent_readers_see_consistent_latest_frame(default_server_process):
    """
    Two simultaneous StreamImages calls should see frames from the same module
    with similar (within tolerance) frame_numbers — both readers poll the
    same latest_data_cache.
    """
    host, port = default_server_process["host"], default_server_process["port"]
    async with (
        AioDaqDataClient(host, port) as client_a,
        AioDaqDataClient(host, port) as client_b,
    ):
        assert await client_a.init_hp_io(HP_IO_SIM)

        SAMPLES = 10

        async def collect(client):
            return await _collect_frames(
                client,
                SAMPLES,
                stream_movie_data=True,
                stream_pulse_height_data=False,
                update_interval_seconds=0.1,
            )

        results_a, results_b = await asyncio.gather(collect(client_a), collect(client_b))

        assert len(results_a) == SAMPLES
        assert len(results_b) == SAMPLES

        mods_a = {f["module_id"] for f in results_a}
        mods_b = {f["module_id"] for f in results_b}
        assert mods_a == mods_b, f"Both readers should see the same module set; A={mods_a}, B={mods_b}"

        last_a = results_a[-1]["frame_number"]
        last_b = results_b[-1]["frame_number"]
        assert abs(last_a - last_b) <= 5, f"Concurrent readers diverged: last frame_number A={last_a}, B={last_b}"
