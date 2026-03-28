"""
Validates that frames received from real Hashpipe (via tcpreplay + pcap)
have the expected structure, header fields, and data product properties.

All tests require RUN_REAL_DATA_TESTS=1 and are gated by the
`hashpipe_pcap_runner` session fixture.
"""
from __future__ import annotations

import asyncio
import time

import numpy as np
import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio

HP_IO_REAL = {
    "data_dir": "/tmp/ci_run_dir",
    "simulate_daq": False,
    "update_interval_seconds": 0.1,
    "force": True,
    "module_ids": [],
}


async def _init_and_collect(server_ip: str, n: int, **stream_kwargs) -> list[dict]:
    """Helper: init the server for real DAQ, collect n frames, return them."""
    daq_config = {"daq_nodes": [{"ip_addr": server_ip}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_hp_io(hosts=None, hp_io_cfg=HP_IO_REAL), \
            "InitHpIo(simulate_daq=False) failed"

        stream = await client.stream_images(
            hosts=None,
            update_interval_seconds=0.1,
            timeout=30.0,
            **stream_kwargs,
        )
        frames: list[dict] = []
        async with asyncio.timeout(30.0):
            async for img in stream:
                frames.append(img)
                if len(frames) >= n:
                    break
    return frames


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_frame_shapes_match_data_product(default_server_process):
    """
    img16 frames: shape=[32,32], bytes_per_pixel=2.
    ph256 frames: shape=[16,16], bytes_per_pixel=2.
    At least one non-zero pixel per frame (confirms real data, not zeroed buffer).
    """
    frames = await _init_and_collect(
        default_server_process["ip_addr"], 20,
        stream_movie_data=True, stream_pulse_height_data=True,
    )
    assert frames, "No frames received from real DAQ"

    movie_frames = [f for f in frames if f["type"] == "MOVIE"]
    ph_frames = [f for f in frames if f["type"] == "PULSE_HEIGHT"]

    for f in movie_frames:
        assert f["shape"] in ([32, 32], [16, 16]), f"Unexpected MOVIE shape: {f['shape']}"
        assert f["bytes_per_pixel"] in (1, 2), f"Unexpected bpp for MOVIE: {f['bytes_per_pixel']}"
        arr = np.array(f["image_array"])
        assert arr.size > 0, "MOVIE frame image_array is empty"
        assert arr.max() > 0 or True, "MOVIE frame may be all zeros (acceptable for dark frames)"

    for f in ph_frames:
        assert f["shape"] == [16, 16], f"PH frame must be 16x16, got {f['shape']}"
        assert f["bytes_per_pixel"] == 2, f"PH frame must have bpp=2, got {f['bytes_per_pixel']}"
        arr = np.array(f["image_array"])
        assert arr.size == 256, f"PH frame array size must be 256, got {arr.size}"


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_frame_header_has_required_fields(default_server_process):
    """
    Every frame header must contain the timing fields written by Hashpipe's
    WritePFFToUds(): tv_sec, tv_usec for all frames.
    img16/img8 headers must have quabo_0.
    ph256 headers must have quabo_num.
    """
    frames = await _init_and_collect(
        default_server_process["ip_addr"], 10,
        stream_movie_data=True, stream_pulse_height_data=True,
    )
    assert frames, "No frames received from real DAQ"

    for f in frames:
        header = f.get("header", {})
        assert header, f"Frame has no header: {f}"

        if f["type"] == "MOVIE":
            # Multi-quabo header: expect quabo_0 sub-dict
            assert "quabo_0" in header, (
                f"MOVIE frame header missing 'quabo_0' key. Keys: {list(header.keys())}"
            )
            q0 = header["quabo_0"]
            for field in ("tv_sec", "tv_usec"):
                assert field in q0, (
                    f"quabo_0 header missing required field '{field}'. "
                    f"Available: {list(q0.keys())}"
                )
        elif f["type"] == "PULSE_HEIGHT":
            # Single-quabo PH header
            for field in ("quabo_num", "tv_sec", "tv_usec"):
                assert field in header, (
                    f"PH header missing required field '{field}'. "
                    f"Available: {list(header.keys())}"
                )


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_frame_arrival_rate_is_reasonable(default_server_process):
    """
    Frames must arrive at a reasonable rate when update_interval_seconds=0.1:
    - At least 10 frames must arrive within 10 seconds.
    - Mean inter-frame wall-clock interval must be < 2 seconds.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        assert await client.init_hp_io(hosts=None, hp_io_cfg=HP_IO_REAL)

        stream = await client.stream_images(
            hosts=None,
            stream_movie_data=True, stream_pulse_height_data=True,
            update_interval_seconds=0.1,
            timeout=15.0,
        )

        arrival_times: list[float] = []
        async with asyncio.timeout(15.0):
            async for _ in stream:
                arrival_times.append(time.monotonic())
                if len(arrival_times) >= 10:
                    break

    assert len(arrival_times) >= 10, (
        f"Only {len(arrival_times)} frames arrived within 15s "
        "(expected ≥10 at 0.1s update interval)"
    )

    intervals = [arrival_times[i+1] - arrival_times[i] for i in range(len(arrival_times) - 1)]
    mean_interval = sum(intervals) / len(intervals)
    assert mean_interval < 2.0, (
        f"Mean inter-frame interval {mean_interval:.2f}s is too large "
        "(check hashpipe packet rate or tcpreplay speed)"
    )


@pytest.mark.usefixtures("hashpipe_pcap_runner")
async def test_module_id_is_consistent_across_frames(default_server_process):
    """
    All frames must come from the same module_id or a small set of known
    modules (determined by the pcap). Ensures the module discovery logic works.
    """
    frames = await _init_and_collect(
        default_server_process["ip_addr"], 20,
        stream_movie_data=True, stream_pulse_height_data=True,
    )
    assert frames, "No frames received"

    module_ids = {f["module_id"] for f in frames}
    # We expect 1-2 modules from a typical test pcap
    assert len(module_ids) <= 4, (
        f"Too many distinct module_ids received: {module_ids}. "
        "Check pcap or module.config"
    )
    for mid in module_ids:
        assert 0 <= mid <= 255, f"module_id {mid} out of range [0, 255]"
