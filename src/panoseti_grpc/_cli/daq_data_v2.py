from __future__ import annotations

import asyncio
import time
from typing import Annotated

import grpc
import typer
from google.protobuf.empty_pb2 import Empty
from rich.console import Console

from panoseti_grpc.generated import (
    daq_data_v2_pb2,
    daq_data_v2_pb2_grpc,
)
from panoseti_grpc.telemetry.logger import get_logger

from .state import state

console = Console()
app = typer.Typer(help="DAQ Data v2 service operations", no_args_is_help=True)


def _make_channel() -> grpc.Channel:
    """Return a synchronous insecure gRPC channel."""
    return grpc.insecure_channel(f"{state.host}:{state.port}")


@app.command(name="ping")
def daq_data_ping() -> None:
    """Ping the DaqDataV2 service and report latency."""
    target = f"{state.host}:{state.port}"
    try:
        with _make_channel() as channel:
            stub = daq_data_v2_pb2_grpc.DaqDataV2Stub(channel)
            t0 = time.monotonic()
            stub.Ping(Empty(), timeout=state.timeout, wait_for_ready=True)
            latency_ms = (time.monotonic() - t0) * 1000
        if state.json:
            import json
            print(json.dumps({"host": state.host, "port": state.port, "latency_ms": round(latency_ms, 2)}))
        else:
            console.print(f"[green]✓[/green] DaqDataV2 Ping OK — {target} — {latency_ms:.1f} ms")
    except grpc.RpcError as e:
        console.print(f"[red]✗ DaqDataV2 Ping FAILED — {e.code().name}: {e.details()}[/red]")
        raise typer.Exit(code=1) from None


async def _stream_images(host: str, port: int, seconds: float, timeout_sec: float) -> int:
    frames_received = 0
    deadline = time.monotonic() + seconds if seconds > 0 else float("inf")
    try:
        async with grpc.aio.insecure_channel(f"{host}:{port}") as channel:
            stub = daq_data_v2_pb2_grpc.DaqDataV2Stub(channel)
            req = daq_data_v2_pb2.StreamImagesRequest(
                stream_movie_data=True,
                stream_pulse_height_data=True,
                update_interval_seconds=0.1,
            )
            call = stub.StreamImages(req)
            async for resp in call:
                if time.monotonic() >= deadline:
                    call.cancel()
                    break
                frames_received += 1
                module_id = resp.pano_image.module_id
                frame_number = resp.pano_image.frame_number
                dp = "movie" if resp.pano_image.type == daq_data_v2_pb2.PanoImage.MOVIE else "ph"
                console.print(f"frame #{frame_number:6d}  module={module_id}  type={dp}")
    except grpc.aio.AioRpcError as e:
        if e.code() == grpc.StatusCode.CANCELLED:
            pass  # Normal stream cancellation
        else:
            console.print(f"[red]✗ StreamImagesV2 RPC failed — {e.code().name}: {e.details()}[/red]")
            return 1

    console.print(f"Stream ended — {frames_received} frame(s) received.")
    return 0 if frames_received > 0 else 1


@app.command(name="stream")
def daq_data_stream(
    seconds: Annotated[float, typer.Option(help="Duration to stream; 0 = run until Ctrl-C")] = 5.0,
) -> None:
    """Stream images from the DaqDataV2 service and print frame summaries."""
    ret = asyncio.run(_stream_images(state.host, state.port, seconds, state.timeout))
    if ret != 0:
        raise typer.Exit(code=ret)
