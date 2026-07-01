from __future__ import annotations

import asyncio
import time
import warnings
from typing import Annotated

import grpc
import typer
from google.protobuf.empty_pb2 import Empty
from rich.console import Console

from panoseti_grpc.generated import (
    daq_data_pb2,
    daq_data_pb2_grpc,
)
from panoseti_grpc.telemetry.logger import get_logger

from .state import state

console = Console()
app = typer.Typer(help="DAQ Data service operations", no_args_is_help=True)


def _make_channel() -> grpc.Channel:
    """Return a synchronous insecure gRPC channel."""
    return grpc.insecure_channel(f"{state.host}:{state.port}")


@app.command(name="ping")
def daq_data_ping() -> None:
    """Ping the DaqData service and report latency. (Deprecated — use 'pseti-grpc stat' instead.)"""
    warnings.warn(
        "pseti-grpc daq-data ping is deprecated; use 'pseti-grpc stat' which probes all services "
        "via the standard gRPC health protocol.",
        DeprecationWarning,
        stacklevel=1,
    )
    target = f"{state.host}:{state.port}"
    try:
        with _make_channel() as channel:
            stub = daq_data_pb2_grpc.DaqDataStub(channel)
            t0 = time.monotonic()
            stub.Ping(Empty(), timeout=state.timeout, wait_for_ready=True)
            latency_ms = (time.monotonic() - t0) * 1000
        if state.json:
            import json

            print(json.dumps({"host": state.host, "port": state.port, "latency_ms": round(latency_ms, 2)}))
        else:
            console.print(f"[green]✓[/green] DaqData Ping OK — {target} — {latency_ms:.1f} ms")
    except grpc.RpcError as e:
        console.print(f"[red]✗ DaqData Ping FAILED — {e.code().name}: {e.details()}[/red]")
        raise typer.Exit(code=1) from None


@app.command(name="init-sim")
def daq_data_init_sim() -> None:
    """Initialize the DaqData service in simulation mode on the target server."""
    from panoseti_grpc.util.resources import load_package_json

    get_logger("pseti-grpc.daq-data", grpc_enabled=state.grpc_logging)

    try:
        hp_io_cfg = load_package_json("panoseti_grpc", "daq_data/config/hp_io_config_simulate.json")
        hp_io_cfg["simulate_daq"] = True
        hp_io_cfg["force"] = True
    except Exception as e:
        console.print(f"[red]Failed to load hp_io_config_simulate.json: {e}[/red]")
        raise typer.Exit(code=1) from None

    try:
        with _make_channel() as channel:
            stub = daq_data_pb2_grpc.DaqDataStub(channel)
            req = daq_data_pb2.InitHpIoRequest(
                **{
                    k: v
                    for k, v in hp_io_cfg.items()
                    if k in {f.name for f in daq_data_pb2.InitHpIoRequest.DESCRIPTOR.fields}
                }
            )
            resp = stub.InitHpIo(req, timeout=state.timeout, wait_for_ready=True)
    except grpc.RpcError as e:
        console.print(f"[red]✗ InitHpIo RPC failed — {e.code().name}: {e.details()}[/red]")
        raise typer.Exit(code=1) from None
    except Exception as e:
        console.print(f"[red]✗ Unexpected error: {e}[/red]")
        raise typer.Exit(code=1) from None

    if resp.success:
        console.print("[green]✓[/green] DaqData simulation mode initialized.")
    else:
        console.print(f"[red]✗ InitHpIo returned success=False: {resp.error_message}[/red]")
        raise typer.Exit(code=1) from None


async def _stream_images(host: str, port: int, seconds: float, timeout_sec: float) -> int:
    frames_received = 0
    deadline = time.monotonic() + seconds if seconds > 0 else float("inf")
    try:
        async with grpc.aio.insecure_channel(f"{host}:{port}") as channel:
            stub = daq_data_pb2_grpc.DaqDataStub(channel)
            req = daq_data_pb2.StreamImagesRequest(
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
                dp = "movie" if resp.pano_image.type == daq_data_pb2.PanoImage.MOVIE else "ph"
                console.print(f"frame #{frame_number:6d}  module={module_id}  type={dp}")
    except grpc.aio.AioRpcError as e:
        if e.code() == grpc.StatusCode.CANCELLED:
            pass  # Normal stream cancellation
        else:
            console.print(f"[red]✗ StreamImages RPC failed — {e.code().name}: {e.details()}[/red]")
            return 1

    console.print(f"Stream ended — {frames_received} frame(s) received.")
    return 0 if frames_received > 0 else 1


@app.command(name="stream")
def daq_data_stream(
    seconds: Annotated[float, typer.Option(help="Duration to stream; 0 = run until Ctrl-C")] = 5.0,
) -> None:
    """Stream images from the DaqData service and print frame summaries."""
    ret = asyncio.run(_stream_images(state.host, state.port, seconds, state.timeout))
    if ret != 0:
        raise typer.Exit(code=ret)
