from __future__ import annotations

import json
from typing import Annotated

import grpc
import typer
from rich.console import Console
from rich.table import Table

from panoseti_grpc.generated import (
    daq_control_pb2,
    daq_control_pb2_grpc,
)
from panoseti_grpc.telemetry.logger import get_logger
from .state import state

console = Console()
app = typer.Typer(help="DAQ Control service operations", no_args_is_help=True)


def _make_channel() -> grpc.Channel:
    """Return a synchronous insecure gRPC channel."""
    return grpc.insecure_channel(f"{state.host}:{state.port}")


@app.command(name="status")
def daq_control_status(
    data_dir: Annotated[str, typer.Option(help="Root data directory on the DAQ node")] = "/tmp",
):
    """Query the DaqControl service for Hashpipe and disk status."""
    get_logger("pseti-grpc.daq-control", grpc_enabled=state.grpc_logging)
    try:
        with _make_channel() as channel:
            stub = daq_control_pb2_grpc.DaqControlStub(channel)
            req = daq_control_pb2.DaqStatusRequest(
                data_dir=data_dir,
                check_hashpipe_running=True,
                check_disk_usage=True,
                check_run_dirs=True,
            )
            resp = stub.StatusDaq(req, timeout=state.timeout, wait_for_ready=True)
    except grpc.RpcError as e:
        console.print(f"[red]✗ DaqControl StatusDaq failed — {e.code().name}: {e.details()}[/red]")
        raise typer.Exit(code=1)

    if not resp.success:
        console.print("[red]✗ StatusDaq returned success=False[/red]")
        raise typer.Exit(code=1)

    if state.json:
        from google.protobuf.json_format import MessageToDict
        print(json.dumps(MessageToDict(resp, preserving_proto_field_name=True)))
        return

    table = Table(title=f"DAQ Control status — {state.host}:{state.port}")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row(
        "Hashpipe running",
        "[green]Yes[/green]" if resp.hashpipe_running else "[yellow]No[/yellow]",
    )
    if resp.disk_usage:
        for k, v in resp.disk_usage.items():
            table.add_row(f"Disk ({k})", str(v))
    if resp.run_dirs:
        table.add_row("Run dirs", "\n".join(resp.run_dirs))
    console.print(table)
