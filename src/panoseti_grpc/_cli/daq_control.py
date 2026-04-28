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


@app.command(name="stat")
def daq_control_status(
    data_dir: Annotated[str, typer.Option(help="Root data directory on the DAQ node")] = "/tmp",
    hashpipe: Annotated[bool, typer.Option(help="Check if Hashpipe process is running")] = True,
    disk: Annotated[bool, typer.Option(help="Check disk usage")] = True,
    runs: Annotated[bool, typer.Option(help="List run directories")] = True,
) -> None:
    """Query the DaqControl service for Hashpipe and disk status."""
    get_logger("pseti-grpc.daq-control", grpc_enabled=state.grpc_logging)
    try:
        with _make_channel() as channel:
            stub = daq_control_pb2_grpc.DaqControlStub(channel)
            req = daq_control_pb2.DaqStatusRequest(
                data_dir=data_dir,
                check_hashpipe_running=hashpipe,
                check_disk_usage=disk,
                check_run_dirs=runs,
            )
            resp = stub.StatusDaq(req, timeout=state.timeout, wait_for_ready=True)
    except grpc.RpcError as e:
        console.print(f"[red]✗ DaqControl StatusDaq failed — {e.code().name}: {e.details()}[/red]")
        raise typer.Exit(code=1) from None

    if not resp.success:
        console.print("[red]✗ StatusDaq returned success=False[/red]")
        if resp.message:
            console.print(f"[dim]Reason: {resp.message}[/dim]")
        raise typer.Exit(code=1) from None

    if state.json:
        from google.protobuf.json_format import MessageToDict

        print(json.dumps(MessageToDict(resp, preserving_proto_field_name=True)))
        return

    table = Table(title=f"DAQ Control status — {state.host}:{state.port}")
    table.add_column("Field")
    table.add_column("Value")

    if hashpipe:
        table.add_row(
            "Hashpipe running",
            "[green]Yes[/green]" if resp.hashpipe_running else "[yellow]No[/yellow]",
        )
    if disk and resp.disk_usage:
        for k, v in resp.disk_usage.items():
            table.add_row(f"Disk ({k})", str(v))
    if runs and resp.run_dirs:
        table.add_row("Run dirs", "\n".join(resp.run_dirs))

    console.print(table)


@app.command(name="get-manifest")
def daq_control_get_manifest(
    run_dir: Annotated[str, typer.Option(help="Target run directory")],
    module_id: Annotated[int, typer.Option(help="Target module ID")],
    data_dir: Annotated[str, typer.Option(help="Root data directory")] = "/tmp",
) -> None:
    """Stream manifest entries for a module's run data."""
    get_logger("pseti-grpc.daq-control.manifest", grpc_enabled=state.grpc_logging)
    try:
        with _make_channel() as channel:
            stub = daq_control_pb2_grpc.DaqControlStub(channel)
            req = daq_control_pb2.GetManifestRequest(data_dir=data_dir, run_dir=run_dir, module_id=module_id)

            table = Table(title=f"Manifest: {run_dir} (Module {module_id})")
            table.add_column("Path", style="green")
            table.add_column("Digest", style="cyan")
            table.add_column("Size (Bytes)", style="magenta")

            found = False
            # Streaming RPC
            for entry in stub.GetManifest(req, timeout=state.timeout):
                found = True
                table.add_row(entry.relative_path, entry.digest_hex, str(entry.size_bytes))

            if found:
                console.print(table)
            else:
                console.print("[yellow]No manifest entries found for this run/module.[/yellow]")

    except grpc.RpcError as e:
        console.print(f"[red]✗ GetManifest failed — {e.code().name}: {e.details()}[/red]")
        raise typer.Exit(code=1) from None
