from __future__ import annotations

import json
from typing import Annotated

import grpc
import typer
from rich.console import Console

from panoseti_grpc.telemetry.client import TelemetryClient
from panoseti_grpc.telemetry.logger import get_logger
from .state import state

console = Console()
app = typer.Typer(help="Telemetry service operations", no_args_is_help=True)


@app.command(name="log")
def telemetry_log(
    service: Annotated[str, typer.Option(help="Service name tag")] = "pseti-grpc",
    message: Annotated[str, typer.Option(help="Log payload JSON string")] = '{"event": "cli_test"}',
    severity: Annotated[int, typer.Option(help="Log severity 1=DEBUG … 5=CRITICAL")] = 2,
):
    """Send a single test log message to the Telemetry service."""
    get_logger("pseti-grpc.telemetry", grpc_enabled=state.grpc_logging)
    client = TelemetryClient(host=state.host, port=state.port)
    try:
        future = client.send_log_future(
            service=service,
            severity=severity,
            message=message,
        )
        result = future.result(timeout=state.timeout)
    except grpc.RpcError as e:
        console.print(f"[red]Telemetry Log RPC failed: {e.code().name} — {e.details()}[/red]")
        raise typer.Exit(code=1)

    if result.success:
        console.print("[green]✓[/green] Log accepted by telemetry service.")
    else:
        console.print("[red]✗ Server rejected log (success=False)[/red]")
        raise typer.Exit(code=1)
