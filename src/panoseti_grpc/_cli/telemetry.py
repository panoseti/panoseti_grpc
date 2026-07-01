from __future__ import annotations

from typing import Annotated

import grpc
import typer
from rich.console import Console

from panoseti_grpc.telemetry.client import TelemetryClient
from panoseti_grpc.telemetry.logger import get_logger

from .state import state

console = Console()
app = typer.Typer(help="Telemetry service operations", no_args_is_help=True)


@app.callback()
def telemetry_callback(
    ctx: typer.Context,
    timeout: Annotated[float | None, typer.Option(help="Telemetry-specific RPC timeout")] = None,
) -> None:
    """
    Telemetry service sub-commands.
    """
    if timeout:
        state.timeout = timeout


@app.command(name="log")
def telemetry_log(
    service: Annotated[str, typer.Option(help="Service name tag")] = "pseti-grpc",
    message: Annotated[str, typer.Option(help="Log payload JSON string")] = '{"event": "cli_test"}',
    severity: Annotated[int, typer.Option(help="Log severity 1=DEBUG … 5=CRITICAL")] = 2,
) -> None:
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
        raise typer.Exit(code=1) from None

    if result.success:
        console.print("[green]✓[/green] Log accepted by telemetry service.")
    else:
        console.print("[red]✗ Server rejected log (success=False)[/red]")
        raise typer.Exit(code=1) from None


@app.command(name="test")
def telemetry_test(
    count: Annotated[int, typer.Option(help="Number of test logs to send")] = 5,
    mixed: Annotated[bool, typer.Option(help="Send mixed severities and payloads")] = True,
) -> None:
    """Send a burst of test log messages to verify telemetry pipeline health."""
    client = TelemetryClient(host=state.host, port=state.port)
    console.print(f"[cyan]Sending {count} test logs to {state.host}:{state.port}...[/cyan]")

    for i in range(count):
        severity = (i % 5) + 1 if mixed else 2
        payload: dict[str, int | float | str | bool | None] = {"event": "cli_test_burst", "iteration": i}
        try:
            client.log_flexible(device_type="cli_test", device_id="pseti_grpc_test_01", data=payload)

            console.print(f" [dim]Sent log {i + 1}/{count} (severity {severity})[/dim]")
        except Exception as e:
            console.print(f"[red]Failed at index {i}: {e}[/red]")
            raise typer.Exit(code=1) from None

    console.print("[green]✓ Burst test complete.[/green]")
