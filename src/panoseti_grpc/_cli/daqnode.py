"""pseti-grpc daqnode — Per-node health and readiness checks.

Reports:
  - gRPC service health via grpc.health.v1 (daq_data, daq_control, telemetry)
  - Grafana Alloy agent liveness via HTTP /-/ready endpoint
  - Disk usage for /var/log/panoseti (Alloy log-ship directory)
"""

from __future__ import annotations

import json
import os
import shutil
from typing import Annotated

import grpc
import typer
from grpc_health.v1 import health_pb2 as _hp2
from grpc_health.v1 import health_pb2_grpc as _hp2_grpc
from rich.console import Console
from rich.table import Table

from .state import state

console = Console()
app = typer.Typer(help="Per-node health and readiness checks.", no_args_is_help=True)

_ACTIVE_SERVICES = ["daqdata.DaqData", "panoseti.daq_control.DaqControl", "panoseti.telemetry.Telemetry"]
_LOG_DIR = "/var/log/panoseti"
_ALLOY_DEFAULT_PORT = 12345


def _check_alloy(alloy_host: str, alloy_port: int, timeout: float) -> tuple[bool, str]:
    """Return (ok, detail) from Alloy's HTTP /-/ready endpoint."""
    try:
        import urllib.request

        url = f"http://{alloy_host}:{alloy_port}/-/ready"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(256).decode("utf-8", errors="replace").strip()
            if resp.status == 200:
                return True, f"HTTP 200 — {body[:80]}"
            return False, f"HTTP {resp.status}"
    except OSError as e:
        return False, f"Unreachable: {e}"
    except Exception as e:
        return False, str(e)


def _disk_usage(path: str) -> str:
    """Return a human-readable disk-usage string for *path*, or an error message."""
    try:
        usage = shutil.disk_usage(path)
        used_gb = usage.used / 1_073_741_824
        total_gb = usage.total / 1_073_741_824
        pct = 100.0 * usage.used / usage.total if usage.total else 0.0
        return f"{used_gb:.1f} GB / {total_gb:.1f} GB ({pct:.0f}% used)"
    except FileNotFoundError:
        return f"[yellow]{path} not found[/yellow]"
    except PermissionError:
        return f"[yellow]{path} permission denied[/yellow]"
    except OSError as e:
        return f"[red]{e}[/red]"


@app.command("status")
def status(
    alloy_host: Annotated[str, typer.Option(help="Alloy HTTP host")] = os.getenv("ALLOY_HOST", "localhost"),
    alloy_port: Annotated[int, typer.Option(help="Alloy HTTP port")] = int(
        os.getenv("ALLOY_PORT", str(_ALLOY_DEFAULT_PORT))
    ),
    log_dir: Annotated[str, typer.Option(help="Log directory to report disk usage for")] = _LOG_DIR,
    skip_alloy: Annotated[bool, typer.Option("--skip-alloy", help="Skip Alloy liveness check")] = False,
) -> None:
    """Report gRPC service health, Alloy agent liveness, and log disk usage."""
    host, port = state.host, state.port
    all_ok = True

    # ── gRPC service health ──────────────────────────────────────────────────
    grpc_table = Table(title=f"gRPC services — {host}:{port}", show_header=True)
    grpc_table.add_column("Service", style="bold")
    grpc_table.add_column("Status")
    grpc_table.add_column("Detail")

    grpc_results: list[dict[str, str]] = []

    for svc in _ACTIVE_SERVICES:
        try:
            with grpc.insecure_channel(f"{host}:{port}") as ch:
                stub = _hp2_grpc.HealthStub(ch)
                resp = stub.Check(_hp2.HealthCheckRequest(service=svc), timeout=state.timeout)
            if resp.status == _hp2.HealthCheckResponse.SERVING:
                status_str, detail, ok = "[green]✓ SERVING[/green]", "grpc.health.v1 SERVING", True
            else:
                status_str, detail, ok = "[red]✗ NOT_SERVING[/red]", "grpc.health.v1 NOT_SERVING", False
        except grpc.RpcError as e:
            code = e.code()
            if code == grpc.StatusCode.UNIMPLEMENTED:
                status_str, detail, ok = "[yellow]— disabled[/yellow]", "UNIMPLEMENTED", True
            elif code == grpc.StatusCode.UNAVAILABLE:
                status_str, detail, ok = "[red]✗ FAIL[/red]", "server unreachable", False
            else:
                status_str, detail, ok = "[red]✗ FAIL[/red]", f"{code.name}: {e.details()}", False
        except Exception as e:
            status_str, detail, ok = "[red]✗ FAIL[/red]", str(e), False

        grpc_table.add_row(svc, status_str, detail)
        grpc_results.append({"service": svc, "status": status_str, "detail": detail})
        if not ok:
            all_ok = False

    # ── Alloy liveness ───────────────────────────────────────────────────────
    alloy_result: dict[str, str] = {}
    if not skip_alloy:
        alloy_ok, alloy_detail = _check_alloy(alloy_host, alloy_port, timeout=state.timeout)
        alloy_status_str = "[green]✓ ready[/green]" if alloy_ok else "[red]✗ unreachable[/red]"
        alloy_result = {
            "alloy_host": alloy_host,
            "alloy_port": str(alloy_port),
            "status": alloy_status_str,
            "detail": alloy_detail,
        }
        if not alloy_ok:
            all_ok = False

    # ── Disk usage ───────────────────────────────────────────────────────────
    disk_detail = _disk_usage(log_dir)
    disk_result = {"path": log_dir, "usage": disk_detail}

    # ── Output ───────────────────────────────────────────────────────────────
    if state.json:
        print(
            json.dumps(
                {
                    "host": host,
                    "port": port,
                    "grpc_services": grpc_results,
                    "alloy": alloy_result,
                    "disk": disk_result,
                }
            )
        )
    else:
        console.print(grpc_table)

        if not skip_alloy:
            alloy_table = Table(title=f"Grafana Alloy — {alloy_host}:{alloy_port}", show_header=True)
            alloy_table.add_column("Check", style="bold")
            alloy_table.add_column("Status")
            alloy_table.add_column("Detail")
            alloy_table.add_row("/-/ready", alloy_result["status"], alloy_result["detail"])
            console.print(alloy_table)

        disk_table = Table(title="Log disk usage", show_header=True)
        disk_table.add_column("Path", style="bold")
        disk_table.add_column("Usage")
        disk_table.add_row(log_dir, disk_detail)
        console.print(disk_table)

    if not all_ok:
        raise typer.Exit(code=1)
