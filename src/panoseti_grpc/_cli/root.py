from __future__ import annotations

import json

import grpc
import typer
from google.protobuf.empty_pb2 import Empty
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc
from rich.console import Console
from rich.table import Table

from panoseti_grpc.generated import (
    daq_control_pb2,
    daq_control_pb2_grpc,
    daq_data_pb2,
    daq_data_pb2_grpc,
)
from panoseti_grpc.telemetry.client import TelemetryClient
from panoseti_grpc.telemetry.logger import get_logger
from .state import state

console = Console()


def _make_channel() -> grpc.Channel:
    """Return a synchronous insecure gRPC channel."""
    return grpc.insecure_channel(f"{state.host}:{state.port}")


def get_reflected_services() -> set[str]:
    """Query gRPC reflection and return the set of advertised service names."""
    try:
        with _make_channel() as channel:
            stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            request = reflection_pb2.ServerReflectionRequest(list_services="")
            responses = stub.ServerReflectionInfo(iter([request]))
            services: set[str] = set()
            for resp in responses:
                for svc in resp.list_services_response.service:
                    services.add(svc.name)
            return services
    except grpc.RpcError:
        return set()


def status():
    """Probe all services and print a connectivity summary."""
    host, port = state.host, state.port
    get_logger("pseti-grpc.status", grpc_enabled=state.grpc_logging)

    table = Table(title=f"Server status — {host}:{port}", show_header=True)
    table.add_column("Service", style="bold")
    table.add_column("Status")
    table.add_column("Detail")

    all_ok = True
    results: list[dict[str, str]] = []

    def add_result(service: str, status_str: str, detail: str, is_ok: bool) -> None:
        nonlocal all_ok
        table.add_row(service, status_str, detail)
        results.append({"service": service, "status": status_str, "detail": detail})
        if not is_ok:
            all_ok = False

    # --- DaqData: Ping RPC ---
    try:
        with _make_channel() as ch:
            daq_data_stub = daq_data_pb2_grpc.DaqDataStub(ch)
            daq_data_stub.Ping(Empty(), timeout=state.timeout, wait_for_ready=False)
        add_result("daq_data", "[green]✓ OK[/green]", "Ping responded", True)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.UNIMPLEMENTED:
            add_result("daq_data", "[yellow]— disabled[/yellow]", "UNIMPLEMENTED (service not hosted)", True)
        else:
            add_result("daq_data", "[red]✗ FAIL[/red]", f"{code.name}: {e.details()}", False)

    # --- DaqControl: StatusDaq RPC ---
    try:
        with _make_channel() as ch:
            daq_control_stub = daq_control_pb2_grpc.DaqControlStub(ch)
            req = daq_control_pb2.DaqStatusRequest(
                data_dir="/tmp",
                check_hashpipe_running=True,
                check_disk_usage=False,
                check_run_dirs=False,
            )
            resp = daq_control_stub.StatusDaq(req, timeout=state.timeout, wait_for_ready=False)
            hp_status = "running" if resp.hashpipe_running else "not running"
            add_result("daq_control", "[green]✓ OK[/green]", f"StatusDaq OK (hashpipe {hp_status})", True)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.UNIMPLEMENTED:
            add_result("daq_control", "[yellow]— disabled[/yellow]", "UNIMPLEMENTED (service not hosted)", True)
        else:
            add_result("daq_control", "[red]✗ FAIL[/red]", f"{code.name}: {e.details()}", False)

    # --- Telemetry: Log RPC ---
    try:
        client = TelemetryClient(host=host, port=port)
        future = client.send_log_future(
            service="pseti-grpc.status",
            severity=2,
            message=json.dumps({"event": "status_probe"}),
        )
        result = future.result(timeout=state.timeout)
        if result.success:
            add_result("telemetry", "[green]✓ OK[/green]", "Log RPC accepted", True)
        else:
            add_result("telemetry", "[red]✗ FAIL[/red]", "Log RPC returned success=False", False)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.UNIMPLEMENTED:
            add_result("telemetry", "[yellow]— disabled[/yellow]", "UNIMPLEMENTED (service not hosted)", True)
        else:
            add_result("telemetry", "[red]✗ FAIL[/red]", f"{code.name}: {e.details()}", False)
    except Exception as e:
        add_result("telemetry", "[red]✗ FAIL[/red]", str(e), False)

    if state.json:
        print(json.dumps({"host": host, "port": port, "services": results}))
    else:
        console.print(table)

    if not all_ok:
        raise typer.Exit(code=1)


def reflect():
    """List all gRPC services advertised by the server via reflection."""
    services = get_reflected_services()
    if not services:
        console.print(f"[red]No services returned from {state.host}:{state.port}.[/red]")
        console.print("Is the server running and reflection enabled?")
        raise typer.Exit(code=1)

    if state.json:
        print(json.dumps({"host": state.host, "port": state.port, "services": sorted(services)}))
    else:
        table = Table(title=f"Reflected services — {state.host}:{state.port}", show_header=True)
        table.add_column("Service Name")
        for svc in sorted(services):
            table.add_row(svc)
        console.print(table)
