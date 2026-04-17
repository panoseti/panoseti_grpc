#!/usr/bin/env python3
"""
pseti-cli — Extensible command-line interface for a running PANOSETI gRPC server.

Connects to the unified server (panoseti-server) and issues RPCs across all
registered services. Designed for observatory operators, developers, and CI
scripts that need to verify service health or trigger one-off operations.

Usage examples
--------------
    pseti-cli --host localhost --port 50051 status
    pseti-cli reflect
    pseti-cli telemetry log --service my-script --message "Observation started"
    pseti-cli daq-data ping
    pseti-cli daq-data init-sim
    pseti-cli daq-data stream --seconds 5
    pseti-cli daq-control status
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time

import grpc
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

console = Console()

# ---------------------------------------------------------------------------
# Shared gRPC channel helper
# ---------------------------------------------------------------------------


def _make_channel(host: str, port: int) -> grpc.Channel:
    """Return a synchronous insecure gRPC channel."""
    return grpc.insecure_channel(f"{host}:{port}")


# ---------------------------------------------------------------------------
# Reflection helper (used by both 'reflect' and 'status' commands)
# ---------------------------------------------------------------------------


def get_reflected_services(host: str, port: int, timeout_sec: float = 5.0) -> set[str]:
    """Query gRPC reflection and return the set of advertised service names.

    Args:
        host: Server hostname or IP address.
        port: Server gRPC port.
        timeout: Seconds to wait for the reflection response.

    Returns:
        Set of fully-qualified service names, or empty set on failure.
    """
    try:
        with _make_channel(host, port) as channel:
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


# ---------------------------------------------------------------------------
# 'status' command — connectivity probe for all three services
# ---------------------------------------------------------------------------


def cmd_status(args: argparse.Namespace) -> int:
    """Probe each registered service and print a status table.

    Returns 0 if all enabled services respond, 1 if any fail.
    """
    host, port = args.host, args.port
    get_logger("pseti-cli.status", grpc_enabled=args.grpc_logging)

    table = Table(title=f"Server status — {host}:{port}", show_header=True)
    table.add_column("Service", style="bold")
    table.add_column("Status")
    table.add_column("Detail")

    all_ok = True
    results: list[dict[str, str]] = []

    def add_result(service: str, status: str, detail: str, is_ok: bool) -> None:
        nonlocal all_ok
        table.add_row(service, status, detail)
        results.append({"service": service, "status": status, "detail": detail})
        if not is_ok:
            all_ok = False

    # --- DaqData: Ping RPC ---
    try:
        with _make_channel(host, port) as ch:
            daq_data_stub = daq_data_pb2_grpc.DaqDataStub(ch)
            daq_data_stub.Ping(Empty(), timeout=args.timeout, wait_for_ready=False)
        add_result("daq_data", "[green]✓ OK[/green]", "Ping responded", True)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.UNIMPLEMENTED:
            add_result("daq_data", "[yellow]— disabled[/yellow]", "UNIMPLEMENTED (service not hosted)", True)
        else:
            add_result("daq_data", "[red]✗ FAIL[/red]", f"{code.name}: {e.details()}", False)

    # --- DaqControl: StatusDaq RPC ---
    try:
        with _make_channel(host, port) as ch:
            daq_control_stub = daq_control_pb2_grpc.DaqControlStub(ch)
            req = daq_control_pb2.DaqStatusRequest(
                data_dir="/tmp",
                check_hashpipe_running=True,
                check_disk_usage=False,
                check_run_dirs=False,
            )
            resp = daq_control_stub.StatusDaq(req, timeout=args.timeout, wait_for_ready=False)
            hp_status = "running" if resp.hashpipe_running else "not running"
            add_result("daq_control", "[green]✓ OK[/green]", f"StatusDaq OK (hashpipe {hp_status})", True)
    except grpc.RpcError as e:
        code = e.code()
        if code == grpc.StatusCode.UNIMPLEMENTED:
            add_result("daq_control", "[yellow]— disabled[/yellow]", "UNIMPLEMENTED (service not hosted)", True)
        else:
            add_result("daq_control", "[red]✗ FAIL[/red]", f"{code.name}: {e.details()}", False)

    # --- Telemetry: Log RPC (send a probe log, check success flag) ---
    try:
        client = TelemetryClient(host=host, port=port)
        future = client.send_log_future(
            service="pseti-cli.status",
            severity=2,
            message=json.dumps({"event": "status_probe"}),
        )
        result = future.result(timeout=args.timeout)
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

    if args.json:
        print(json.dumps({"host": host, "port": port, "services": results}))
    else:
        console.print(table)

    return 0 if all_ok else 1


# ---------------------------------------------------------------------------
# 'reflect' command — list all services from gRPC reflection
# ---------------------------------------------------------------------------


def cmd_reflect(args: argparse.Namespace) -> int:
    """List all gRPC services advertised by the server via reflection.

    Returns 0 on success, 1 if reflection fails.
    """
    services = get_reflected_services(args.host, args.port, timeout_sec=args.timeout)
    if not services:
        console.print(f"[red]No services returned from {args.host}:{args.port}.[/red]")
        console.print("Is the server running and reflection enabled?")
        return 1

    if args.json:
        print(json.dumps({"host": args.host, "port": args.port, "services": sorted(services)}))
    else:
        table = Table(title=f"Reflected services — {args.host}:{args.port}", show_header=True)
        table.add_column("Service Name")
        for svc in sorted(services):
            table.add_row(svc)
        console.print(table)

    return 0


# ---------------------------------------------------------------------------
# 'telemetry log' command
# ---------------------------------------------------------------------------


def cmd_telemetry_log(args: argparse.Namespace) -> int:
    """Send a single test log message to the Telemetry service.

    Args:
        args.service: Service name tag attached to the log entry.
        args.message: Free-form message string (stored in payload_json).
        args.severity: Numeric severity (1=DEBUG … 5=CRITICAL).

    Returns 0 on success, 1 on failure.
    """
    get_logger("pseti-cli.telemetry", grpc_enabled=args.grpc_logging)
    client = TelemetryClient(host=args.host, port=args.port)
    try:
        future = client.send_log_future(
            service=args.service,
            severity=args.severity,
            message=args.message,
        )
        result = future.result(timeout=args.timeout)
    except grpc.RpcError as e:
        console.print(f"[red]Telemetry Log RPC failed: {e.code().name} — {e.details()}[/red]")
        return 1

    if result.success:
        console.print("[green]✓[/green] Log accepted by telemetry service.")
        return 0
    else:
        console.print("[red]✗ Server rejected log (success=False)[/red]")
        return 1


# ---------------------------------------------------------------------------
# 'daq-data ping' command
# ---------------------------------------------------------------------------


def cmd_daq_data_ping(args: argparse.Namespace) -> int:
    """Ping the DaqData service and report latency.

    Returns 0 if the Ping RPC completes successfully, 1 otherwise.
    """
    target = f"{args.host}:{args.port}"
    try:
        with _make_channel(args.host, args.port) as channel:
            stub = daq_data_pb2_grpc.DaqDataStub(channel)
            t0 = time.monotonic()
            stub.Ping(Empty(), timeout=args.timeout, wait_for_ready=True)
            latency_ms = (time.monotonic() - t0) * 1000
        if args.json:
            print(json.dumps({"host": args.host, "port": args.port, "latency_ms": round(latency_ms, 2)}))
        else:
            console.print(f"[green]✓[/green] DaqData Ping OK — {target} — {latency_ms:.1f} ms")
        return 0
    except grpc.RpcError as e:
        console.print(f"[red]✗ DaqData Ping FAILED — {e.code().name}: {e.details()}[/red]")
        return 1


# ---------------------------------------------------------------------------
# 'daq-data init-sim' command
# ---------------------------------------------------------------------------


def cmd_daq_data_init_sim(args: argparse.Namespace) -> int:
    """Initialize the DaqData service in simulation mode on the target server.

    Sends an InitHpIo RPC with simulate_daq=True, loading the bundled
    hp_io_config_simulate.json profile. This is the same action as
    ``panoseti-daq-data --init-sim`` but via the unified server port.

    Returns 0 on success, 1 on failure.
    """
    from panoseti_grpc.util.resources import load_package_json

    get_logger("pseti-cli.daq-data", grpc_enabled=args.grpc_logging)

    try:
        hp_io_cfg = load_package_json("panoseti_grpc", "daq_data/config/hp_io_config_simulate.json")
        hp_io_cfg["simulate_daq"] = True
        hp_io_cfg["force"] = True
    except Exception as e:
        console.print(f"[red]Failed to load hp_io_config_simulate.json: {e}[/red]")
        return 1

    try:
        with _make_channel(args.host, args.port) as channel:
            stub = daq_data_pb2_grpc.DaqDataStub(channel)
            req = daq_data_pb2.InitHpIoRequest(
                **{
                    k: v
                    for k, v in hp_io_cfg.items()
                    if k in {f.name for f in daq_data_pb2.InitHpIoRequest.DESCRIPTOR.fields}
                }
            )
            resp = stub.InitHpIo(req, timeout=args.timeout, wait_for_ready=True)
    except grpc.RpcError as e:
        console.print(f"[red]✗ InitHpIo RPC failed — {e.code().name}: {e.details()}[/red]")
        return 1
    except Exception as e:
        console.print(f"[red]✗ Unexpected error: {e}[/red]")
        return 1

    if resp.success:
        console.print("[green]✓[/green] DaqData simulation mode initialized.")
        return 0
    else:
        console.print(f"[red]✗ InitHpIo returned success=False: {resp.error_message}[/red]")
        return 1


# ---------------------------------------------------------------------------
# 'daq-data stream' command
# ---------------------------------------------------------------------------


async def _stream_images(host: str, port: int, seconds: float, timeout_sec: float) -> int:
    """Stream images from the DaqData service and print a one-line summary per frame.

    Args:
        host: Server host.
        port: Server port.
        seconds: How long to stream before cancelling (0 = stream until Ctrl-C).
        timeout: gRPC call timeout in seconds.

    Returns 0 on success (at least one frame received), 1 on error/no frames.
    """
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


def cmd_daq_data_stream(args: argparse.Namespace) -> int:
    """Stream images from the DaqData service and print frame summaries.

    Returns 0 if at least one frame was received, 1 otherwise.
    """
    return asyncio.run(_stream_images(args.host, args.port, args.seconds, args.timeout))


# ---------------------------------------------------------------------------
# 'daq-control status' command
# ---------------------------------------------------------------------------


def cmd_daq_control_status(args: argparse.Namespace) -> int:
    """Query the DaqControl service for Hashpipe and disk status.

    Returns 0 on success, 1 on failure.
    """
    get_logger("pseti-cli.daq-control", grpc_enabled=args.grpc_logging)
    try:
        with _make_channel(args.host, args.port) as channel:
            stub = daq_control_pb2_grpc.DaqControlStub(channel)
            req = daq_control_pb2.DaqStatusRequest(
                data_dir=args.data_dir,
                check_hashpipe_running=True,
                check_disk_usage=True,
                check_run_dirs=True,
            )
            resp = stub.StatusDaq(req, timeout=args.timeout, wait_for_ready=True)
    except grpc.RpcError as e:
        console.print(f"[red]✗ DaqControl StatusDaq failed — {e.code().name}: {e.details()}[/red]")
        return 1

    if not resp.success:
        console.print("[red]✗ StatusDaq returned success=False[/red]")
        return 1

    if args.json:
        from google.protobuf.json_format import MessageToDict

        print(json.dumps(MessageToDict(resp, preserving_proto_field_name=True)))
        return 0

    table = Table(title=f"DAQ Control status — {args.host}:{args.port}")
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
    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser with all subcommands registered."""
    parser = argparse.ArgumentParser(
        prog="pseti-cli",
        description="CLI for the PANOSETI unified gRPC server.",
    )

    # Global flags
    parser.add_argument(
        "--host",
        default=os.getenv("HEADNODE_IP", "localhost"),
        help="Server hostname or IP address (default: $HEADNODE_IP or 'localhost')",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("HEADNODE_GRPC_PORT", "50051")),
        help="Server gRPC port (default: $HEADNODE_GRPC_PORT or 50051)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="RPC timeout in seconds (default: 10.0)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output instead of rich tables",
    )
    parser.add_argument(
        "--grpc-logging",
        action="store_true",
        help="Forward CLI logs to the Telemetry service via gRPC",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="CLI log verbosity (default: INFO)",
    )

    subparsers = parser.add_subparsers(dest="command", title="commands", metavar="<command>")
    subparsers.required = True

    # --- status ---
    subparsers.add_parser(
        "status",
        help="Probe all services and print a connectivity summary",
    ).set_defaults(func=cmd_status)

    # --- reflect ---
    subparsers.add_parser(
        "reflect",
        help="List all services advertised via gRPC reflection",
    ).set_defaults(func=cmd_reflect)

    # --- telemetry ---
    telem_parser = subparsers.add_parser(
        "telemetry",
        help="Telemetry service operations",
    )
    telem_sub = telem_parser.add_subparsers(dest="telem_cmd", metavar="<subcommand>")
    telem_sub.required = True

    log_parser = telem_sub.add_parser("log", help="Send a test log message to Telemetry")
    log_parser.add_argument(
        "--service",
        default="pseti-cli",
        help="Service name attached to the log entry (default: pseti-cli)",
    )
    log_parser.add_argument(
        "--message",
        default='{"event": "cli_test"}',
        help='Log payload JSON string (default: \'{"event": "cli_test"}\')',
    )
    log_parser.add_argument(
        "--severity",
        type=int,
        default=2,
        choices=(1, 2, 3, 4, 5),
        help="Log severity 1=DEBUG … 5=CRITICAL (default: 2=INFO)",
    )
    log_parser.set_defaults(func=cmd_telemetry_log)

    # --- daq-data ---
    dd_parser = subparsers.add_parser(
        "daq-data",
        help="DAQ Data service operations",
    )
    dd_sub = dd_parser.add_subparsers(dest="dd_cmd", metavar="<subcommand>")
    dd_sub.required = True

    dd_sub.add_parser("ping", help="Ping the DaqData service").set_defaults(func=cmd_daq_data_ping)

    dd_sub.add_parser(
        "init-sim",
        help="Initialize DaqData in simulation mode (loads hp_io_config_simulate.json)",
    ).set_defaults(func=cmd_daq_data_init_sim)

    stream_parser = dd_sub.add_parser("stream", help="Stream images and print frame summaries")
    stream_parser.add_argument(
        "--seconds",
        type=float,
        default=5.0,
        help="Duration to stream in seconds; 0 = run until Ctrl-C (default: 5.0)",
    )
    stream_parser.set_defaults(func=cmd_daq_data_stream)

    # --- daq-control ---
    dc_parser = subparsers.add_parser(
        "daq-control",
        help="DAQ Control service operations",
    )
    dc_sub = dc_parser.add_subparsers(dest="dc_cmd", metavar="<subcommand>")
    dc_sub.required = True

    dc_status = dc_sub.add_parser("status", help="Query Hashpipe status and disk usage")
    dc_status.add_argument(
        "--data-dir",
        default="/tmp",
        help="Root data directory on the DAQ node (default: /tmp)",
    )
    dc_status.set_defaults(func=cmd_daq_control_status)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entry point for the pseti-cli tool."""
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(name)s — %(message)s",
    )

    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
