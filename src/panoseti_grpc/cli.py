#!/usr/bin/env python3
"""
pseti-grpc — Extensible command-line interface for a running PSETI gRPC server.

Connects to the unified server (panoseti-server) and issues RPCs across all
registered services. Designed for observatory operators, developers, and CI
scripts that need to verify service health or trigger one-off operations.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Any

import typer

# Local Imports
from ._cli.state import state
from .util.cli import BaseLazyGroup, display_tree_callback


class GrpcLazyGroup(BaseLazyGroup):
    """
    Custom Click Group that lazy-loads commands from other modules.
    Ensures that heavy dependencies (like Protobuf or Rich) aren't loaded
    until a specific command is actually executed.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        lazy_mapping = {
            "stat": ("panoseti_grpc._cli.root", "stat", "Probe all services and print a summary."),
            "reflect": ("panoseti_grpc._cli.root", "reflect", "List all services via gRPC reflection."),
            "telemetry": ("panoseti_grpc._cli.telemetry", "app", "Telemetry service operations."),
            "daq-data": ("panoseti_grpc._cli.daq_data", "app", "DAQ Data service operations."),
            "daq-control": ("panoseti_grpc._cli.daq_control", "app", "DAQ Control service operations."),
            "server": ("panoseti_grpc._cli.server", "app", "Manage and run the unified gRPC server."),
        }
        super().__init__(*args, lazy_mapping=lazy_mapping, **kwargs)


# Base app for integration into PSETI (excludes 'test' to avoid redundancy)
app = typer.Typer(
    cls=GrpcLazyGroup,
    help="PSETI unified gRPC CLI.",
    no_args_is_help=True,
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback()
def main(
    ctx: typer.Context,
    host: Annotated[str, typer.Option(help="Server hostname or IP address")] = os.getenv("HEADNODE_IP", "localhost"),
    port: Annotated[int, typer.Option(help="Server gRPC port")] = int(os.getenv("HEADNODE_GRPC_PORT", "50051")),
    timeout: Annotated[float, typer.Option(help="Global RPC timeout in seconds")] = 10.0,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON output")] = False,
    grpc_logging: Annotated[
        bool, typer.Option("--grpc-logging", help="Forward CLI logs to Telemetry via gRPC")
    ] = False,
    log_level: Annotated[str, typer.Option(help="CLI log verbosity (DEBUG, INFO, etc)")] = "INFO",
    tree: Annotated[
        bool,
        typer.Option("--tree", "-t", help="Display the command tree for PSETI gRPC.", callback=display_tree_callback),
    ] = False,
) -> None:
    """
    PSETI gRPC CLI entry point.
    """
    state.host = host
    state.port = port
    state.timeout = timeout
    state.json = json_output
    state.grpc_logging = grpc_logging
    state.log_level = log_level

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(levelname)s %(name)s — %(message)s",
    )


# Standalone app for pseti-grpc CLI (includes 'test' for independent gRPC validation)
standalone_app = typer.Typer(
    cls=GrpcLazyGroup,
    help="PSETI unified gRPC CLI (Standalone).",
    no_args_is_help=True,
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)

# Reuse the callback
standalone_app.callback()(main)

# Add the test command explicitly to the standalone app only
try:
    from tests.qa import app as test_app

    standalone_app.add_typer(test_app, name="test")
except ImportError:
    pass

if __name__ == "__main__":
    standalone_app()
