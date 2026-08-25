#!/usr/bin/env python3
"""
pseti-grpc — Extensible command-line interface for a running PSETI gRPC server.

Connects to the unified server (pseti-grpc server) and issues RPCs across all
registered services. Designed for observatory operators, developers, and CI
scripts that need to verify service health or trigger one-off operations.
"""

from __future__ import annotations

import importlib.metadata
import importlib.resources
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import typer

# Local Imports
from ._cli.state import state
from .util.cli import BaseLazyGroup, display_tree_callback
from .util.env_loader import load_pseti_grpc_env

# Load .env variables (if any) before evaluating the option defaults below
# (some read os.environ, e.g. PSETI_GRPC_HOST/PSETI_GRPC_PORT) and before any
# subcommand runs -- mirrors panoseti's `pseti` CLI (control.pseti), which
# does the same for the same reason.
load_pseti_grpc_env()


def _version_callback(value: bool) -> None:
    if value:
        print(f"pseti-grpc {importlib.metadata.version('panoseti-grpc')}")
        raise typer.Exit()


def _env_template_callback(value: bool) -> None:
    if not value:
        return
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    dest = Path.cwd() / f".env_grpc_{timestamp}"
    if dest.exists():
        print(f"Refusing to overwrite existing file: {dest}")
        raise typer.Exit(code=1)
    # importlib.resources (not a bare __file__-relative path) so this also
    # works if the package is ever imported from a zipped wheel.
    resource = importlib.resources.files("panoseti_grpc").joinpath(".env_example")
    with importlib.resources.as_file(resource) as src:
        shutil.copyfile(src, dest)
    print(f"Wrote .env template to {dest}")
    raise typer.Exit()


def _config_template_callback(value: bool) -> None:
    if not value:
        return
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    dest = Path.cwd() / f"pseti_grpc_config_{timestamp}"
    if dest.exists():
        print(f"Refusing to overwrite existing directory: {dest}")
        raise typer.Exit(code=1)
    # importlib.resources (not a bare __file__-relative path) so this also
    # works if the package is ever imported from a zipped wheel.
    config_dir = importlib.resources.files("panoseti_grpc").joinpath("config")
    with importlib.resources.as_file(config_dir) as src:
        dest.mkdir(parents=True)
        for toml_file in sorted(src.glob("*.toml")):
            shutil.copy2(toml_file, dest / toml_file.name)
    print(f"Wrote config template directory to {dest}")
    raise typer.Exit()


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
            "daqnode": ("panoseti_grpc._cli.daqnode", "app", "Per-node health: gRPC services, Alloy, disk."),
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
    host: Annotated[
        str, typer.Option(help="Server hostname or IP address. Default: PSETI_GRPC_HOST env var, or localhost.")
    ] = os.getenv("PSETI_GRPC_HOST", "localhost"),
    port: Annotated[
        int, typer.Option(help="Server gRPC port. Default: PSETI_GRPC_PORT env var, or 50051.")
    ] = int(os.getenv("PSETI_GRPC_PORT", "50051")),
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
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Print the installed panoseti-grpc package version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
    env_template: Annotated[
        bool,
        typer.Option(
            "--env-template",
            help=(
                "Copy the packaged .env_example to ./.env_grpc_<timestamp> and exit. "
                "Point PSETI_GRPC_ENV_FILE at the generated file to load it."
            ),
            callback=_env_template_callback,
            is_eager=True,
        ),
    ] = False,
    config_template: Annotated[
        bool,
        typer.Option(
            "--config-template",
            help=(
                "Copy the packaged config/*.toml files to "
                "./pseti_grpc_config_<timestamp> and exit."
            ),
            callback=_config_template_callback,
            is_eager=True,
        ),
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
