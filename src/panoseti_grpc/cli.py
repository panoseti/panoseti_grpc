#!/usr/bin/env python3
"""
pseti-grpc — Extensible command-line interface for a running PANOSETI gRPC server.

Connects to the unified server (panoseti-server) and issues RPCs across all
registered services. Designed for observatory operators, developers, and CI
scripts that need to verify service health or trigger one-off operations.
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
from pathlib import Path
from typing import Annotated

import click
import typer
import typer.core

# Local Imports
from ._cli.state import state


class GrpcLazyGroup(typer.core.TyperGroup):
    """
    Custom Click Group that lazy-loads commands from other modules.
    Ensures that heavy dependencies (like Protobuf or Rich) aren't loaded
    until a specific command is actually executed.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Mapping of command/group name -> (module_path, attr_name, help_string)
        self.lazy_mapping = {
            "status": ("panoseti_grpc._cli.root", "status", "Probe all services and print a summary."),
            "reflect": ("panoseti_grpc._cli.root", "reflect", "List all services via gRPC reflection."),
            "telemetry": ("panoseti_grpc._cli.telemetry", "app", "Telemetry service operations."),
            "daq-data": ("panoseti_grpc._cli.daq_data", "app", "DAQ Data service operations."),
            "daq-control": ("panoseti_grpc._cli.daq_control", "app", "DAQ Control service operations."),
        }

    def list_commands(self, ctx: click.Context) -> list[str]:
        base_cmds = super().list_commands(ctx)
        return sorted(set(base_cmds) | set(self.lazy_mapping.keys()))

    def get_command(self, ctx: click.Context, name: str) -> click.Command | None:
        # 1. Try standard command
        cmd = super().get_command(ctx, name)
        if cmd is not None:
            return cmd

        # 2. Try lazy command
        if name in self.lazy_mapping:
            module_path, attr_name, help_str = self.lazy_mapping[name]

            # Optimization: Skip loading if we just want the top-level help
            is_help_mode = any(arg in sys.argv for arg in ["--help", "-h"])
            is_targeting_this = (name in sys.argv)
            if is_help_mode and not is_targeting_this and not getattr(ctx, "resilient_parsing", False):
                return click.Command(name, help=help_str)

            try:
                mod = importlib.import_module(module_path)
                obj = getattr(mod, attr_name)

                # Convert Typer to Click if needed
                click_cmd = typer.main.get_command(obj) if isinstance(obj, typer.Typer) else obj

                # Promote single-command groups (e.g. status) to actual commands
                if isinstance(click_cmd, click.Group):
                    command_names = click_cmd.list_commands(ctx)
                    if len(command_names) == 1:
                        actual_cmd = click_cmd.get_command(ctx, command_names[0])
                        if actual_cmd:
                            if not actual_cmd.help:
                                actual_cmd.help = click_cmd.help
                            actual_cmd.name = name
                            return actual_cmd

                click_cmd.name = name
                if not click_cmd.help:
                    click_cmd.help = help_str
                return click_cmd
            except Exception as e:
                click.secho(f"Error loading command '{name}': {e}", fg="red", err=True)
                return None
        return None


app = typer.Typer(
    cls=GrpcLazyGroup,
    help="PANOSETI unified gRPC CLI.",
    no_args_is_help=True,
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback()
def main(
    ctx: typer.Context,
    host: Annotated[str, typer.Option(help="Server hostname or IP address")] = os.getenv("HEADNODE_IP", "localhost"),
    port: Annotated[int, typer.Option(help="Server gRPC port")] = int(os.getenv("HEADNODE_GRPC_PORT", "50051")),
    timeout: Annotated[float, typer.Option(help="RPC timeout in seconds")] = 10.0,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON output")] = False,
    grpc_logging: Annotated[bool, typer.Option(help="Forward CLI logs to Telemetry via gRPC")] = False,
    log_level: Annotated[str, typer.Option(help="CLI log verbosity")] = "INFO",
):
    """
    PANOSETI gRPC CLI entry point.
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


if __name__ == "__main__":
    app()
