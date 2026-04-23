from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(help="Manage and run the unified gRPC server.", no_args_is_help=True)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    config: Annotated[Path | None, typer.Option(help="Path to a server.toml config file (overrides --profile)")] = None,
    profile: Annotated[
        str,
        typer.Option(
            help=(
                "Bundled deployment profile: 'default' (all services), "
                "'daq_node' (daq_data + daq_control), 'headnode' (telemetry only). "
                "Ignored when --config is provided."
            )
        ),
    ] = "default",
    services: Annotated[
        str | None,
        typer.Option(
            help=(
                "Comma-separated list of services to enable, overriding the config toggle. "
                "Example: --services telemetry,daq_data"
            )
        ),
    ] = None,
    list_services: Annotated[bool, typer.Option(help="Print all registered services and exit.")] = False,
) -> None:
    """
    Run the PANOSETI Unified gRPC Server.
    """
    if ctx.invoked_subcommand is not None:
        return

    from panoseti_grpc.server import PanosetiServer, PanosetiServerConfig, ServiceRegistry

    if list_services:
        print("Registered PANOSETI gRPC services:")
        for name in ServiceRegistry.all():
            print(f"  {name}")
        return

    # Load config
    cfg = PanosetiServerConfig.from_toml(config) if config is not None else PanosetiServerConfig.load_profile(profile)

    # CLI --services override
    if services is not None:
        enabled = {s.strip() for s in services.split(",")}
        for name in ServiceRegistry.all():
            if hasattr(cfg.services, name):
                setattr(cfg.services, name, name in enabled)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(PanosetiServer.run(cfg))


if __name__ == "__main__":
    app()
