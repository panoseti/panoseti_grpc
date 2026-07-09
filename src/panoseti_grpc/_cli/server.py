from __future__ import annotations

import asyncio
import contextlib
import logging
import os
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
                "'daq_node' (daq_data + daq_control), "
                "'headnode' (telemetry + daq_data gateway), "
                "'gateway' (telemetry + daq_data gateway; same shape as 'headnode'). "
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
    port: Annotated[
        int | None,
        typer.Option(
            help=(
                "Explicit bind port, highest precedence. Prefer --port-env "
                "(or the HEADNODE_GRPC_PORT/DAQNODE_GRPC_PORT env vars) for "
                "deployments so the same .env drives both server and clients."
            )
        ),
    ] = None,
    port_env: Annotated[
        str | None,
        typer.Option(
            "--port-env",
            help=(
                "Name of the environment variable that overrides the bind port "
                "(e.g. HEADNODE_GRPC_PORT for a headnode/gateway profile, "
                "DAQNODE_GRPC_PORT for a daq_node profile). See "
                "unified_main.resolve_bind_port() for full precedence -- this "
                "is the same resolver, shared so this entry point (the real "
                "'pseti-grpc server' console script) and unified_main.py's "
                "('python -m panoseti_grpc') can't drift apart again."
            ),
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
    from panoseti_grpc.unified_main import resolve_bind_port

    if list_services:
        print("Registered PANOSETI gRPC services:")
        for name, descriptor in ServiceRegistry.all().items():
            tag = "  [DEPRECATED]" if descriptor.deprecated else ""
            print(f"  {name}{tag}")
        return

    # Load config
    cfg = PanosetiServerConfig.from_toml(config) if config is not None else PanosetiServerConfig.load_profile(profile)

    # CLI --services override
    if services is not None:
        enabled = {s.strip() for s in services.split(",")}
        for name in ServiceRegistry.all():
            if hasattr(cfg.services, name):
                setattr(cfg.services, name, name in enabled)

    # Bind-port resolution: must run after config load (need cfg.port as the
    # lowest-priority fallback) and before PanosetiServer.run (which binds
    # cfg.port verbatim). See resolve_bind_port()'s docstring for precedence.
    cfg.port = resolve_bind_port(port, port_env, cfg.port)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("panoseti_grpc._cli.server").info(
        "Binding port %d (--port=%s --port-env=%s -> %s)",
        cfg.port,
        port,
        port_env,
        os.getenv(port_env) if port_env else None,
    )

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(PanosetiServer.run(cfg))


if __name__ == "__main__":
    app()
