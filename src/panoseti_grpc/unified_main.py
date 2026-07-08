"""
Console script entry point for the PANOSETI unified gRPC server.

Usage
-----
pseti-grpc server                                   # bundled default config (all services)
pseti-grpc server --profile daq_node                # DAQ node: daq_data + daq_control
pseti-grpc server --profile headnode                # Headnode: telemetry only
pseti-grpc server --config /path/to/server.toml     # custom config file
pseti-grpc server --services telemetry,daq_data     # override enabled services
pseti-grpc server --list-services                   # print registered services and exit
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
from pathlib import Path


def resolve_bind_port(port: int | None, port_env: str | None, cfg_port: int) -> int:
    """Resolve the port the unified server should actually bind.

    There must be exactly one seam where server-bind-port precedence is
    decided, so it can never desync from the client-side resolver
    (``control.utils.util.resolve_grpc_port``) that picks the *same* env
    vars in the *same* order. Precedence, highest first:

    1. ``port`` — explicit CLI override (``--port``).
    2. ``os.getenv(port_env)`` — the role-scoped var the deployment
       (compose ``command:``, systemd unit, or operator) names via
       ``--port-env`` (e.g. ``HEADNODE_GRPC_PORT`` or ``DAQNODE_GRPC_PORT``).
       This can win over an *explicit* TOML port, deliberately: a
       deploy-time env var is a more specific, more recent signal than a
       config file that ships with the package/checkout.
    3. ``cfg_port`` — whatever ``PanosetiServerConfig`` already resolved
       (an explicit TOML value, or its own ``GRPC_PORT``-env
       ``default_factory`` fallback, or the built-in 50051 default).

    Plain parameters (not an argparse.Namespace/Typer context) so this one
    function can be shared verbatim by *both* CLI entry points that start
    the unified server -- ``unified_main.main()`` (argparse, reached via
    ``python -m panoseti_grpc``) and ``_cli/server.py`` (Typer, reached via
    the actual ``pseti-grpc server`` console script). These independently
    duplicate the config-load/service-toggle/run sequence; having drifted
    once already (the Typer one never gained --port/--port-env when this
    function was first added here), duplicating the resolution logic itself
    inline in each would only invite the same drift again.
    """
    if port is not None:
        return port
    if port_env:
        env_val = os.getenv(port_env)
        if env_val is not None:
            return int(env_val)
    return cfg_port


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pseti-grpc server",
        description="PANOSETI Unified gRPC Server — hosts multiple services on one port.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to a server.toml config file (overrides --profile).",
    )
    parser.add_argument(
        "--profile",
        choices=["default", "daq_node", "headnode", "gateway"],
        default="default",
        help=(
            "Bundled deployment profile: "
            "'default' (all services), "
            "'daq_node' (daq_data + daq_control), "
            "'headnode' (telemetry + daq_data gateway), "
            "'gateway' (telemetry + daq_data gateway; same shape as 'headnode', "
            "kept separate for sites that want telemetry-only vs. gateway split later). "
            "Ignored when --config is provided."
        ),
    )
    parser.add_argument(
        "--services",
        type=str,
        default=None,
        metavar="SVC1,SVC2,...",
        help=(
            "Comma-separated list of services to enable, overriding the config toggle. "
            "Example: --services telemetry,daq_data"
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        metavar="PORT",
        help=(
            "Explicit bind port, highest precedence. Prefer --port-env "
            "(or the HEADNODE_GRPC_PORT/DAQNODE_GRPC_PORT env vars) for "
            "deployments so the same .env drives both server and clients."
        ),
    )
    parser.add_argument(
        "--port-env",
        type=str,
        default=None,
        metavar="VAR",
        help=(
            "Name of the environment variable that overrides the bind port "
            "(e.g. HEADNODE_GRPC_PORT for a headnode/gateway profile, "
            "DAQNODE_GRPC_PORT for a daq_node profile). This is how one "
            "shared PanosetiServerConfig serves two different roles without "
            "profile-sniffing -- the deployment (compose command:, systemd "
            "unit, or operator) names the applicable var explicitly. See "
            "resolve_bind_port() for full precedence."
        ),
    )
    parser.add_argument(
        "--list-services",
        action="store_true",
        help="Print all registered services and exit.",
    )
    args = parser.parse_args()

    # Lazy import to keep startup fast for --list-services
    from panoseti_grpc.server import PanosetiServer, PanosetiServerConfig, ServiceRegistry

    if args.list_services:
        print("Registered PANOSETI gRPC services:")
        for name, descriptor in ServiceRegistry.all().items():
            tag = "  [DEPRECATED]" if descriptor.deprecated else ""
            print(f"  {name}{tag}")
        return

    # Load config
    if args.config is not None:
        cfg = PanosetiServerConfig.from_toml(args.config)
    else:
        cfg = PanosetiServerConfig.load_profile(args.profile)

    # CLI --services override
    if args.services is not None:
        enabled = {s.strip() for s in args.services.split(",")}
        for name in ServiceRegistry.all():
            if hasattr(cfg.services, name):
                setattr(cfg.services, name, name in enabled)

    # Bind-port resolution: the one seam where an env var (or --port) can
    # reconfigure the server without editing TOML. Must run after config
    # load (need cfg.port as the lowest-priority fallback) and before
    # PanosetiServer.run (which binds cfg.port verbatim).
    cfg.port = resolve_bind_port(args.port, args.port_env, cfg.port)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("panoseti_grpc.unified_main").info(
        "Binding port %d (--port=%s --port-env=%s -> %s)",
        cfg.port,
        args.port,
        args.port_env,
        os.getenv(args.port_env) if args.port_env else None,
    )

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(PanosetiServer.run(cfg))


if __name__ == "__main__":
    main()
