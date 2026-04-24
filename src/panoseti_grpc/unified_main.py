"""
Console script entry point for the PANOSETI unified gRPC server.

Usage
-----
panoseti-server                                   # bundled default config (all services)
panoseti-server --profile daq_node                # DAQ node: daq_data + daq_control
panoseti-server --profile headnode                # Headnode: telemetry only
panoseti-server --config /path/to/server.toml     # custom config file
panoseti-server --services telemetry,daq_data     # override enabled services
panoseti-server --list-services                   # print registered services and exit
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="panoseti-server",
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
        choices=["default", "daq_node", "headnode"],
        default="default",
        help=(
            "Bundled deployment profile: "
            "'default' (all services), "
            "'daq_node' (daq_data + daq_control), "
            "'headnode' (telemetry only). "
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
        "--list-services",
        action="store_true",
        help="Print all registered services and exit.",
    )
    args = parser.parse_args()

    # Lazy import to keep startup fast for --list-services
    from panoseti_grpc.server import PanosetiServer, PanosetiServerConfig, ServiceRegistry

    if args.list_services:
        print("Registered PANOSETI gRPC services:")
        for name in ServiceRegistry.all():
            print(f"  {name}")
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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(PanosetiServer.run(cfg))


if __name__ == "__main__":
    main()
