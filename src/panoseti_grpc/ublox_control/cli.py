#!/usr/bin/env python3

"""
Command Line Interface for the client-side RPC methods.
These commands expect the following to function correctly:
    1. Required gRPC Python packages are installed.
    2. A network connection to a gRPC UbloxControl server intance.

"""
import asyncio
import argparse
import json
from pathlib import Path
from rich import print

from .client import AioUbloxControlClient
from .resources import default_f9t_cfg


async def run_init(args):
    """Handles the 'init' command."""
    f9t_config = default_f9t_cfg.copy()
    # In a real-world scenario, you would load or construct a specific config
    # For this example, we just set the device path from the command line
    if not args.device.startswith('/dev/'):
        print("[bold red]Error:[/] The device path must be a valid path (e.g., /dev/ttyACM0)")
        return

    f9t_config['device'] = args.device

    async with AioUbloxControlClient(args.hosts) as client:
        print(f"Attempting to initialize F9T on hosts: {args.hosts}")
        success = await client.init_f9t(args.hosts, f9t_config)
        if success:
            print("[bold green]Initialization successful![/bold green]")
        else:
            print("[bold red]Initialization failed. Check server logs for details.[/bold red]")


async def run_capture(args):
    """Handles the 'capture' command."""
    async with AioUbloxControlClient(args.hosts) as client:
        print(f"Starting data capture from hosts: {args.hosts}")
        print("Press CTRL+C to stop.")
        try:
            async for data in client.capture_ublox(args.hosts, args.patterns):
                print(data)
        except asyncio.CancelledError:
            print("\nCapture stopped by user.")


def main():
    """Sets up argument parsing and dispatches commands."""
    parser = argparse.ArgumentParser(description="UbloxControl gRPC Client CLI")
    parser.add_argument("--hosts", nargs="+", default=["localhost:50051"], help="List of server hosts to connect to.")

    subparsers = parser.add_subparsers(required=True, dest="command")

    # Init command
    parser_init = subparsers.add_parser("init", help="Initialize the ZED-F9T device.")
    parser_init.add_argument("device", help="The device file path on the server (e.g., /dev/ttyACM0).")
    parser_init.set_defaults(func=run_init)

    # Capture command
    parser_capture = subparsers.add_parser("capture", help="Capture and stream data from the F9T.")
    parser_capture.add_argument("--patterns", nargs="*", help="Regex patterns to filter message names.")
    parser_capture.set_defaults(func=run_capture)

    args = parser.parse_args()

    try:
        asyncio.run(args.func(args))
    except KeyboardInterrupt:
        print("\nExiting.")


if __name__ == "__main__":
    main()

