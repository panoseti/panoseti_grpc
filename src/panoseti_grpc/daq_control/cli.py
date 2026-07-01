#!/usr/bin/env python3
import argparse
import json
import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from panoseti_grpc.daq_control.client import DaqControlClient

# Setup Rich Console
console = Console()
logger = logging.getLogger("daqcontrol.cli")


def setup_logging(level_name: Any) -> None:
    level = getattr(logging, level_name.upper())
    logging.basicConfig(
        level=level, format="%(message)s", datefmt="[%X]", handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )


def load_config(configfn: Any) -> dict[str, Any]:
    with open(configfn) as f:
        config: dict[str, Any] = json.load(f)
        return config


def human(n: Any) -> str:
    if n is None or n == -1:
        return "N/A"
    n_float = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n_float < 1024:
            return f"{n_float:.2f} {unit}"
        n_float /= 1024
    return f"{n_float:.2f} PB"


def run_client(args: Any) -> None:
    client = DaqControlClient(args.host, args.port)
    console.print(f"[bold green]Connected to Daq Control Server at {args.host}:{args.port}[/]")
    p = load_config(args.config)
    if args.op == "startdaq":
        logger.debug("Starting Daq Capture...")
        if client.StartDaq(p["startdaq"]):
            console.print("[bold green]Daq Capture started successfully.[/]")
            logger.debug("Daq Capture started successfully.")
    elif args.op == "stopdaq":
        logger.debug("Stop Daq Capture...")
        if client.StopDaq(p["stopdaq"]):
            console.print("[bold green]Daq Capture stopped successfully.[/]")
            logger.debug("Daq Capture stopped successfully.")
    elif args.op == "statusdaq":
        logger.debug("Getting Daq status...")
        success, status = client.StatusDaq(p["statusdaq"])
        if success:
            console.print("[bold bright_blue]******** Daq Node Status ********[/]")
            if p["statusdaq"]["check_hashpipe_running"]:
                console.print("[bold magenta]* HASHPIPE Running[/]")
                console.print(f"[bold magenta]    - Status: {status['hashpipe_running']}[/]")
            if p["statusdaq"]["check_disk_usage"]:
                console.print(f"[bold cyan]* Disk Usage ({p['statusdaq']['data_dir']})[/]")
                console.print(
                    f"[bold cyan]    - Total Disk Space: {human(status['disk_usage']['total_disk_space'])}[/]"
                )
                console.print(f"[bold cyan]    - Used  Disk Space: {human(status['disk_usage']['used_disk_space'])}[/]")
                console.print(f"[bold cyan]    - Free  Disk Space: {human(status['disk_usage']['free_disk_space'])}[/]")
            if p["statusdaq"]["check_run_dirs"]:
                console.print("[bold yellow]* Run Dirs [/]")
                for r in status["run_dirs"]:
                    console.print(f"[bold yellow]   - {r}[/]")
    elif args.op == "cleanupdata":
        logger.debug("Clean up data dirs...")
        cleanup_resp = client.CleanupData(p["cleanupdata"])
        if cleanup_resp.get("success", False):
            console.print("[bold bright_blue]Clean up data dirs successfully.[/]")
            datadir = p["cleanupdata"]["data_dir"]
            rundir = p["cleanupdata"]["run_dir"]
            module_id = p["cleanupdata"]["module_id"]
            console.print("[bold yellow]Cleaned up Directories [/]")
            console.print(f"[bold yellow]   - {datadir}/{rundir}[/]")
            for id in module_id:
                console.print(f"[bold yellow]   - {datadir}/module_{id}/{rundir}[/]")
            logger.debug("Clean up data dirs successfully.")


def main() -> None:
    parser = argparse.ArgumentParser(description="PANOSETI Daq Control CLI")
    parser.add_argument("--host", default="localhost", help="gRPC Server Host")
    parser.add_argument("--port", type=int, default=50051, help="gRPC Server Port")
    parser.add_argument(
        "--op",
        choices=["startdaq", "stopdaq", "statusdaq", "cleanupdata"],
        default="startdaq",
        help="Valid operations.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="configs/startdaq.json",
        help="config file contains parameters for the specific operation.",
    )
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])

    args = parser.parse_args()
    setup_logging(args.log_level)
    run_client(args)


if __name__ == "__main__":
    main()
