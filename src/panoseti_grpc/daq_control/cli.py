#!/usr/bin/env python3
import argparse
import logging
import json

from rich.console import Console
from rich.logging import RichHandler

from panoseti_grpc.daq_control.client import DaqControlClient

# Setup Rich Console
console = Console()
logger = logging.getLogger("daqcontrol.cli")

def setup_logging(level_name):
    level = getattr(logging, level_name.upper())
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )

def load_config(configfn):
    with open(configfn, 'r') as f:
        return json.load(f)

def human(n):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024

def run_client(args):
    client = DaqControlClient(args.host, args.port)
    console.print(f"[bold green]Connected to Daq Control Server at {args.host}:{args.port}[/]")
    p = load_config(args.config)
    if args.op == 'startdaq':
        logger.info('Starting Daq Capture...')
        if client.StartDaq(p):
            print('Daq Capture started successfully.')
            logger.info('Daq Capture started successfully.')
    elif args.op == 'stopdaq':
        logger.info('Stop Daq Capture...')
        if client.StopDaq(p):
            print('Daq Capture stopped successfully.')
            logger.info('Daq Capture stopped successfully.')
    elif args.op == 'statusdaq':
        logger.info('Getting Daq status...')
        success, status = client.StatusDaq(p)
        if success:
            print('Daq Status:')
            if p['check_hashpipe_running']:
                print('* HASHPIPE Status: ', status['hashpipe_running'])
            if p['check_disk_usage']:
                print(f"* Disk Usage {p['root_dir']}:")
                print("    - Total Disk Space: ", human(status['total_disk_space']))
                print("    - Used Disk Space: ", human(status['used_disk_space']))
                print("    - Free Disk Space: ", human(status['free_disk_space']))
                
def main():
    parser = argparse.ArgumentParser(description="PANOSETI Daq Control CLI")
    parser.add_argument("--host", default="localhost", help="gRPC Server Host")
    parser.add_argument("--port", type=int, default=50051, help="gRPC Server Port")
    parser.add_argument("--op", choices=['startdaq', 'stopdaq', 'statusdaq', ], default='startdaq', help="Valid operations.")
    parser.add_argument("--config", type=str, default='configs/startdaq.json', help="config file contains parameters for the specific operation.")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    run_client(args)

if __name__ == '__main__':
    main()
