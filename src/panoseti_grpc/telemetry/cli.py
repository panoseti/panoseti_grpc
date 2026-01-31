#!/usr/bin/env python3
import argparse
import time
import random
import logging
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.table import Table

from panoseti_grpc.telemetry.client import TelemetryClient

# Setup Rich Console
console = Console()
logger = logging.getLogger("telemetry.cli")


def setup_logging(level_name):
    level = getattr(logging, level_name.upper())
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )


def generate_payload(payload_type, iteration):
    """Generates dummy data based on the requested type."""
    device_id = f"cli_device_{random.randint(1, 99):02d}"

    if payload_type == "test":
        return "log_test", {
            "device_id": device_id,
            "iteration": iteration,
            "value": random.uniform(0, 100),
            "message": f"CLI_MSG_{iteration}",
            "active": True
        }

    elif payload_type == "gnss":
        return "log", {
            "device_type": "gnss",
            "device_id": device_id,
            "data": {
                "satellites": random.randint(4, 12),
                "lat": 37.3 + random.uniform(-0.1, 0.1),
                "lon": -121.8 + random.uniform(-0.1, 0.1),
                "fix_mode": "3D",
                "extra_data": {"hdop": 1.5}
            }
        }

    elif payload_type == "dew":
        return "log", {
            "device_type": "dew",
            "device_id": device_id,
            "data": {
                "temp_c": random.uniform(10, 30),
                "humidity": random.uniform(20, 80)
            }
        }

    elif payload_type == "flex":
        return "log_flexible", {
            "device_type": "test_flex",
            "device_id": device_id,
            "data": {
                "cpu_load": random.randint(10, 90),
                "fan_rpm": random.randint(1000, 5000),
                "status": "nominal"
            }
        }
    return None, {}


def run_sender(args):
    client = TelemetryClient(host=args.host, port=args.port)
    console.print(f"[bold green]Connected to Telemetry Server at {args.host}:{args.port}[/]")

    types_to_send = []
    if args.type == 'mixed':
        types_to_send = ['test', 'gnss', 'dew', 'flex']
    else:
        types_to_send = [args.type]

    # Metrics
    success_count = 0
    fail_count = 0
    total_latency_ms = 0
    min_latency = float('inf')
    max_latency = 0

    try:
        with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeRemainingColumn(),
                TextColumn("[dim cyan]({task.fields[latency]} ms/req)"),
                console=console
        ) as progress:

            task = progress.add_task(f"Sending {args.count} messages...", total=args.count, latency="0.0")

            for i in range(args.count):
                # Pick a type (round-robin if mixed)
                current_type = types_to_send[i % len(types_to_send)]
                method_name, kwargs = generate_payload(current_type, i)

                # Log payload at DEBUG level
                logger.debug(f"Payload #{i}: {kwargs}")

                start_time = time.perf_counter()
                try:
                    # Dynamic dispatch to client methods
                    method = getattr(client, method_name)
                    method(**kwargs)

                    # If we get here, call was successful (client.py raises on failure)
                    success_count += 1
                    status_symbol = "[green]✔[/]"

                except Exception as e:
                    fail_count += 1
                    status_symbol = "[red]✘[/]"
                    logger.error(f"Failed to send message #{i}: {e}")

                # Metrics Calculation
                end_time = time.perf_counter()
                latency_ms = (end_time - start_time) * 1000
                total_latency_ms += latency_ms
                min_latency = min(min_latency, latency_ms)
                max_latency = max(max_latency, latency_ms)
                avg_latency = total_latency_ms / (i + 1)

                # Update Progress Bar
                progress.update(
                    task,
                    advance=1,
                    description=f"{status_symbol} Sending [bold cyan]{current_type}[/]",
                    latency=f"{latency_ms:.1f}"
                )

                if args.delay > 0:
                    time.sleep(args.delay)

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping due to user interrupt[/]")
    except Exception as e:
        console.print(f"[bold red]Fatal Error:[/bold red] {e}")
    finally:
        # Print Summary Table
        print_summary(args, success_count, fail_count, min_latency, max_latency, total_latency_ms)


def print_summary(args, success, fail, min_lat, max_lat, total_lat):
    """Prints a pretty summary table of the run statistics."""
    total = success + fail
    avg_lat = (total_lat / total) if total > 0 else 0.0

    table = Table(title="Telemetry Run Summary", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("Total Messages", str(total))
    table.add_row("Success", f"[green]{success}[/]")
    table.add_row("Failed", f"[red]{fail}[/]")
    table.add_row("Avg Latency", f"{avg_lat:.2f} ms")
    table.add_row("Min Latency", f"{min_lat:.2f} ms")
    table.add_row("Max Latency", f"{max_lat:.2f} ms")
    table.add_row("Target Host", f"{args.host}:{args.port}")

    console.print("\n")
    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="PANOSETI Telemetry CLI Data Generator")
    parser.add_argument("--host", default="localhost", help="gRPC Server Host")
    parser.add_argument("--port", type=int, default=50051, help="gRPC Server Port")
    parser.add_argument("--type", choices=['test', 'gnss', 'dew', 'flex', 'mixed'], default='test',
                        help="Type of payload to send. 'mixed' cycles through all.")
    parser.add_argument("--count", type=int, default=10, help="Number of messages to send")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between messages (seconds)")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])

    args = parser.parse_args()
    setup_logging(args.log_level)
    run_sender(args)


if __name__ == "__main__":
    main()