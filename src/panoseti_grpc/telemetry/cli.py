#!/usr/bin/env python3
import argparse
import time
import random
import math
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


def generate_waveforms(i):
    """
    Generates predictable waveforms for visualization.
    i: current iteration count
    """
    # Sine Wave (Period = 100 ticks)
    sine_val = 50 + 40 * math.sin(i * 0.0628)

    # Square Wave (Period = 50 ticks)
    square_val = 100 if (i % 50) < 25 else 0

    # Sawtooth (Period = 100 ticks)
    saw_val = i % 100

    return sine_val, square_val, saw_val


def generate_payload(payload_type, iteration):
    """
    Generates dummy data and selects the correct client method
    based on the Strict/Experimental policy.
    """
    # Use fixed device IDs so Grafana panels are stable
    device_id_prod = "cli_prod_01"
    device_id_flex = "cli_flex_01"

    sine, square, saw = generate_waveforms(iteration)

    # --- PRODUCTION TYPES (Strict) ---
    if payload_type == "test":
        # Uses specific log_test method for CI
        # Maps to 'metadata' DB
        return "log_test", {
            "device_id": device_id_prod,
            "iteration": iteration,
            "value": sine,  # Graph this!
            "message": "MSG_OK",
            "active": True
        }

    elif payload_type == "gnss":
        return "log_strict", {
            "device_type": "gnss",
            "device_id": device_id_prod,
            "data": {
                "satellites": int(8 + 4 * math.sin(iteration * 0.1)),
                "lat": 37.338 + (0.001 * sine / 100),
                "lon": -121.88 + (0.001 * square / 100),
                "fix_mode": "3D",
                "extra_data": {"hdop": 1.0 + (saw / 100)}
            }
        }

    elif payload_type == "dew":
        return "log_strict", {
            "device_type": "dew",
            "device_id": device_id_prod,
            "data": {
                "temp_c": 20 + (sine / 10),  # 20C +/- 4C
                "humidity": 50 + (square / 4)  # 50% or 75%
            }
        }

    # --- EXPERIMENTAL TYPES (Flexible) ---
    elif payload_type == "flex":
        # Uses log_flexible (No validation, TTL enforced)
        # Maps to 'dev_metadata' DB
        return "log_flexible", {
            "device_type": "test_flex",
            "device_id": device_id_flex,
            "data": {
                "cpu_load": saw,  # 0-100 Ramp
                "fan_rpm": 2000 + (sine * 20),  # Varying RPM
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
                logger.debug(f"Payload #{i} ({method_name}): {kwargs}")

                start_time = time.perf_counter()
                try:
                    # Dynamic dispatch to client methods
                    method = getattr(client, method_name)
                    method(**kwargs)

                    # If we get here, call was successful
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
    parser.add_argument("--type", choices=['test', 'gnss', 'dew', 'flex', 'mixed'], default='mixed',
                        help="Type of payload to send. 'mixed' cycles through all.")
    parser.add_argument("--count", type=int, default=1000, help="Number of messages to send")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between messages (seconds)")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])

    args = parser.parse_args()
    setup_logging(args.log_level)
    run_sender(args)


if __name__ == "__main__":
    main()