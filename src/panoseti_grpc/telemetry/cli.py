#!/usr/bin/env python3
import argparse
import time
import random
import numpy as np
import math
import logging
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.table import Table

from panoseti_grpc.telemetry.client import TelemetryClient
from panoseti_grpc.telemetry.logging import get_logger

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


class SimulatedException(Exception):
    pass


def generate_logs(logger_instance, count, delay):
    """
    Generates a realistic stream of telescope observatory logs.
    Includes structured metadata, state transitions, and simulated stack traces.
    """

    # 1. Define Narrative Scenarios
    components = ["DOMECTRL", "CAM_04", "CAM_05", "COOLING", "GNSS_MAIN"]

    # Probability weights for log levels: mostly INFO, some DEBUG, rare ERROR
    levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
    weights = [0.3, 0.5, 0.19, 0.005, 0.005]

    # structured data generators
    def get_cooling_data(i):
        # Temperature drops over time then stabilizes
        temp = max(-20, 25 - (i * 0.5)) + random.uniform(-0.5, 0.5)
        return "Cooling system active", {"temp_c": round(temp, 2), "power_pct": 85 if temp > -19 else 40}

    def get_dome_data(i):
        az = (i * 10) % 360
        return f"Dome rotating to azimuth {az}", {"azimuth": az, "motor_current": round(random.uniform(2.0, 2.5), 2)}

    def get_observation_data(i):
        return "Frame captured", {"exposure_ms": 100, "gain": 200, "mean_adu": int(random.gauss(1400, 50))}

    console.print(f"[bold green]Starting Log Simulation ({count} events)...[/]")


    for i in range(count):
        # Pick a random component and severity
        comp = random.choice(components)
        lvl = random.choices(levels, weights)[0]

        # Default payload
        msg = f"Routine health check for {comp}"
        extra = {"component": comp, "iteration": i}

        # --- Scenario Logic ---
        if comp == "COOLING":
            msg, data = get_cooling_data(i % 50)  # reset cycle every 50
            extra.update(data)

        elif comp == "DOMECTRL":
            msg, data = get_dome_data(i)
            extra.update(data)

        elif comp.startswith("CAM") and lvl == logging.INFO:
            msg, data = get_observation_data(i)
            extra.update(data)

        # --- Chaos Engineering (Simulate Errors) ---
        if lvl >= logging.ERROR:
            try:
                # Raise a fake exception to generate a real stack trace
                if random.random() > 0.999:
                    x = 1 / 0
                else:
                    raise SimulatedException(f"Hardware timeout on {comp}")
            except Exception:
                # logger.exception automatically attaches the traceback
                logger_instance.exception(f"CRITICAL FAILURE in {comp}", extra=extra)
        else:
            # Standard log with structured context
            logger_instance.log(lvl, msg, extra=extra)

        # Add jitter to the delay for realism
        time.sleep(delay * random.uniform(0.5, 1.5))


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
    parser.add_argument("--type", choices=['test', 'gnss', 'dew', 'flex', 'mixed', 'log'],
                        default='mixed', help="Type of payload to send.")
    parser.add_argument("--count", type=int, default=1000, help="Number of messages to send")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between messages (seconds)")
    parser.add_argument("--log-level", default="debug", choices=["debug", "info", "warning", "error"])

    args = parser.parse_args()

    if args.type == 'log':
        # Create a specific logger hooked to gRPC
        # We assume the CLI is running on a 'client' machine talking to 'host'
        level = getattr(logging, args.log_level.upper())
        grpc_logger = get_logger(
            "CLI_TESTER",
            level=level,
            grpc_enabled=True,
            reset=True,
        )
        generate_logs(grpc_logger, args.count, args.delay)
        return

    setup_logging(args.log_level)
    run_sender(args)


if __name__ == "__main__":
    main()