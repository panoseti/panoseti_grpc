from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import time
from pathlib import Path
from typing import Annotated

import psutil
import typer

from panoseti_grpc.telemetry.logger import get_logger

app = typer.Typer(help="Manage and run the unified gRPC server.", no_args_is_help=True)

logger = get_logger(
    "pseti_grpc.server",
    console=True,
    log_dir=os.getenv("PSETI_LOGS", "/var/log/panoseti"),
    grpc_enabled=False,
)


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
                "Explicit bind port, highest precedence. Developer/debug use "
                "only -- hidden from --help. Regular use should rely on the "
                "PSETI_GRPC_PORT env var (both server and every `pseti-grpc` "
                "client command read it as their default), or --port-env "
                "(the HEADNODE_GRPC_PORT/DAQNODE_GRPC_PORT env vars) for "
                "role-scoped fleet deployments."
            ),
            hidden=True,
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
                "('python -m panoseti_grpc') can't drift apart again. "
                "Deployment/debug use only -- hidden from --help; regular use "
                "should rely on the PSETI_GRPC_PORT env var instead."
            ),
            hidden=True,
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
        logger.info("Registered PANOSETI gRPC services:")
        for name, descriptor in ServiceRegistry.all().items():
            tag = "  [DEPRECATED]" if descriptor.deprecated else ""
            logger.info(f"  {name}{tag}")
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
    logger.info(
        f"Binding port {cfg.port} (--port={port} --port-env={port_env} -> "
        f"{os.getenv(port_env) if port_env else None})"
    )

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(PanosetiServer.run(cfg))


def _is_process_alive(pid: int) -> bool:
    try:
        return bool(psutil.Process(pid).status() != psutil.STATUS_ZOMBIE)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def _find_server_processes() -> list[int]:
    """PIDs of running `pseti-grpc server` processes on this host, excluding self.

    Matches cmdline containing a `pseti-grpc` arg (basename match, tolerates
    absolute paths to the console-script wrapper) AND a distinct `server` arg.
    Self-pid exclusion (not string matching on "stop") is what stops this
    `stop` invocation's own process -- whose cmdline also contains
    `pseti-grpc` -- from matching itself; mirrors
    daq_control/server.py::_get_pids_by_name.

    Known limitation: does not match `python -m panoseti_grpc`
    (unified_main.py) -- that entry point's cmdline is
    ['python3', '-m', 'panoseti_grpc', ...], with no 'pseti-grpc' or 'server'
    token, so it's invisible to this heuristic.
    """
    my_pid = os.getpid()
    pids: list[int] = []
    for proc in psutil.process_iter(["pid", "cmdline", "status"]):
        try:
            pid = proc.info["pid"]
            if pid == my_pid or proc.info["status"] == psutil.STATUS_ZOMBIE:
                continue
            cmdline = proc.info["cmdline"] or []
            basenames = [arg.split("/")[-1] for arg in cmdline]
            if "pseti-grpc" in basenames and "server" in cmdline:
                pids.append(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return pids


@app.command("stop")
def stop(
    grace_period: Annotated[
        float,
        typer.Option(
            "--grace-period",
            help=(
                "Seconds to wait after SIGTERM before escalating to SIGKILL. "
                "Matches PanosetiServerConfig.shutdown_grace_period's default."
            ),
        ),
    ] = 5.0,
) -> None:
    """Stop a running `pseti-grpc server` process on this host, if any.

    Sends SIGTERM -- the signal PanosetiServer.run() already handles via its
    shutdown_event path for graceful teardown -- waits up to --grace-period
    seconds, then escalates to SIGKILL for any survivor. If the process is
    owned by a different user, permission is denied and reported instead of
    raising.
    """
    pids = _find_server_processes()
    if not pids:
        logger.info("No pseti-grpc server process is running.")
        return

    logger.info(f"Found {len(pids)} pseti-grpc server process(es): {pids}")

    remaining: list[int] = []
    permission_denied: list[int] = []
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            remaining.append(pid)
        except ProcessLookupError:
            continue
        except PermissionError:
            permission_denied.append(pid)

    poll_interval = 0.2
    deadline = time.monotonic() + grace_period
    while remaining and time.monotonic() < deadline:
        time.sleep(poll_interval)
        remaining = [pid for pid in remaining if _is_process_alive(pid)]

    still_alive: list[int] = []
    if remaining:
        logger.info(f"{len(remaining)} process(es) did not exit within {grace_period}s; sending SIGKILL: {remaining}")
        kill_targets: list[int] = []
        for pid in remaining:
            try:
                os.kill(pid, signal.SIGKILL)
                kill_targets.append(pid)
            except ProcessLookupError:
                continue
            except PermissionError:
                permission_denied.append(pid)
        time.sleep(0.5)
        still_alive = [pid for pid in kill_targets if _is_process_alive(pid)]

    stopped_count = len(pids) - len(still_alive) - len(permission_denied)
    if stopped_count > 0:
        logger.info(f"Stopped {stopped_count} pseti-grpc server process(es).")

    if permission_denied:
        logger.warning(f"Permission denied for process(es) {permission_denied}.")
        logger.warning("Likely started by a different user.")
        logger.warning("Ask that user to stop it, or re-run `pseti-grpc server stop`")
        logger.warning("with sufficient privileges (e.g. sudo, or as the owning user).")

    if still_alive:
        logger.info(f"Failed to stop process(es): {still_alive}")

    if permission_denied or still_alive:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
