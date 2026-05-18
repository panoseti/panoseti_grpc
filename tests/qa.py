#!/usr/bin/env python3
"""
qa.py — PSETI Unified QA Runner (gRPC version)

Refactored to use the modular model-driven approach from the control plane.
"""

import asyncio
import os
from pathlib import Path
from typing import Annotated

import typer

try:
    from .grpc_qa_utils import QA_TOML_PATH, TestRunner
except ImportError, ValueError:
    from grpc_qa_utils import QA_TOML_PATH, TestRunner

from panoseti_grpc.util.cli import display_tree_callback

app = typer.Typer(help="PSETI gRPC Service QA Runner", no_args_is_help=True)


@app.callback()
def main_callback(
    ctx: typer.Context,
    debug: bool = typer.Option(False, "--debug", "--no-teardown", help="Bypass container teardown for debugging."),
    no_build: bool = typer.Option(False, "--no-build", help="Do not attempt to build images, use existing ones."),
    tool: str = typer.Option("docker", "--tool", help="Container tool to use (docker or podman)."),
    tree: Annotated[
        bool,
        typer.Option("--tree", "-t", help="Display the command tree for gRPC tests.", callback=display_tree_callback),
    ] = False,
) -> None:
    """PSETI Unified QA Runner."""
    if tree:
        return
    # Ensure we are always running from the grpc/tests directory
    # so that relative paths in qa.toml resolve correctly.
    grpc_tests_root = Path(__file__).parent.resolve()
    os.chdir(grpc_tests_root)

    ctx.obj = TestRunner(QA_TOML_PATH)
    ctx.obj.no_teardown = debug
    ctx.obj.no_build = no_build
    ctx.obj.container_tool = tool


@app.command()
def lint(
    ctx: typer.Context,
    target: Annotated[str, typer.Argument(help="Scope to lint: 'ruff', 'mypy', or 'all'")] = "all",
) -> None:
    """Run linters (Ruff, MyPy)."""
    ok = asyncio.run(ctx.obj.run_suite("lint", target=target))
    if not ok:
        raise typer.Exit(code=1)


def register_test_suites() -> None:
    # Load the config temporarily to find all suites
    temp_runner = TestRunner(QA_TOML_PATH)
    for name, suite in temp_runner.cfg.suites.items():
        if name == "lint":
            continue

        def make_command(s_name=name, s_desc=suite.description):
            def _run(ctx: typer.Context) -> None:
                ok = asyncio.run(ctx.obj.run_suite(s_name))
                if not ok:
                    raise typer.Exit(code=1)

            _run.__doc__ = s_desc
            return _run

        app.command(name=name.replace("_", "-"))(make_command())


@app.command(name="all")
def run_all(ctx: typer.Context) -> None:
    """Run full suite: lint + all tests."""
    runner: TestRunner = ctx.obj

    async def _run_all() -> bool:
        success = True
        # 1. Linting
        success &= await runner.run_suite("lint")

        # 2. Sequential tests
        for name in ["daq_data", "daq_control", "telemetry", "unified_server"]:
            if name in runner.cfg.suites:
                success &= await runner.run_suite(name)
        return success

    ok = asyncio.run(_run_all())
    if not ok:
        raise typer.Exit(code=1)


# Initialize the app with dynamic commands
register_test_suites()

if __name__ == "__main__":
    app()
