#!/usr/bin/env python3
"""
qa.py — PSETI Unified QA Runner

Refactored to Typer for integration into pseti-grpc CLI.
"""

import asyncio
import sys
import time
import tomllib
from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from panoseti_grpc.util.cli import display_tree_callback


app = typer.Typer(help="PSETI Unified QA Runner", no_args_is_help=True)


class C:
    """ANSI colour helpers. Each static method wraps a string in the colour escape."""

    _GREEN = "\033[92m"
    _RED = "\033[91m"
    _YELLOW = "\033[93m"
    _CYAN = "\033[96m"
    _BOLD = "\033[1m"
    _DIM = "\033[2m"
    _RESET = "\033[0m"

    @staticmethod
    def green(s: str) -> str:
        return f"{C._GREEN}{s}{C._RESET}"

    @staticmethod
    def red(s: str) -> str:
        return f"{C._RED}{s}{C._RESET}"

    @staticmethod
    def yellow(s: str) -> str:
        return f"{C._YELLOW}{s}{C._RESET}"

    @staticmethod
    def cyan(s: str) -> str:
        return f"{C._CYAN}{s}{C._RESET}"

    @staticmethod
    def bold(s: str) -> str:
        return f"{C._BOLD}{s}{C._RESET}"

    @staticmethod
    def dim(s: str) -> str:
        return f"{C._DIM}{s}{C._RESET}"

    @staticmethod
    def paint(s: str, code: str) -> str:
        return f"{code}{s}{C._RESET}"


# Colorwheel used to assign a distinct hue to each parallel task.
PALETTE = [
    "\033[38;5;81m",  # sky blue
    "\033[38;5;118m",  # lime green
    "\033[38;5;214m",  # orange
    "\033[38;5;207m",  # pink / magenta
    "\033[38;5;147m",  # soft purple
    "\033[38;5;43m",  # teal
    "\033[38;5;220m",  # gold
    "\033[38;5;203m",  # coral / salmon
]


class Result:
    """Outcome of a single QA task."""

    __slots__ = ("code", "elapsed", "name")

    def __init__(self, name: str, code: int, elapsed: float) -> None:
        self.name = name
        self.code = code
        self.elapsed = elapsed

    @property
    def ok(self) -> bool:
        return self.code == 0


QA_TOML_PATH = Path(__file__).parent / "qa.toml"


class QARunner:
    """Loads qa.toml and drives linting / testing tasks."""

    def __init__(self, config_path: Path) -> None:
        try:
            with open(config_path, "rb") as fh:
                self._cfg: dict[str, Any] = tomllib.load(fh)
        except FileNotFoundError:
            print(C.red(f"Error: {config_path} not found."), file=sys.stderr)
            sys.exit(1)
        self._settings: dict[str, Any] = self._cfg.get("settings", {})

    # ── config accessors ──────────────────────────────────────────────────────

    def lint_tasks(self, target: str | None) -> dict[str, str]:
        cfg: dict[str, Any] = self._cfg.get("lint", {})
        if target and target != "all":
            return {f"lint.{k}": str(v["command"]) for k, v in cfg.items() if target in k}
        return {f"lint.{k}": str(v["command"]) for k, v in cfg.items()}

    def lint_descriptions(self, target: str | None) -> dict[str, str]:
        cfg: dict[str, Any] = self._cfg.get("lint", {})
        if target and target != "all":
            return {f"lint.{k}": str(v.get("description", "")) for k, v in cfg.items() if target in k}
        return {f"lint.{k}": str(v.get("description", "")) for k, v in cfg.items()}

    def test_tasks(self, kind: str) -> dict[str, str]:
        cfg: dict[str, Any] = self._cfg.get("test", {})
        if kind not in cfg:
            return {}
        return {f"test.{kind}": str(cfg[kind]["command"])}

    def test_description(self, kind: str) -> str:
        test_cfg: dict[str, Any] = self._cfg.get("test", {})
        entry: dict[str, Any] = test_cfg.get(kind, {})
        return str(entry.get("description", ""))

    # ── streaming core ────────────────────────────────────────────────────────

    @staticmethod
    async def _stream(
        name: str,
        cmd: str,
        lock: asyncio.Lock,
        tag: str = "",
    ) -> Result:
        start = time.monotonic()
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert proc.stdout is not None

        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip()
            async with lock:
                print(f"{tag}{line}", flush=True)

        await proc.wait()
        return Result(name, proc.returncode or 0, time.monotonic() - start)

    # ── output helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _header(title: str) -> None:
        bar = "─" * 60
        print(f"\n{C.bold(C.yellow(bar))}", flush=True)
        print(f"{C.bold(C.yellow(f'  {title}'))}", flush=True)
        print(f"{C.bold(C.yellow(bar))}", flush=True)

    @staticmethod
    def _task_line(name: str, desc: str, cmd: str) -> None:
        print(f"  {C.cyan(f'[{name}]')}  {desc}", flush=True)
        print(f"  {C.dim(cmd)}", flush=True)

    @staticmethod
    def _summary(
        results: list[Result],
        colors: dict[str, str] | None = None,
    ) -> None:
        if not results:
            return
        width = max(len(r.name) for r in results)
        print(f"\n{C.bold('Results')}", flush=True)
        for r in results:
            icon = C.green("✓") if r.ok else C.red("✗")
            status = C.green("passed") if r.ok else C.red("FAILED")
            code = (colors or {}).get(r.name, C._CYAN)
            name = C.paint(r.name.ljust(width), code)
            print(f"  {icon}  {name}  {status}  {C.dim(f'{r.elapsed:.1f}s')}", flush=True)

    # ── public run methods ────────────────────────────────────────────────────

    async def run_parallel(
        self,
        title: str,
        tasks: dict[str, str],
        descriptions: dict[str, str] | None = None,
    ) -> list[Result]:
        self._header(title)
        if not tasks:
            print(C.yellow("  (no tasks configured)"))
            return []

        task_colors = {name: PALETTE[i % len(PALETTE)] for i, name in enumerate(tasks)}

        descs = descriptions or {}
        for name, cmd in tasks.items():
            colored_name = C.paint(f"[{name}]", task_colors[name])
            print(f"  {colored_name}  {descs.get(name, '')}", flush=True)
            print(f"  {C.dim(cmd)}", flush=True)
        print(flush=True)

        lock = asyncio.Lock()
        results = list(
            await asyncio.gather(
                *[self._stream(n, c, lock, tag=C.paint(f"[{n}]", task_colors[n]) + " ") for n, c in tasks.items()]
            )
        )
        self._summary(results, colors=task_colors)
        return results

    async def run_sequential(
        self,
        title: str,
        tasks: dict[str, str],
        descriptions: dict[str, str] | None = None,
    ) -> list[Result]:
        self._header(title)
        if not tasks:
            print(C.yellow("  (no tasks configured)"))
            return []

        descs = descriptions or {}
        lock = asyncio.Lock()
        results: list[Result] = []

        for name, cmd in tasks.items():
            self._task_line(name, descs.get(name, ""), cmd)
            print(flush=True)
            result = await self._stream(name, cmd, lock)
            results.append(result)
            icon = C.green("✓ passed") if result.ok else C.red("✗ FAILED")
            print(f"\n{C.cyan(f'[{name}]')} {icon}  {C.dim(f'{result.elapsed:.1f}s')}", flush=True)

        return results


@app.command()
def lint(
    target: Annotated[str, typer.Argument(help="Scope to lint: 'ruff', 'mypy', or 'all'")] = "all",
) -> None:
    """Run linters (Ruff, MyPy)."""
    runner = QARunner(QA_TOML_PATH)

    async def _run() -> None:
        tasks = runner.lint_tasks(target)
        descs = runner.lint_descriptions(target)
        results = await runner.run_parallel("LINTING", tasks, descs)
        return all(r.ok for r in results)

    ok = asyncio.run(_run())
    if not ok:
        raise typer.Exit(code=1)


def register_test_suites() -> None:
    test_suites = ["daq_data", "daq_control", "telemetry", "ublox", "unified_server", "hashpipe_daq_data"]
    for suite in test_suites:

        def make_suite_cmd(s=suite) -> None:
            async def _run_suite() -> None:
                runner = QARunner(QA_TOML_PATH)
                tasks = runner.test_tasks(s)
                descs = {f"test.{s}": runner.test_description(s)}
                results = await runner.run_sequential(f"{s.upper()} TESTS", tasks, descs)
                return all(r.ok for r in results)

            ok = asyncio.run(_run_suite())
            if not ok:
                raise typer.Exit(code=1)

        app.command(name=suite, help=f"Run {suite} tests")(make_suite_cmd)


register_test_suites()


@app.command(name="all")
def run_all() -> None:
    """Run full suite: lint + all tests."""
    runner = QARunner(QA_TOML_PATH)

    async def _run_all_tasks() -> None:
        width = 60
        print(f"\n{C.bold(C.cyan('═' * width))}")
        print(f"{C.bold(C.cyan('  PSETI — Full QA Suite'))}")
        print(f"{C.bold(C.cyan('═' * width))}")

        all_results: list[Result] = []

        # 1. Linters (Parallel)
        all_results += await runner.run_parallel(
            "LINTING",
            runner.lint_tasks("all"),
            runner.lint_descriptions("all"),
        )

        # 2. Test Suites (Sequential)
        for suite in ["daq_data", "daq_control", "telemetry", "ublox", "unified_server"]:
            all_results += await runner.run_sequential(
                f"{suite.upper()} TESTS",
                runner.test_tasks(suite),
                {f"test.{suite}": runner.test_description(suite)},
            )

        print(f"\n{C.bold(C.cyan('═' * width))}")
        print(f"{C.bold(C.cyan('  Full Suite Summary'))}")
        print(f"{C.bold(C.cyan('═' * width))}")
        runner._summary(all_results)
        return all(r.ok for r in all_results)

    ok = asyncio.run(_run_all_tasks())
    if not ok:
        raise typer.Exit(code=1)


@app.callback()
def main_callback(
    ctx: typer.Context,
    tree: Annotated[bool, typer.Option("--tree", "-t", help="Display the command tree for gRPC tests.", callback=display_tree_callback)] = False
) -> None:
    """PSETI Unified QA Runner."""
    if tree:
        return
    # Ensure we are always running from the grpc/ directory root
    # so Docker/Compose paths resolve correctly.
    grpc_root = Path(__file__).parent.parent.resolve()
    import os

    os.chdir(grpc_root)


if __name__ == "__main__":
    app()
