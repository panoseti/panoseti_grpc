#!/usr/bin/env python3
"""
qa.py — PANOSETI Unified QA Runner

Output streams in real time. Parallel tasks (lint) prefix every line with
the task name so concurrent streams never mangle each other. Sequential
tasks (tests) stream without a prefix — the section header is enough.

Usage:
  python tests/qa.py lint [ruff|mypy]
  python tests/qa.py daq_data
  python tests/qa.py daq_control
  python tests/qa.py telemetry
  python tests/qa.py ublox
  python tests/qa.py unified_server
  python tests/qa.py hashpipe_daq_data
  python tests/qa.py all
"""

import argparse
import asyncio
import sys
import time
import tomllib
from pathlib import Path
from typing import Any


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
# Uses 256-colour escape codes for a wider, more distinguishable palette.
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
        """
        Spawn ``cmd``, streaming its stdout+stderr line-by-line.
        """
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
        """
        Run all tasks concurrently. Each output line is prefixed with a coloured tag.
        """
        self._header(title)
        if not tasks:
            print(C.yellow("  (no tasks configured)"))
            return []

        # Assign a unique palette colour to each task, cycling if needed.
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
        """
        Run tasks one at a time, streaming output without a line prefix.
        """
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


# ── Command handlers ───────────────────────────────────────────────────────────


async def cmd_lint(args: argparse.Namespace, runner: QARunner) -> bool:
    target = str(getattr(args, "target", "all") or "all")
    tasks = runner.lint_tasks(target)
    descs = runner.lint_descriptions(target)
    results = await runner.run_parallel("LINTING", tasks, descs)
    return all(r.ok for r in results)


async def cmd_test_suite(args: argparse.Namespace, runner: QARunner) -> bool:
    kind = args.command
    tasks = runner.test_tasks(kind)
    descs = {f"test.{kind}": runner.test_description(kind)}
    results = await runner.run_sequential(f"{kind.upper()} TESTS", tasks, descs)
    return all(r.ok for r in results)


async def cmd_all(args: argparse.Namespace, runner: QARunner) -> bool:
    width = 60
    print(f"\n{C.bold(C.cyan('═' * width))}")
    print(f"{C.bold(C.cyan('  PANOSETI — Full QA Suite'))}")
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


# ── Entry point ────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(prog="python tests/qa.py", add_help=True)
    sub = parser.add_subparsers(dest="command")

    p_lint = sub.add_parser("lint", help="Run linters (Ruff, MyPy)")
    p_lint.add_argument(
        "target",
        nargs="?",
        choices=["ruff", "mypy", "all"],
        default="all",
        help="Scope to lint (default: all)",
    )
    p_lint.set_defaults(func=cmd_lint)

    # Individual test suites
    test_suites = ["daq_data", "daq_control", "telemetry", "ublox", "unified_server", "hashpipe_daq_data"]
    for suite in test_suites:
        p_suite = sub.add_parser(suite, help=f"Run {suite} tests")
        p_suite.set_defaults(func=cmd_test_suite)

    p_all = sub.add_parser("all", help="Run full suite: lint + all tests")
    p_all.set_defaults(func=cmd_all)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    runner = QARunner(QA_TOML_PATH)

    try:
        ok = asyncio.run(args.func(args, runner))
    except KeyboardInterrupt:
        print(f"\n{C.yellow('Interrupted.')}")
        sys.exit(130)
    except Exception as exc:
        print(C.red(f"Unexpected error: {exc}"), file=sys.stderr)
        raise

    if ok:
        print(f"\n{C.bold(C.green('✓  QA passed successfully!'))}")
        sys.exit(0)
    else:
        print(f"\n{C.bold(C.red('✗  QA failed. Fix the errors above.'))}")
        sys.exit(1)


if __name__ == "__main__":
    main()
