"""Shared package-resource loader for all panoseti_grpc services."""

from __future__ import annotations

import importlib.resources as _resources
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO, cast


def load_package_resource(package: str, fname: str | Path, parser: Callable[[TextIO], Any] | None = None) -> Any:
    """Load a file bundled inside a Python package and parse it.

    Args:
        package: importlib anchor package name (e.g. ``'panoseti_grpc'``).
        fname:   path relative to the package root
                 (e.g. ``'daq_data/config/daq_data_server_config.json'``).
        parser:  callable that accepts an open text-mode file object and
                 returns the parsed data.  Defaults to ``json.load``.
    """
    if parser is None:
        parser = json.load
    resource_path = _resources.files(package).joinpath(fname)
    with resource_path.open("r") as f:
        # Cast f as TextIO to ensure it's compatible with the parser type hint
        return parser(cast(TextIO, f))


def load_package_json(package: str, fname: str | Path) -> dict[str, Any]:
    """Load a JSON resource file bundled inside *package*."""
    return cast(dict[str, Any], load_package_resource(package, fname, parser=json.load))
