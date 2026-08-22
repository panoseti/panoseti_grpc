"""
.env loading for the ``pseti-grpc`` CLI.

Mirrors ``panoseti``'s ``control.utils.env_loader`` so both CLIs behave the
same way: a plain ``KEY=value`` ``.env`` file (no ``export``) is loaded
straight into ``os.environ`` via ``python-dotenv``, not by shelling out to
``source`` -- so operators don't have to remember that a bare ``KEY=value``
line, sourced in bash, only sets a shell-local variable and never reaches a
child process's environment.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


def _resolve_env_path() -> Path | None:
    """Return the .env file that will be (or was) loaded, or None if not found."""
    env_file = os.getenv("PSETI_GRPC_ENV_FILE")
    if env_file:
        p = Path(env_file)
        return p if p.is_file() else None
    default = Path(".env")
    return default if default.is_file() else None


def load_pseti_grpc_env() -> None:
    """Load environment variables from a .env file into os.environ.

    If ``PSETI_GRPC_ENV_FILE`` is set, loads that specific file. Otherwise
    looks for a ``.env`` file in the current working directory. Variables
    loaded this way overwrite any existing ones in ``os.environ``
    (``override=True``), matching ``panoseti``'s ``load_pseti_env()``.
    """
    env_path = _resolve_env_path()
    if env_path:
        load_dotenv(dotenv_path=env_path, override=True)
