import pytest
import pytest_asyncio
import asyncio
import json
import logging
from pathlib import Path
import os
import urllib.parse
import uuid
import copy
from typing import Optional, Tuple

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict

TEST_CFG_DIR = Path("tests/ublox_control/config")
TEST_CFG_DIR.mkdir(exist_ok=True)


@pytest.fixture(scope="session")
def server_config_base():
    """Provides a base server configuration dictionary."""
    with open(TEST_CFG_DIR / "ublox_control_server_config.json", "r") as f:
        cfg = json.load(f)
    return cfg

