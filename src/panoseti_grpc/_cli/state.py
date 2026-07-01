from __future__ import annotations

import os


class CliState:
    def __init__(self) -> None:
        self.host = os.getenv("HEADNODE_IP", "localhost")
        self.port = int(os.getenv("HEADNODE_GRPC_PORT", "50051"))
        self.timeout = 10.0
        self.json = False
        self.grpc_logging = False
        self.log_level = "INFO"


state = CliState()
