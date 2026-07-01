#!/usr/bin/env python3

"""Runs protoc with the gRPC plugin to generate messages and gRPC stubs."""

import os
import re
import subprocess
import sys

# Paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROTOS_DIR = os.path.join(ROOT_DIR, "protos")
OUT_DIR = os.path.join(ROOT_DIR, "src", "panoseti_grpc", "generated")

print(f"{ROOT_DIR=}, {PROTOS_DIR=}, {OUT_DIR=}")
# sys.exit()


def compile_protos() -> None:
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)
        # Create __init__.py so Python treats it as a package
        with open(os.path.join(OUT_DIR, "__init__.py"), "w") as f:
            f.write("# Auto-generated gRPC code\n")

    proto_files = [f for f in os.listdir(PROTOS_DIR) if f.endswith(".proto")]

    for proto in proto_files:
        print(f"Compiling {proto}...")
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{PROTOS_DIR}",
                f"--python_out={OUT_DIR}",
                f"--grpc_python_out={OUT_DIR}",
                f"--mypy_out={OUT_DIR}",
                f"--mypy_grpc_out={OUT_DIR}",
                os.path.join(PROTOS_DIR, proto),
            ]
        )

    fix_relative_imports()


def fix_relative_imports() -> None:
    """
    Patches generated _pb2.py and .pyi files to use relative imports.
    Changes 'import daq_data_pb2' -> 'from . import daq_data_pb2'
    """
    print("Patching relative imports...")
    for filename in os.listdir(OUT_DIR):
        if filename.endswith(".py") or filename.endswith(".pyi"):
            filepath = os.path.join(OUT_DIR, filename)
            with open(filepath) as f:
                content = f.read()

            # Regex to find standard proto imports and make them relative
            # Looks for: import [name]_pb2 as [alias]
            # Replaces with: from . import [name]_pb2 as [alias]
            new_content = re.sub(r"^import (\w+_pb2)", r"from . import \1", content, flags=re.MULTILINE)

            if new_content != content:
                with open(filepath, "w") as f:
                    f.write(new_content)
    print("Done.")


if __name__ == "__main__":
    compile_protos()
