#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

source ~/miniconda3/etc/profile.d/conda.sh

conda activate grpc-py39 || {
    echo "Failed to activate conda environment: grpc-py39"
    exit 1
}

python -m daq_data.server > daq_server.log 2>&1 
