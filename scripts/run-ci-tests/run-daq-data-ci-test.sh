#!/bin/bash
set -e

# Define the name for the Docker image
IMAGE_NAME="panoseti-daq-data-ci"

echo "--- Building CI Docker Image: $IMAGE_NAME ---"
docker build -t $IMAGE_NAME -f tests/daq_data/Dockerfile .

echo "--- Running Integration Tests ---"
# Run the tests inside the container.
# The --rm flag ensures the container is removed after the test run.
docker run --rm \
    $IMAGE_NAME \
    python3 -m pytest -v -s --maxfail=2 tests/daq_data/

echo "--- CI Test Run Completed Successfully ---"