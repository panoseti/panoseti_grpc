#!/bin/bash
set -e

# Define the name for the Docker image
IMAGE_NAME="panoseti-daq-ci"

echo "--- Building CI Docker Image: $IMAGE_NAME ---"
docker --debug build -t $IMAGE_NAME -f tests/daq_data_hashpipe/Dockerfile .

echo "--- Running Integration Tests ---"
# Run the tests inside the container.
# The RUN_REAL_DATA_TESTS=1 environment variable enables the fixture.
# The --rm flag ensures the container is removed after the test run.
docker run --rm -e RUN_REAL_DATA_TESTS=1 $IMAGE_NAME \
    python3 -m pytest -v tests/daq_data_hashpipe/

echo "--- CI Test Run Completed Successfully ---"
