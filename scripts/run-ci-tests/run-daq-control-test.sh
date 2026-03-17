#!/bin/bash
set -e

# Define the compose file to use
COMPOSE_FILE="tests/daq_control/docker-compose.test.yml"

echo "--- Building Daq Control CI Environment ---"
# Build the test runner image and pull database images
docker compose -f $COMPOSE_FILE build

echo "--- Running Daq Control Tests ---"
# 1. 'up': Starts the Test Runner
# 2. '--exit-code-from test_runner': If pytest fails, the script exits with error
# 3. '--abort-on-container-exit': Stops databases as soon as tests finish
docker compose -f $COMPOSE_FILE up \
    --exit-code-from test_runner \
    --abort-on-container-exit

echo "--- Cleaning Up ---"
# Ensure all containers and networks are removed
docker compose -f $COMPOSE_FILE down

echo "--- Daq Control CI Run Completed Successfully ---"