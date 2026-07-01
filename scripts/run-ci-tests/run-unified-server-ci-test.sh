#!/bin/bash
set -e

COMPOSE_FILE="tests/unified_server/docker-compose.test.yml"

echo "--- Building Unified Server CI Environment ---"
docker compose -f $COMPOSE_FILE build

echo "--- Running Unified Server Tests ---"
docker compose -f $COMPOSE_FILE up \
    --build \
    --exit-code-from test_runner \
    --abort-on-container-exit

echo "--- Cleaning Up ---"
docker compose -f $COMPOSE_FILE down --volumes --remove-orphans

echo "--- Unified Server CI Run Completed Successfully ---"
