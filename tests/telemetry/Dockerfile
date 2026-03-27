# Stage 1: Builder (Compiles dependencies)
FROM python:3.14-slim AS builder
ENV VENV_PATH=/opt/venv
RUN python3 -m venv $VENV_PATH
ENV PATH="$VENV_PATH/bin:$PATH"

RUN apt-get update && apt-get install -y gcc build-essential && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml LICENSE README.md /app/
COPY src ./src/
# Install package + dependencies
RUN pip install .

# Stage 2: Test Runner (Includes Pytest, Redis client, Influx client)
FROM python:3.14-slim AS runner
ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/src /app/src
# Install testing libs
RUN pip install pytest pytest-asyncio pytest-timeout redis influxdb
COPY src/panoseti_grpc/telemetry/telemetry_config.toml .
COPY tests ./tests
#CMD ["python", "-m", "pytest", "-v", "tests/telemetry", "-k", "test_logger", "--asyncio-mode=auto", "--asyncio-debug"]
CMD ["python", "-m", "pytest", "-v", "tests/telemetry"]

# --- NEW STAGE ---
# Stage 3: Minimal Client (Workers)
FROM python:3.14-slim AS client
ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"
WORKDIR /app
# Copy installed package from builder
COPY --from=builder /opt/venv /opt/venv
# Copy source for reference (optional, but good for tracebacks)
COPY --from=builder /app/src /app/src
# Copy ONLY the worker script
COPY tests/telemetry/dummy_worker.py ./dummy_worker.py
# Default Entrypoint
CMD ["python", "dummy_worker.py"]