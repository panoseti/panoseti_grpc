# --- Stage 1: Builder with cached deps ---
FROM python:3.9-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    VENV_PATH=/opt/venv
RUN python3 -m venv $VENV_PATH
ENV PATH="$VENV_PATH/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential \
 && rm -rf /var/lib/apt/lists/*

# 1) Install dependencies and build the panoseti-grpc package
WORKDIR /app

# Install test dependencies
RUN pip install pytest pytest-asyncio

# Copy relevant files for panoseti-grpc package
COPY pyproject.toml LICENSE README.md /app/
COPY src src/

# Build the panoseti-grpc package from the current repo
RUN pip install .

# Copy test directory
COPY tests /app/tests

# --- Stage 2: Final runtime image ---
FROM python:3.9-slim

ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

#COPY src/panoseti_grpc/ublox_control/config /app/ublox_control/config
CMD ["python", "-m", "pytest", "-v", "tests/ublox_control"]
