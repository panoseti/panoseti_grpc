# --- Stage 1: Builder with cached deps ---
FROM python:3.9-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    VENV_PATH=/opt/venv
# Create and use a virtualenv to isolate Python deps
RUN python3 -m venv $VENV_PATH
ENV PATH="$VENV_PATH/bin:$PATH"

# System build deps only for wheels that may need compiling (kept out of final image)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential \
 && rm -rf /var/lib/apt/lists/*

# 1) Cache-friendly install of Python deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Copy only the parts of the repo needed for these tests
# Doing this AFTER deps preserves the cached pip layer for code-only changes
COPY . .

# --- Stage 2: Final runtime image (clean) ---
FROM python:3.9-slim

ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"
WORKDIR /app

# Copy the virtualenv (preinstalled deps) and app code from builder
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

# Optional: set environment to speed up Python in containers
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Default test command for daq_data suite
CMD ["python", "-m", "pytest", "-v", "tests/daq_data"]