# --- Stage 1: Builder with cached deps ---
FROM python:3.9-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    VENV_PATH=/opt/venv
RUN python3 -m venv $VENV_PATH
ENV PATH="$VENV_PATH/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# If ublox_control has its own requirements, copy and install those.
# Otherwise, use the repo-wide requirements.txt as below.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# --- Stage 2: Final runtime image ---
FROM python:3.9-slim

ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

CMD ["python", "-m", "pytest", "-v", "tests/ublox_control"]
