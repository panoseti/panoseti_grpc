# Stage 1: Build Environment for C/C++ Artifacts
# Use the final base image for the builder to ensure consistency.
FROM python:3.9-slim AS builder

LABEL stage="builder"

ENV DEBIAN_FRONTEND=noninteractive
ARG TEST_DIR="tests/daq_data_hashpipe"

# Create a fresh, architecture-native virtual environment.
ENV VENV_PATH=/opt/venv
RUN python3 -m venv $VENV_PATH
# Activate the venv by adding its bin directory to the PATH.
ENV PATH="$VENV_PATH/bin:$PATH"

# Install build-time dependencies for hashpipe
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    autoconf \
    automake \
    libtool \
    g++ \
    make && \
    rm -rf /var/lib/apt/lists/*

# --- Build and Install Hashpipe ---
WORKDIR /build
RUN git clone --depth 1 https://github.com/david-macmahon/hashpipe.git hashpipe
WORKDIR /build/hashpipe/src
RUN autoreconf -is && \
    ./configure --prefix=/usr/local && \
    make && \
    make install

# Install Python dependencies into a python venv
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# --- Build the custom panoseti_daq Plugin ---
# The compiled .so file will be in the workdir /build/panoseti_daq
WORKDIR /build
RUN git clone --depth 1 --branch uds-to-grpc https://github.com/panoseti/panoseti_daq.git panoseti_daq
WORKDIR /build/panoseti_daq
# Overwrite the Makefile with our Makefile to define small data buffers
COPY "${TEST_DIR}/Makefile" .
COPY "${TEST_DIR}/databuf.h" .

RUN make
RUN make install
# RUN ./HSD_init.sh


# Stage 2: Final Runtime and Test Environment
# This stage creates the final, lean image.
FROM python:3.9-slim

LABEL stage="runtime"
ARG TEST_DIR="tests/daq_data_hashpipe"

# Install runtime-only and build-essential dependencies
# build-essential is required for packages that compile C extensions, like netifaces.
RUN apt-get update && \
    apt-get install -y --no-install-recommends tcpreplay build-essential && \
    rm -rf /var/lib/apt/lists/*

# --- Set up the Python Environment ---
# Create a fresh, architecture-native virtual environment.
ENV VENV_PATH=/opt/venv
RUN python3 -m venv $VENV_PATH
# Activate the venv by adding its bin directory to the PATH.
ENV PATH="$VENV_PATH/bin:$PATH"

# --- Copy Artifacts and Install Dependencies ---
WORKDIR /app


# Copy application source code from the builder stage's /app directory
COPY --from=builder /app .

# Copy compiled artifacts from the builder stage
COPY --from=builder /usr/local/bin/hashpipe /usr/local/bin/
COPY --from=builder /usr/local/lib/libhashpipe.so* /usr/local/lib/
COPY --from=builder /usr/local/lib/libhashpipestatus.so* /usr/local/lib/
COPY --from=builder /build/panoseti_daq/hashpipe.so* /usr/local/lib/
COPY --from=builder /opt/venv /opt/venv

# Update the shared library cache to ensure the system finds libhashpipe.so
RUN ldconfig
RUN apt-get remove build-essential --autoremove -y

# --- Finalize ---
# Copy test data
COPY "${TEST_DIR}/PH_onsky_NGC7331_1pixTrig_Anytrig1.5pe__20250801_102213_965_filtered2.pcapng" /app/test_data.pcapng

# Set a working directory for command execution
WORKDIR /app
