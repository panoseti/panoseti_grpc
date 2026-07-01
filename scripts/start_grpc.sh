#!/bin/bash
# Launches the unified pseti-grpc server.
# Profile is selected by PSETI_GRPC_PROFILE (default: daq_node).
# Run as the user whose Python environment has panoseti_grpc installed.

PROFILE="${PSETI_GRPC_PROFILE:-daq_node}"

exec pseti-grpc server --profile "$PROFILE"
