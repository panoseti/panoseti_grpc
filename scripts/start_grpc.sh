#!/bin/bash
# Launches the unified pseti-grpc server.
# Profile is selected by PSETI_GRPC_PROFILE (default: daq_node).
# Run as the user whose Python environment has panoseti_grpc installed.
#
# Bind port: NOT read from a fixed env var here -- the role-scoped var
# (HEADNODE_GRPC_PORT for headnode/gateway profiles, DAQNODE_GRPC_PORT for
# daq_node/default) is passed to `pseti-grpc server` via --port-env, which
# resolves it at startup (see unified_main.py's resolve_bind_port()). This
# keeps the bare-metal path in sync with the docker-compose path, which
# passes the same --port-env argument via each compose file's `command:`.
#
# Systemd's Environment=/EnvironmentFile= directives (see
# setup_panoseti_grpc.sh) are how HEADNODE_GRPC_PORT/DAQNODE_GRPC_PORT
# actually reach this script's process environment; `pseti admin deploy
# --mode bare-metal` writes/updates that EnvironmentFile over SSH so a
# `.env` port change propagates here without hand-editing the unit file.

PROFILE="${PSETI_GRPC_PROFILE:-daq_node}"

case "$PROFILE" in
    headnode|gateway) PORT_ENV="HEADNODE_GRPC_PORT" ;;
    *)                PORT_ENV="DAQNODE_GRPC_PORT" ;;
esac

exec pseti-grpc server --profile "$PROFILE" --port-env "$PORT_ENV"
