#!/bin/bash
# Installs panoseti_grpc.service and (optionally) panoseti_alloy.service.
# Usage: setup_panoseti_grpc.sh [--alloy] [--no-alloy]
#   --alloy     Also install panoseti_alloy.service (default)
#   --no-alloy  Skip the Alloy unit

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_SCRIPT="$SCRIPT_DIR/start_grpc.sh"
ALLOY_CONFIG="$SCRIPT_DIR/../deploy/alloy/config.alloy"
USER="$(whoami)"
INSTALL_ALLOY=true

for arg in "$@"; do
    case "$arg" in
        --alloy)    INSTALL_ALLOY=true ;;
        --no-alloy) INSTALL_ALLOY=false ;;
    esac
done

chmod +x "$START_SCRIPT"

# --- panoseti_grpc.service ---
GRPC_UNIT="/etc/systemd/system/panoseti_grpc.service"
sudo bash -c "cat > $GRPC_UNIT" <<EOL
[Unit]
Description=PANOSETI Unified gRPC Server
After=network.target

[Service]
Type=simple
User=$USER
ExecStart=$START_SCRIPT
Restart=on-failure
RestartSec=5
Environment=PSETI_LOGS=/var/log/panoseti

[Install]
WantedBy=multi-user.target
EOL

sudo systemctl daemon-reload
sudo systemctl enable panoseti_grpc
sudo systemctl restart panoseti_grpc
echo "panoseti_grpc.service installed and started."

# --- panoseti_alloy.service (optional) ---
if [ "$INSTALL_ALLOY" = true ]; then
    ALLOY_UNIT="/etc/systemd/system/panoseti_alloy.service"
    ALLOY_CFG_DEST="/etc/alloy/config.alloy"

    sudo mkdir -p /etc/alloy
    sudo cp "$ALLOY_CONFIG" "$ALLOY_CFG_DEST"

    sudo bash -c "cat > $ALLOY_UNIT" <<EOL
[Unit]
Description=Grafana Alloy — PANOSETI log shipper
After=network.target panoseti_grpc.service

[Service]
Type=simple
User=root
ExecStart=/usr/bin/alloy run $ALLOY_CFG_DEST
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOL

    sudo systemctl daemon-reload
    sudo systemctl enable panoseti_alloy
    sudo systemctl restart panoseti_alloy
    echo "panoseti_alloy.service installed and started (config: $ALLOY_CFG_DEST)."
fi

echo ""
echo "Setup complete. Service status:"
sudo systemctl status panoseti_grpc --no-pager
if [ "$INSTALL_ALLOY" = true ]; then
    sudo systemctl status panoseti_alloy --no-pager
fi
