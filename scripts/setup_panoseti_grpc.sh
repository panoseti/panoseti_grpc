#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/start_grpc.sh"
SERVICE_NAME="panoseti_grpc_daemon"
USER="$(whoami)"  

# make sure the script is exectuable
chmod +x "$SCRIPT_PATH"

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

# create service file
sudo bash -c "cat > $SERVICE_FILE" <<EOL
[Unit]
Description=GRPC DAQ Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$SCRIPT_DIR
ExecStart=$SCRIPT_PATH
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOL

# start service
sudo systemctl daemon-reload

sudo systemctl enable $SERVICE_NAME

sudo systemctl start $SERVICE_NAME

echo "Service '$SERVICE_NAME' setup complete. Status:"
sudo systemctl status $SERVICE_NAME --no-pager
