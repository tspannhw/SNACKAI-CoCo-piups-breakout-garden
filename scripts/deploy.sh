#!/usr/bin/env bash
# =============================================================================
# PiUPS Breakout Garden - Raspberry Pi Deployment Script
# Installs systemd service for auto-start on boot
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"
SERVICE_NAME="piups-sensor"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
VENV_DIR="${PROJECT_DIR}/venv"

echo "=== PiUPS Breakout Garden Deployment ==="
echo "Project: ${PROJECT_DIR}"
echo "Service: ${SERVICE_NAME}"
echo ""

# Check we're running on a Pi
if [ ! -f /sys/devices/virtual/thermal/thermal_zone0/temp ]; then
    echo "WARNING: Not running on Raspberry Pi (no thermal zone detected)"
    echo "Continuing anyway..."
fi

# Build venv if not exists
if [ ! -d "${VENV_DIR}" ]; then
    echo "Building virtual environment..."
    cd "${PROJECT_DIR}"
    ./manage.sh build
fi

# Install systemd service
echo "Installing systemd service..."
sudo cp "${SCRIPT_DIR}/piups-sensor.service" "${SERVICE_FILE}"

# Replace placeholders in service file
sudo sed -i "s|__PROJECT_DIR__|${PROJECT_DIR}|g" "${SERVICE_FILE}"
sudo sed -i "s|__VENV_DIR__|${VENV_DIR}|g" "${SERVICE_FILE}"

# Reload systemd
sudo systemctl daemon-reload

# Enable and start
sudo systemctl enable "${SERVICE_NAME}"
sudo systemctl start "${SERVICE_NAME}"

echo ""
echo "=== Deployment Complete ==="
echo "Service status:"
sudo systemctl status "${SERVICE_NAME}" --no-pager || true
echo ""
echo "Useful commands:"
echo "  sudo systemctl status ${SERVICE_NAME}   # Check status"
echo "  sudo systemctl stop ${SERVICE_NAME}     # Stop service"
echo "  sudo systemctl restart ${SERVICE_NAME}  # Restart"
echo "  sudo journalctl -u ${SERVICE_NAME} -f   # Follow logs"
