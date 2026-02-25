#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

sudo cp "$ROOT_DIR/ops/systemd/whale-copy-engine.service" /etc/systemd/system/
sudo cp "$ROOT_DIR/ops/systemd/whale-copy-telegram.service" /etc/systemd/system/
sudo cp "$ROOT_DIR/ops/systemd/whale-copy-watchdog.service" /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable whale-copy-engine.service
sudo systemctl enable whale-copy-telegram.service
sudo systemctl enable whale-copy-watchdog.service

echo "systemd units installed and enabled"
