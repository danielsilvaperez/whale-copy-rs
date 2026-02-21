#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="${ENGINE_LOG_DIR:-/var/log/whale-copy-platform}"
RETENTION_DAYS="${LOG_RETENTION_DAYS:-7}"

find "$LOG_DIR" -type f -name '*engine.jsonl*' -mtime "+${RETENTION_DAYS}" -delete
