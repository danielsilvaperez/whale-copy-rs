#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
}

require_cmd cargo
require_cmd npm

cd "$ROOT_DIR/engine-rs"
cargo build --release

cd "$ROOT_DIR/control-bot-ts"
npm ci
npm run build

echo "build complete"
