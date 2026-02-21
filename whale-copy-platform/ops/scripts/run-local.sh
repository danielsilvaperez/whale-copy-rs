#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.env"
  set +a
fi

cd "$ROOT_DIR/engine-rs"
cargo run &
ENGINE_PID=$!

cleanup() {
  kill "$ENGINE_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cd "$ROOT_DIR/control-bot-ts"
npm run build
node dist/main.js
