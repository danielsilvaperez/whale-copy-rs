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

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    echo "Missing required env var: $key" >&2
    exit 1
  fi
}

require_cmd cargo
require_cmd npm
require_cmd node

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing $ROOT_DIR/.env. Create it from $ROOT_DIR/.env.example first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ROOT_DIR/.env"
set +a

require_env TELEGRAM_BOT_TOKEN
require_env TELEGRAM_ALLOWED_CHAT_IDS

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
