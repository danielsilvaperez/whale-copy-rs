#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

cd "$ROOT_DIR/engine-rs"
cargo build --release

cd "$ROOT_DIR/control-bot-ts"
npm ci
npm run build

echo "build complete"
