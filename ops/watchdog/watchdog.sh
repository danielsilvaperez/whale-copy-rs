#!/usr/bin/env bash
set -euo pipefail

ENGINE_SERVICE="${ENGINE_SERVICE:-whale-copy-engine.service}"
TELEGRAM_SERVICE="${TELEGRAM_SERVICE:-whale-copy-telegram.service}"
ENGINE_SOCKET_PATH="${ENGINE_SOCKET_PATH:-/tmp/whale-copy-engine.sock}"
ENGINE_DB_PATH="${ENGINE_DB_PATH:-/var/lib/whale-copy-platform/whale_copy_platform.db}"
WATCHDOG_INTERVAL_SEC="${WATCHDOG_INTERVAL_SEC:-12}"
ENGINE_HEARTBEAT_MAX_AGE_SEC="${ENGINE_HEARTBEAT_MAX_AGE_SEC:-30}"
TELEGRAM_HEARTBEAT_MAX_AGE_SEC="${TELEGRAM_HEARTBEAT_MAX_AGE_SEC:-40}"
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

send_telegram_alert() {
  local message="$1"
  if [[ -z "$TELEGRAM_BOT_TOKEN" || -z "$TELEGRAM_CHAT_ID" ]]; then
    return 0
  fi

  local escaped_message
  escaped_message=$(printf '%s' "$message" | jq -Rs .)
  
  curl -sS --max-time 6 \
    -H 'content-type: application/json' \
    -d "{\"chat_id\":\"${TELEGRAM_CHAT_ID}\",\"text\":${escaped_message}}" \
    "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" >/dev/null || true
}

service_active() {
  systemctl is-active --quiet "$1"
}

restart_service() {
  local service_name="$1"
  local reason="$2"

  log "restarting ${service_name}: ${reason}"
  systemctl restart "$service_name"
  send_telegram_alert "[watchdog] restarted ${service_name}: ${reason}"
}

ping_engine() {
  python3 - "$ENGINE_SOCKET_PATH" <<'PY'
import json
import socket
import sys

sock_path = sys.argv[1]
try:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(2.5)
    sock.connect(sock_path)
    sock.sendall(b'{"method":"ping","params":{}}\n')
    data = sock.recv(4096)
    sock.close()
    if not data:
        raise RuntimeError("empty ping response")
    payload = json.loads(data.decode().splitlines()[0])
    if not payload.get("ok"):
        raise RuntimeError(payload.get("error", "ping failed"))
except Exception as exc:
    print(str(exc), file=sys.stderr)
    sys.exit(1)
PY
}

heartbeat_is_fresh() {
  local service_name="$1"
  local max_age="$2"

  if ! command -v sqlite3 >/dev/null 2>&1; then
    return 0
  fi

  if [[ ! -f "$ENGINE_DB_PATH" ]]; then
    return 1
  fi

  local age
  age="$(sqlite3 "$ENGINE_DB_PATH" "SELECT CAST(strftime('%s','now') - strftime('%s', updated_at) AS INTEGER) FROM service_heartbeats WHERE service=? LIMIT 1;" "${service_name}" 2>/dev/null || true)"

  if [[ -z "$age" ]]; then
    return 1
  fi

  if [[ "$age" =~ ^[0-9]+$ ]] && (( age <= max_age )); then
    return 0
  fi

  return 1
}

main_loop() {
  log "watchdog started interval=${WATCHDOG_INTERVAL_SEC}s"

  while true; do
    if ! service_active "$ENGINE_SERVICE"; then
      restart_service "$ENGINE_SERVICE" "service inactive"
    fi

    if ! ping_engine; then
      restart_service "$ENGINE_SERVICE" "rpc ping failure"
    fi

    if ! heartbeat_is_fresh "engine" "$ENGINE_HEARTBEAT_MAX_AGE_SEC"; then
      restart_service "$ENGINE_SERVICE" "engine heartbeat stale"
    fi

    if ! service_active "$TELEGRAM_SERVICE"; then
      restart_service "$TELEGRAM_SERVICE" "service inactive"
    fi

    if ! heartbeat_is_fresh "telegram" "$TELEGRAM_HEARTBEAT_MAX_AGE_SEC"; then
      restart_service "$TELEGRAM_SERVICE" "telegram heartbeat stale"
    fi

    sleep "$WATCHDOG_INTERVAL_SEC"
  done
}

main_loop
