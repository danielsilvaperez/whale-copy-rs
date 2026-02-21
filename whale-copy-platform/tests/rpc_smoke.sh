#!/usr/bin/env bash
set -euo pipefail

SOCKET_PATH="${ENGINE_SOCKET_PATH:-/tmp/whale-copy-engine.sock}"

request() {
  local payload="$1"
  python3 - "$SOCKET_PATH" "$payload" <<'PY'
import socket
import sys

sock_path = sys.argv[1]
payload = sys.argv[2]

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(sock_path)
sock.sendall((payload + "\n").encode())
print(sock.recv(8192).decode().strip())
sock.close()
PY
}

request '{"method":"ping","params":{}}'
request '{"method":"get_status","params":{}}'
request '{"method":"tail_events","params":{"limit":5}}'
