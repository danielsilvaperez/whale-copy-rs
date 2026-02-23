#!/usr/bin/env bash
set -euo pipefail

SOCKET_PATH="${ENGINE_SOCKET_PATH:-/tmp/whale-copy-engine.sock}"

python3 - "$SOCKET_PATH" <<'PY'
import json
import socket
import sys

sock_path = sys.argv[1]


def rpc(method, params=None):
    payload = {"method": method, "params": params or {}}
    raw = json.dumps(payload)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(sock_path)
    sock.sendall((raw + "\n").encode())
    response = sock.recv(65536).decode().strip()
    sock.close()
    return json.loads(response)


def expect_ok(method, params=None):
    response = rpc(method, params)
    if not response.get("ok", False):
        raise RuntimeError(f"{method} failed unexpectedly: {response}")
    print(f"ok: {method}")
    return response


def expect_error(method, params=None):
    response = rpc(method, params)
    if response.get("ok", False):
        raise RuntimeError(f"{method} unexpectedly succeeded: {response}")
    print(f"ok (expected error): {method}")
    return response


wallet = "0x1111111111111111111111111111111111111111"

expect_ok("ping")
expect_ok("get_status")
expect_ok("tail_events", {"limit": 5})
expect_ok("set_mode", {"mode": "dry_run"})
expect_ok("set_multiplier", {"multiplier": 1.3})
expect_ok("set_follower_equity", {"follower_equity_usd": 15000})
expect_ok("set_copy_sells", {"copy_sells": False})
expect_ok("set_copy_sells", {"copy_sells": True})
expect_ok("add_wallet", {"wallet": wallet})

list_after_add = expect_ok("list_wallets")
wallets_after_add = (list_after_add.get("result") or {}).get("wallets") or []
if wallet not in wallets_after_add:
    raise RuntimeError(f"wallet missing after add: {wallets_after_add}")

expect_ok("remove_wallet", {"wallet": wallet})
list_after_remove = expect_ok("list_wallets")
wallets_after_remove = (list_after_remove.get("result") or {}).get("wallets") or []
if wallet in wallets_after_remove:
    raise RuntimeError(f"wallet still present after remove: {wallets_after_remove}")

expect_error("set_multiplier", {"multiplier": -1})

print("rpc smoke checks passed")
PY
