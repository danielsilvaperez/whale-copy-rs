# whale-copy-platform

Low-latency Polymarket wallet copy platform with split architecture:

1. `engine-rs`: Rust execution engine (signal ingestion, sizing, risk, execution, RPC, persistence).
2. `control-bot-ts`: Telegram control plane (runtime commands, notifications, heartbeat).
3. `shared/sql`: SQLite WAL schema and audit/event tables.
4. `ops/systemd` + `ops/watchdog`: Linux daemon deployment and auto-restart guard.

## Implemented v1 features

- Wallet copy pipeline with dedupe cache and guard chain.
- Proportional sizing: `source_notional * (follower_equity/source_equity) * multiplier`.
- Hybrid cap ordering:
  - `max_copy_notional_per_trade`
  - `max_market_exposure_usd`
  - `max_daily_notional_usd`
  - `max_open_positions`
- Fee/slippage/execution-premium gate.
- `LIMIT_IOC` execution intent with bounded retry/deviation guard.
- Event-driven wallet scoring + rotation suggestions.
- Full Telegram command surface for runtime mutation and logs.
- Command audit + structured event persistence in SQLite.
- `systemd` units + standalone watchdog process.

## Layout

- `engine-rs/`
- `control-bot-ts/`
- `shared/`
- `ops/systemd/`
- `ops/watchdog/`
- `ops/scripts/`
- `docs/`
- `tests/`

## Quick start (local)

1. Copy `.env.example` to `.env` and fill required variables.
2. Engine tests/build:
   - `cd engine-rs && cargo test`
3. Telegram bot build/tests:
   - `cd control-bot-ts && npm install && npm run build && npm test`
4. Run locally:
   - `./ops/scripts/run-local.sh`

`run-local.sh` auto-loads `/opt/whale-copy-platform/.env` style variables from project root `.env`.

## RPC overview

JSON line protocol over Unix socket (default `/tmp/whale-copy-engine.sock`).

- Request:
```json
{"method":"get_status","params":{},"actor_chat_id":"12345"}
```
- Response:
```json
{"ok":true,"result":{"health":{}},"request_id":"..."}
```

Method list is documented in `shared/rpc/schema.json`.

## Linux deployment

- Build artifacts:
  - `./ops/scripts/build-all.sh`
- Install unit files from `ops/systemd/` into `/etc/systemd/system/`.
- Configure env files under `/etc/whale-copy-platform/`.
- Enable services:
  - `sudo systemctl daemon-reload`
  - `sudo systemctl enable --now whale-copy-engine.service`
  - `sudo systemctl enable --now whale-copy-telegram.service`
  - `sudo systemctl enable --now whale-copy-watchdog.service`

Detailed runbook: `docs/OPERATIONS.md`.
