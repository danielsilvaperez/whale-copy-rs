# Architecture

## Engine (`engine-rs`)

Main loops:

1. `signal_loop`: fetch source wallet activity, parse fills, dedupe, risk-evaluate, execute, persist.
2. `rotation_loop`: fetch/score candidates, emit rotation suggestion when threshold/cooldown rules pass.
3. `heartbeat_loop`: updates `service_heartbeats` for watchdog checks.
4. `rpc_server`: Unix socket JSON RPC for runtime control.

Hot state:

- `RuntimeSettings`
- tracked wallet allowlist
- pending suggestions
- dedupe cache
- event ring buffer
- market/daily exposure counters

Persistence (`SQLite WAL`):

- `settings`
- `wallet_allowlist`
- `wallet_scores`
- `rotation_suggestions`
- `execution_events`
- `orders`
- `risk_events`
- `service_heartbeats`
- `command_audit`

## Control Bot (`control-bot-ts`)

Responsibilities:

- Long-poll Telegram updates.
- Enforce chat allowlist.
- Translate slash commands to engine RPC mutations.
- Poll tail events and push notifications.
- Report Telegram heartbeat through `report_heartbeat` RPC.

## Watchdog

`ops/watchdog/watchdog.sh` runs as a dedicated service and:

1. Checks `systemd` active state for engine and Telegram bot.
2. Pings engine RPC socket.
3. Validates heartbeat freshness from SQLite.
4. Restarts failed services and sends Telegram critical alert.
