# Test Plan Mapping

Implemented automated tests:

- `engine-rs`: unit tests for sizing math, cap ordering, fee/slippage gate, parser, rotation decision, dedupe, RPC methods, and live-mode execution safety.
- `control-bot-ts`: command parser + command handler + config + RPC client tests.
- `tests/rpc_smoke.sh`: socket-level RPC smoke checks (read + mutating methods + validation error path).

Manual/integration checks (runbook):

1. Simulated source fills -> copied order path (`/logs` + DB `orders`).
2. RPC end-to-end via Telegram commands.
3. Duplicate event replay verification (dedupe prevents second order).
4. Kill engine/telegram services and confirm watchdog restart + alert.

Run smoke script manually:

```bash
ENGINE_SOCKET_PATH=/tmp/whale-copy-engine.sock ./tests/rpc_smoke.sh
```
