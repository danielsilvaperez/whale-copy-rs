# Test Plan Mapping

Implemented automated tests:

- `engine-rs`: unit tests for sizing math, cap ordering, fee/slippage gate, parser, rotation decision, dedupe, rpc mode parse.
- `control-bot-ts`: command parser validation tests.

Manual/integration checks (runbook):

1. Simulated source fills -> copied order path (`/logs` + DB `orders`).
2. RPC end-to-end via Telegram commands.
3. Duplicate event replay verification (dedupe prevents second order).
4. Kill engine/telegram services and confirm watchdog restart + alert.
