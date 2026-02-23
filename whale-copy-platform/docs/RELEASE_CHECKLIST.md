# Release Checklist (v1)

## 1) Preflight

- Confirm branch is up to date with main.
- Confirm CI is green (`.github/workflows/ci.yml`).
- Confirm no unreviewed TODO/FIXME items for release scope.

## 2) Environment and safety

- Verify `ENGINE_MODE` is set intentionally (`dry_run` or `live`).
- For live mode, verify `EXECUTION_API_BASE` is configured and reachable.
- Keep `ALLOW_LIVE_SIMULATION=false` in production.
- Verify `TELEGRAM_BOT_TOKEN` and `TELEGRAM_ALLOWED_CHAT_IDS` are set.
- Verify env files have strict permissions (`chmod 600 /etc/whale-copy-platform/*.env`).

## 3) Test gates

- Run Rust tests:
  - `cd whale-copy-platform/engine-rs && cargo test`
- Run control bot build/tests:
  - `cd whale-copy-platform/control-bot-ts && npm ci && npm run build && npm test`
- Run RPC smoke checks with engine running:
  - `cd whale-copy-platform/tests && ENGINE_SOCKET_PATH=/tmp/whale-copy-engine.sock ./rpc_smoke.sh`

## 4) Deployment checks

- Build artifacts:
  - `cd whale-copy-platform && ./ops/scripts/build-all.sh`
- Validate `systemd` units are installed and enabled.
- Verify service health:
  - `systemctl status whale-copy-engine.service`
  - `systemctl status whale-copy-telegram.service`
  - `systemctl status whale-copy-watchdog.service`
- Verify structured logs are flowing for engine and Telegram bot.

## 5) Functional acceptance

- Telegram `/status` returns current health/settings.
- Telegram `/wallet_add` and `/wallet_remove` mutate allowlist correctly.
- Telegram `/equity` and `/copy_sells` apply runtime settings.
- Failed live execution paths create both:
  - `orders.status = failed`
  - `risk_events.reason = execution_failed`

## 6) Rollback readiness

- Keep previous release artifacts available.
- Capture current env files and service unit hashes before deploy.
- If rollback is needed: stop services, restore previous artifacts/env, restart services, run smoke checks.
