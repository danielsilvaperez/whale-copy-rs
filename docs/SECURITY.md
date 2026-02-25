# Security Notes

## v1 controls

- EOA execution key is environment-only.
- No private keys are persisted in SQLite.
- Telegram control is allowlist-only (`TELEGRAM_ALLOWED_CHAT_IDS`).
- Every mutating control command is written to `command_audit` with actor chat ID.
- Live mode blocks execution when `EXECUTION_API_BASE` is unset unless `ALLOW_LIVE_SIMULATION=true` is explicitly enabled.

## Required operator hardening

1. Set strict permissions on env files:
   - `chmod 600 /etc/whale-copy-platform/*.env`
2. Run services under a dedicated non-root user.
3. Keep engine socket path accessible only to service user/group.
4. Enable host firewall and SSH key-only access.
5. Rotate Telegram bot token if leaked.

## Recommended v2 upgrade

- Replace EOA hot key with remote signer or HSM-backed signer.
