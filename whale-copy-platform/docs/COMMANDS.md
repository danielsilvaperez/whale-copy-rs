# Command Surface

## Telegram

- `/status`
- `/health`
- `/startbot`
- `/stopbot`
- `/pause`
- `/resume`
- `/panic`
- `/mode <dry_run|live>`
- `/risk <conservative|balanced|aggressive>`
- `/multiplier <value>`
- `/caps key=value ...`
- `/wallet_add <wallet>`
- `/wallet_remove <wallet>`
- `/wallet_list`
- `/suggestions`
- `/approve <suggestion_id>`
- `/reject <suggestion_id>`
- `/logs [limit]`

## Engine RPC

- `ping`
- `get_status`
- `set_mode`
- `set_risk_profile`
- `set_multiplier`
- `set_caps`
- `add_wallet`
- `remove_wallet`
- `list_wallets`
- `pause_trading`
- `resume_trading`
- `panic_close`
- `ack_suggestion`
- `reject_suggestion`
- `tail_events`
- `list_suggestions`
- `report_heartbeat`
