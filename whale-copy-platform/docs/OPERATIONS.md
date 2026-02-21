# Operations

## 1) Build

```bash
cd /opt/whale-copy-platform
./ops/scripts/build-all.sh
```

## 2) Environment files

Create:

- `/etc/whale-copy-platform/engine.env`
- `/etc/whale-copy-platform/telegram.env`
- `/etc/whale-copy-platform/watchdog.env`

Start from `.env.example` and split values by service.

Required minimum:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_ALLOWED_CHAT_IDS`
- `ENGINE_DB_PATH`
- `ENGINE_SOCKET_PATH`

## 3) Install services

```bash
sudo cp ops/systemd/whale-copy-engine.service /etc/systemd/system/
sudo cp ops/systemd/whale-copy-telegram.service /etc/systemd/system/
sudo cp ops/systemd/whale-copy-watchdog.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now whale-copy-engine.service
sudo systemctl enable --now whale-copy-telegram.service
sudo systemctl enable --now whale-copy-watchdog.service
```

## 4) Validate runtime

```bash
systemctl status whale-copy-engine.service
systemctl status whale-copy-telegram.service
systemctl status whale-copy-watchdog.service
journalctl -u whale-copy-engine.service -f
journalctl -u whale-copy-telegram.service -f
journalctl -u whale-copy-watchdog.service -f
```

## 5) Smoke checks

- Telegram `/status` returns engine snapshot.
- Telegram `/wallet_add <wallet>` mutates allowlist.
- Telegram `/logs 10` returns recent structured events.
- Kill engine process and verify watchdog restarts + alert.
