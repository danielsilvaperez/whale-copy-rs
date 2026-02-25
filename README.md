<div align="center">

# 🐋 Whale Copy Platform

**Low-latency Polymarket wallet copy trading with institutional-grade risk management**

[![Rust](https://img.shields.io/badge/Rust-1.70+-orange?logo=rust)](https://www.rust-lang.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8+-blue?logo=typescript)](https://www.typescriptlang.org)
[![SQLite](https://img.shields.io/badge/SQLite-WAL-green?logo=sqlite)](https://sqlite.org)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

<img src="https://img.shields.io/badge/Engine-⚡%20Rust-red?style=for-the-badge" />
<img src="https://img.shields.io/badge/Control-💬%20Telegram-blue?style=for-the-badge" />
<img src="https://img.shields.io/badge/Deploy-🖥️%20systemd-purple?style=for-the-badge" />

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Telegram Commands](#-telegram-commands)
- [Deployment](#-deployment)
- [Security](#-security)
- [Documentation](#-documentation)

---

## 🎯 Overview

**Whale Copy Platform** is a high-performance, low-latency copy trading system designed for Polymarket prediction markets. It tracks high-performing wallets ("whales") and automatically executes proportional trades with sophisticated risk management.

### Why Whale Copy?

| Feature | Benefit |
|---------|---------|
| ⚡ **Sub-second latency** | Rust-based execution engine for rapid signal processing |
| 🛡️ **Multi-layer risk guards** | Position caps, exposure limits, and automatic circuit breakers |
| 📱 **Telegram control plane** | Mobile-first monitoring and runtime configuration |
| 🔄 **Smart wallet rotation** | Event-driven scoring with rotation suggestions |
| 📝 **Full audit trail** | SQLite WAL with structured event persistence |
| 🖥️ **Production ready** | systemd integration with watchdog auto-restart |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Whale Copy Platform                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────────────┐         ┌──────────────────┐                │
│   │  📊 Polymarket   │         │   👤 Operator    │                │
│   │    Data API      │         │    (Telegram)    │                │
│   └────────┬─────────┘         └────────┬─────────┘                │
│            │                            │                         │
│            ▼                            ▼                         │
│   ┌──────────────────────────────────────────────────┐           │
│   │              ⚡ Engine (engine-rs)                │           │
│   │  ┌──────────┐ ┌──────────┐ ┌────────────────┐   │           │
│   │  │  Signal  │ │ Rotation │ │  RPC Server    │   │           │
│   │  │   Loop   │ │   Loop   │ │ (Unix Socket)  │   │           │
│   │  └──────────┘ └──────────┘ └────────────────┘   │           │
│   │                                                  │           │
│   │  ┌──────────┐ ┌──────────┐ ┌────────────────┐   │           │
│   │  │  Risk    │ │ Position │ │   Heartbeat    │   │           │
│   │  │  Engine  │ │   Sizing │ │    Emitter     │   │           │
│   │  └──────────┘ └──────────┘ └────────────────┘   │           │
│   └────────────────────┬─────────────────────────────┘           │
│                        │                                          │
│            ┌───────────┴───────────┐                             │
│            ▼                       ▼                             │
│   ┌──────────────┐        ┌────────────────┐                     │
│   │  💾 SQLite   │        │  💬 Telegram   │                     │
│   │  (WAL Mode)  │        │ Control Bot    │                     │
│   └──────────────┘        └────────────────┘                     │
│                                    │                             │
│   ┌──────────────────────────────────────────────────┐           │
│   │              🐕 Watchdog Service                 │           │
│   │    Health checks • Auto-restart • Alerts        │           │
│   └──────────────────────────────────────────────────┘           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Language | Purpose |
|-----------|----------|---------|
| **Engine** | 🦀 Rust | Signal ingestion, risk evaluation, execution, persistence |
| **Control Bot** | 🔷 TypeScript | Telegram interface for runtime commands |
| **Watchdog** | 🐚 Bash | Service health monitoring and auto-restart |
| **Database** | 💾 SQLite | WAL-mode persistence with full audit trail |

---

## ✨ Features

### 🔥 Signal Processing
- Real-time wallet activity monitoring via WebSocket
- Intelligent fill parsing with deduplication cache
- Proportional position sizing: `source_notional × (follower_equity/source_equity) × multiplier`

### 🛡️ Risk Management
| Guard | Description |
|-------|-------------|
| `max_copy_notional_per_trade` | Per-trade size limit |
| `max_market_exposure_usd` | Market-level exposure cap |
| `max_daily_notional_usd` | Daily trading limit |
| `max_open_positions` | Position count limit |
| Fee/slippage gate | Execution premium validation |
| `LIMIT_IOC` orders | Bounded retry with deviation guard |

### 🔄 Wallet Rotation
- Continuous wallet scoring algorithm
- Automatic rotation suggestions when thresholds pass
- Confidence floor and cooldown enforcement

### 📊 Position Reconciliation
- Periodic internal state vs exchange reconciliation
- Configurable warning/critical thresholds
- Auto-correct and pause-on-critical options

### 💬 Telegram Control
- Full runtime command surface
- Real-time notifications and heartbeat
- Structured event tailing (`/logs`)
- Emergency panic mode (`/panic`)

---

## 🚀 Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) 1.70+ (for engine)
- [Node.js](https://nodejs.org/) 18+ (for Telegram bot)
- SQLite 3 (usually pre-installed)

### 1. Clone & Setup

```bash
git clone https://github.com/yourusername/whale-copy-platform.git
cd whale-copy-platform

# Copy and configure environment
cp .env.example .env
# Edit .env with your settings (see Configuration section)
```

### 2. Build & Test

```bash
# Build everything
./ops/scripts/build-all.sh

# Test Rust engine
cd engine-rs && cargo test

# Test Telegram bot
cd ../control-bot-ts && npm install && npm run build && npm test
```

### 3. Run Locally

```bash
# Ensure TELEGRAM_BOT_TOKEN and TELEGRAM_ALLOWED_CHAT_IDS are set in .env
./ops/scripts/run-local.sh
```

> ⚠️ **For live trading**: Set `ALLOW_LIVE_SIMULATION=false` and configure `EXECUTION_API_BASE`

---

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ENGINE_MODE` | `dry_run` | `dry_run` or `live` |
| `RISK_PROFILE` | `aggressive` | `conservative`, `balanced`, or `aggressive` |
| `COPY_MULTIPLIER` | `1.25` | Position size multiplier |
| `FOLLOWER_EQUITY_USD` | `10000` | Your trading equity |
| `MAX_ACTIVE_WALLETS` | `5` | Number of wallets to track |
| `COPY_SELLS` | `true` | Mirror sell orders |

### Risk Caps

```env
MAX_COPY_NOTIONAL_PER_TRADE=1500
MAX_MARKET_EXPOSURE_USD=7500
MAX_DAILY_NOTIONAL_USD=45000
MAX_OPEN_POSITIONS=25
```

### Telegram Setup

```env
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
TELEGRAM_ALLOWED_CHAT_IDS=123456789,987654321
TELEGRAM_CHAT_ID=123456789
```

See [`.env.example`](.env.example) for complete configuration options.

---

## 💬 Telegram Commands

### Status & Monitoring
| Command | Description |
|---------|-------------|
| `/status` | Engine snapshot and current state |
| `/health` | Health check with component status |
| `/logs [limit]` | Recent structured events (default: 30) |

### Runtime Control
| Command | Description |
|---------|-------------|
| `/startbot` | Start the copy engine |
| `/stopbot` | Stop the copy engine |
| `/pause` | Pause trading temporarily |
| `/resume` | Resume trading |
| `/panic` | Emergency close all positions |

### Configuration
| Command | Description |
|---------|-------------|
| `/mode <dry_run\|live>` | Switch trading mode |
| `/risk <conservative\|balanced\|aggressive>` | Set risk profile |
| `/multiplier <value>` | Update copy multiplier |
| `/equity <usd>` | Update follower equity |
| `/copy_sells <on\|off>` | Toggle sell mirroring |
| `/caps key=value ...` | Update risk caps |

### Wallet Management
| Command | Description |
|---------|-------------|
| `/wallet_add <wallet>` | Add wallet to allowlist |
| `/wallet_remove <wallet>` | Remove wallet from allowlist |
| `/wallet_list` | Show tracked wallets |
| `/suggestions` | List rotation suggestions |
| `/approve <id>` | Approve rotation suggestion |
| `/reject <id>` | Reject rotation suggestion |

---

## 🖥️ Deployment

### systemd Services

```bash
# Install service files
sudo cp ops/systemd/whale-copy-engine.service /etc/systemd/system/
sudo cp ops/systemd/whale-copy-telegram.service /etc/systemd/system/
sudo cp ops/systemd/whale-copy-watchdog.service /etc/systemd/system/

# Create environment files
sudo mkdir -p /etc/whale-copy-platform
# Copy and edit env files for each service

# Start services
sudo systemctl daemon-reload
sudo systemctl enable --now whale-copy-engine.service
sudo systemctl enable --now whale-copy-telegram.service
sudo systemctl enable --now whale-copy-watchdog.service

# Monitor
journalctl -u whale-copy-engine.service -f
```

### Directory Structure

```
/opt/whale-copy-platform/
├── engine/               # Rust engine binary
├── telegram-bot/         # TypeScript bot
├── data/                 # SQLite database
└── logs/                 # Application logs

/etc/whale-copy-platform/
├── engine.env
├── telegram.env
└── watchdog.env
```

---

## 🔒 Security

### v1 Security Controls

- ✅ Private keys stored **only** in environment variables
- ✅ No key persistence in database
- ✅ Telegram allowlist enforcement (`TELEGRAM_ALLOWED_CHAT_IDS`)
- ✅ Command audit logging with actor chat IDs
- ✅ Live mode safety blocks (requires `EXECUTION_API_BASE`)

### Required Hardening

```bash
# Secure environment files
chmod 600 /etc/whale-copy-platform/*.env

# Run as dedicated non-root user
useradd -r -s /bin/false whalecopy

# Restrict socket access
chmod 660 /tmp/whale-copy-engine.sock
chown whalecopy:whalecopy /tmp/whale-copy-engine.sock
```

### Recommended v2 Upgrades

- Replace EOA hot key with **remote signer** or **HSM-backed signer**
- Enable **2FA** for Telegram commands
- Implement **IP allowlisting** for RPC socket

See [`docs/SECURITY.md`](docs/SECURITY.md) for full security guidelines.

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | System design and data flow |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | Production deployment guide |
| [`docs/COMMANDS.md`](docs/COMMANDS.md) | Complete command reference |
| [`docs/SECURITY.md`](docs/SECURITY.md) | Security best practices |
| [`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md) | Release procedures |

---

## 🤝 RPC Protocol

The engine exposes a JSON-RPC interface over Unix socket:

```bash
# Default socket path
/tmp/whale-copy-engine.sock
```

### Example Request

```json
{"method":"get_status","params":{},"actor_chat_id":"12345"}
```

### Example Response

```json
{"ok":true,"result":{"health":"healthy","mode":"dry_run"},"request_id":"uuid"}
```

See [`shared/rpc/schema.json`](shared/rpc/schema.json) for the complete schema.

---

## 🧪 Testing

```bash
# Run smoke tests
./tests/rpc_smoke.sh

# Test engine
cd engine-rs && cargo test

# Test Telegram bot
cd control-bot-ts && npm test
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built with** 🦀 **Rust** + 🔷 **TypeScript** + 💾 **SQLite**

*Trade smarter. Copy whales. Manage risk.*

</div>
