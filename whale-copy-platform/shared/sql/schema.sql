PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wallet_allowlist (
  wallet TEXT PRIMARY KEY,
  source TEXT NOT NULL DEFAULT 'manual',
  active INTEGER NOT NULL DEFAULT 1,
  inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wallet_scores (
  wallet TEXT PRIMARY KEY,
  score REAL NOT NULL,
  pnl_consistency REAL NOT NULL,
  win_rate REAL NOT NULL,
  drawdown_penalty REAL NOT NULL,
  recent_activity REAL NOT NULL,
  fill_quality REAL NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rotation_suggestions (
  id TEXT PRIMARY KEY,
  wallet TEXT NOT NULL,
  score REAL NOT NULL,
  reason TEXT NOT NULL,
  expected_impact TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  decided_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_rotation_suggestions_status_created_at
  ON rotation_suggestions(status, created_at DESC);

CREATE TABLE IF NOT EXISTS execution_events (
  id TEXT PRIMARY KEY,
  class TEXT NOT NULL,
  event_type TEXT NOT NULL,
  execution_id TEXT,
  wallet TEXT,
  market_slug TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_events_created_at
  ON execution_events(created_at DESC);

CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL,
  market_slug TEXT NOT NULL,
  side TEXT NOT NULL,
  style TEXT NOT NULL,
  request_json TEXT NOT NULL,
  response_json TEXT NOT NULL,
  status TEXT NOT NULL,
  latency_ms REAL NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_execution_id
  ON orders(execution_id);

CREATE TABLE IF NOT EXISTS risk_events (
  id TEXT PRIMARY KEY,
  execution_id TEXT,
  decision TEXT NOT NULL,
  reason TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_risk_events_created_at
  ON risk_events(created_at DESC);

CREATE TABLE IF NOT EXISTS service_heartbeats (
  service TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  details_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS command_audit (
  id TEXT PRIMARY KEY,
  actor_chat_id TEXT,
  command TEXT NOT NULL,
  old_value_json TEXT,
  new_value_json TEXT,
  result TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_command_audit_created_at
  ON command_audit(created_at DESC);
