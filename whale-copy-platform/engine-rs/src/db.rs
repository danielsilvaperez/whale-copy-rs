use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

use crate::types::{
    EventClass, EventEnvelope, ExecutionResult, RotationSuggestion, RuntimeSettings,
    SuggestionStatus, WalletScore,
};

const SCHEMA_SQL: &str = include_str!("../../shared/sql/schema.sql");

#[derive(Clone)]
pub struct Db {
    path: Arc<String>,
}

impl Db {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: Arc::new(path.into()),
        }
    }

    async fn with_conn<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Connection) -> Result<R> + Send + 'static,
    {
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || -> Result<R> {
            let conn = Connection::open(path.as_str()).context("opening sqlite database")?;
            conn.pragma_update(None, "journal_mode", "WAL")
                .context("setting journal_mode=WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")
                .context("enabling foreign keys")?;
            f(&conn)
        })
        .await
        .context("waiting for sqlite worker")?
    }

    pub async fn init(&self) -> Result<()> {
        let db_path = self.path.as_ref().clone();
        if let Some(parent) = Path::new(&db_path).parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating sqlite data dir {}", parent.display()))?;
        }

        self.with_conn(move |conn| {
            conn.execute_batch(SCHEMA_SQL).context("applying schema")?;
            Ok(())
        })
        .await
    }

    pub async fn load_runtime_settings(&self) -> Result<Option<RuntimeSettings>> {
        self.with_conn(move |conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT value FROM settings WHERE key = 'runtime_settings'",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .context("loading runtime_settings")?;

            raw.map(|value| {
                serde_json::from_str::<RuntimeSettings>(&value)
                    .with_context(|| "parsing runtime_settings from sqlite")
            })
            .transpose()
        })
        .await
    }

    pub async fn save_runtime_settings(&self, settings: &RuntimeSettings) -> Result<()> {
        let payload = serde_json::to_string(settings).context("serializing runtime settings")?;

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO settings (key, value, version, updated_at)
                VALUES (
                  'runtime_settings',
                  ?1,
                  COALESCE((SELECT version + 1 FROM settings WHERE key = 'runtime_settings'), 1),
                  CURRENT_TIMESTAMP
                )
                ON CONFLICT(key) DO UPDATE SET
                  value = excluded.value,
                  version = settings.version + 1,
                  updated_at = CURRENT_TIMESTAMP
                ",
                params![payload],
            )
            .context("persisting runtime_settings")?;
            Ok(())
        })
        .await
    }

    pub async fn load_wallet_allowlist(&self) -> Result<BTreeSet<String>> {
        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT wallet FROM wallet_allowlist WHERE active = 1 ORDER BY inserted_at ASC",
                )
                .context("preparing wallet allowlist query")?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .context("querying wallet allowlist")?;

            let mut wallets = BTreeSet::new();
            for wallet in rows {
                wallets.insert(wallet.context("reading wallet allowlist row")?);
            }
            Ok(wallets)
        })
        .await
    }

    pub async fn set_wallet_active(&self, wallet: &str, source: &str, active: bool) -> Result<()> {
        let wallet = wallet.to_owned();
        let source = source.to_owned();
        let active_int = i64::from(active);

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO wallet_allowlist (wallet, source, active, inserted_at, updated_at)
                VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(wallet) DO UPDATE SET
                  source = excluded.source,
                  active = excluded.active,
                  updated_at = CURRENT_TIMESTAMP
                ",
                params![wallet, source, active_int],
            )
            .context("upserting wallet allowlist row")?;
            Ok(())
        })
        .await
    }

    pub async fn load_pending_suggestions(&self) -> Result<BTreeMap<String, RotationSuggestion>> {
        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "
                    SELECT id, wallet, score, reason, expected_impact, status, created_at, decided_at
                    FROM rotation_suggestions
                    WHERE status = 'pending'
                    ORDER BY created_at DESC
                    ",
                )
                .context("preparing pending suggestions query")?;
            let rows = stmt
                .query_map([], |row| {
                    let created_raw: String = row.get(6)?;
                    let decided_raw: Option<String> = row.get(7)?;

                    Ok(RotationSuggestion {
                        id: row.get(0)?,
                        wallet: row.get(1)?,
                        score: row.get(2)?,
                        reason: row.get(3)?,
                        expected_impact: row.get(4)?,
                        status: parse_suggestion_status(row.get::<_, String>(5)?.as_str()),
                        created_at: parse_ts(&created_raw),
                        decided_at: decided_raw.map(|v| parse_ts(&v)),
                    })
                })
                .context("loading pending suggestions")?;

            let mut map = BTreeMap::new();
            for row in rows {
                let suggestion = row.context("reading pending suggestion")?;
                map.insert(suggestion.id.clone(), suggestion);
            }
            Ok(map)
        })
        .await
    }

    pub async fn save_rotation_suggestion(&self, suggestion: &RotationSuggestion) -> Result<()> {
        let suggestion = suggestion.clone();

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO rotation_suggestions (id, wallet, score, reason, expected_impact, status, created_at, decided_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(id) DO UPDATE SET
                  wallet = excluded.wallet,
                  score = excluded.score,
                  reason = excluded.reason,
                  expected_impact = excluded.expected_impact,
                  status = excluded.status,
                  created_at = excluded.created_at,
                  decided_at = excluded.decided_at
                ",
                params![
                    suggestion.id,
                    suggestion.wallet,
                    suggestion.score,
                    suggestion.reason,
                    suggestion.expected_impact,
                    suggestion_status_to_str(suggestion.status),
                    suggestion.created_at.to_rfc3339(),
                    suggestion.decided_at.map(|v| v.to_rfc3339()),
                ],
            )
            .context("upserting rotation suggestion")?;
            Ok(())
        })
        .await
    }

    pub async fn set_rotation_suggestion_status(
        &self,
        suggestion_id: &str,
        status: SuggestionStatus,
        decided_at: DateTime<Utc>,
    ) -> Result<()> {
        let suggestion_id = suggestion_id.to_owned();
        let status_value = suggestion_status_to_str(status).to_owned();
        let decided_value = decided_at.to_rfc3339();

        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE rotation_suggestions SET status = ?1, decided_at = ?2 WHERE id = ?3",
                params![status_value, decided_value, suggestion_id],
            )
            .context("updating rotation_suggestions status")?;
            Ok(())
        })
        .await
    }

    pub async fn upsert_wallet_scores(&self, scores: &[WalletScore]) -> Result<()> {
        if scores.is_empty() {
            return Ok(());
        }

        let scores = scores.to_vec();
        self.with_conn(move |conn| {
            let tx = conn.unchecked_transaction().context("starting wallet_scores tx")?;
            {
                let mut stmt = tx
                    .prepare(
                        "
                        INSERT INTO wallet_scores (
                          wallet, score, pnl_consistency, win_rate, drawdown_penalty, recent_activity, fill_quality, updated_at
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        ON CONFLICT(wallet) DO UPDATE SET
                          score = excluded.score,
                          pnl_consistency = excluded.pnl_consistency,
                          win_rate = excluded.win_rate,
                          drawdown_penalty = excluded.drawdown_penalty,
                          recent_activity = excluded.recent_activity,
                          fill_quality = excluded.fill_quality,
                          updated_at = excluded.updated_at
                        ",
                    )
                    .context("preparing wallet_scores upsert")?;

                for score in &scores {
                    stmt.execute(params![
                        score.wallet,
                        score.score,
                        score.pnl_consistency,
                        score.win_rate,
                        score.drawdown_penalty,
                        score.recent_activity,
                        score.fill_quality,
                        score.updated_at.to_rfc3339(),
                    ])
                    .with_context(|| format!("upserting wallet_score for {}", score.wallet))?;
                }
            }
            tx.commit().context("committing wallet_scores tx")?;
            Ok(())
        })
        .await
    }

    pub async fn load_wallet_scores(&self) -> Result<HashMap<String, WalletScore>> {
        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "
                    SELECT wallet, score, pnl_consistency, win_rate, drawdown_penalty, recent_activity, fill_quality, updated_at
                    FROM wallet_scores
                    ",
                )
                .context("preparing wallet_scores load")?;
            let rows = stmt
                .query_map([], |row| {
                    let updated_raw: String = row.get(7)?;
                    Ok(WalletScore {
                        wallet: row.get(0)?,
                        score: row.get(1)?,
                        pnl_consistency: row.get(2)?,
                        win_rate: row.get(3)?,
                        drawdown_penalty: row.get(4)?,
                        recent_activity: row.get(5)?,
                        fill_quality: row.get(6)?,
                        updated_at: parse_ts(&updated_raw),
                    })
                })
                .context("querying wallet_scores")?;

            let mut out = HashMap::new();
            for row in rows {
                let score = row.context("reading wallet_score row")?;
                out.insert(score.wallet.clone(), score);
            }
            Ok(out)
        })
        .await
    }

    pub async fn insert_event(
        &self,
        event: &EventEnvelope,
        execution_id: Option<&str>,
        wallet: Option<&str>,
        market_slug: Option<&str>,
    ) -> Result<()> {
        let event = event.clone();
        let payload = serde_json::to_string(&event.payload).context("serializing event payload")?;
        let execution_id = execution_id.map(ToOwned::to_owned);
        let wallet = wallet.map(ToOwned::to_owned);
        let market_slug = market_slug.map(ToOwned::to_owned);

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO execution_events (id, class, event_type, execution_id, wallet, market_slug, payload_json, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ",
                params![
                    event.id,
                    event.class.as_str(),
                    event.event_type,
                    execution_id,
                    wallet,
                    market_slug,
                    payload,
                    event.ts.to_rfc3339(),
                ],
            )
            .context("inserting execution_event")?;
            Ok(())
        })
        .await
    }

    pub async fn insert_risk_event(
        &self,
        execution_id: Option<&str>,
        decision: &str,
        reason: &str,
        payload: &Value,
    ) -> Result<()> {
        let execution_id = execution_id.map(ToOwned::to_owned);
        let decision = decision.to_owned();
        let reason = reason.to_owned();
        let payload_json =
            serde_json::to_string(payload).context("serializing risk event payload")?;
        let id = uuid::Uuid::new_v4().to_string();
        let created_at = Utc::now().to_rfc3339();

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO risk_events (id, execution_id, decision, reason, payload_json, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ",
                params![id, execution_id, decision, reason, payload_json, created_at],
            )
            .context("inserting risk_event")?;
            Ok(())
        })
        .await
    }

    pub async fn insert_order(
        &self,
        order_id: &str,
        execution_id: &str,
        market_slug: &str,
        side: &str,
        style: &str,
        request: &Value,
        result: &ExecutionResult,
    ) -> Result<()> {
        let order_id = order_id.to_owned();
        let execution_id = execution_id.to_owned();
        let market_slug = market_slug.to_owned();
        let side = side.to_owned();
        let style = style.to_owned();
        let request_json = serde_json::to_string(request).context("serializing order request")?;
        let response_json = serde_json::to_string(result).context("serializing order response")?;
        let status = if result.success { "filled" } else { "failed" }.to_string();
        let latency_ms = result.latency_ms;
        let created_at = Utc::now().to_rfc3339();

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO orders (id, execution_id, market_slug, side, style, request_json, response_json, status, latency_ms, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ",
                params![
                    order_id,
                    execution_id,
                    market_slug,
                    side,
                    style,
                    request_json,
                    response_json,
                    status,
                    latency_ms,
                    created_at,
                ],
            )
            .context("inserting order row")?;
            Ok(())
        })
        .await
    }

    pub async fn write_heartbeat(
        &self,
        service: &str,
        status: &str,
        details: &Value,
    ) -> Result<()> {
        let service = service.to_owned();
        let status = status.to_owned();
        let details_json =
            serde_json::to_string(details).context("serializing heartbeat details")?;
        let updated_at = Utc::now().to_rfc3339();

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO service_heartbeats (service, status, details_json, updated_at)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(service) DO UPDATE SET
                  status = excluded.status,
                  details_json = excluded.details_json,
                  updated_at = excluded.updated_at
                ",
                params![service, status, details_json, updated_at],
            )
            .context("upserting service heartbeat")?;
            Ok(())
        })
        .await
    }

    pub async fn latest_heartbeat(&self, service: &str) -> Result<Option<DateTime<Utc>>> {
        let service = service.to_owned();
        self.with_conn(move |conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT updated_at FROM service_heartbeats WHERE service = ?1",
                    params![service],
                    |row| row.get(0),
                )
                .optional()
                .context("querying latest heartbeat")?;

            Ok(raw.as_deref().map(parse_ts))
        })
        .await
    }

    pub async fn audit_command(
        &self,
        actor_chat_id: Option<&str>,
        command: &str,
        old_value: Option<&Value>,
        new_value: Option<&Value>,
        result: &str,
    ) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let actor_chat_id = actor_chat_id.map(ToOwned::to_owned);
        let command = command.to_owned();
        let old_json = old_value
            .map(serde_json::to_string)
            .transpose()
            .context("serializing command old value")?;
        let new_json = new_value
            .map(serde_json::to_string)
            .transpose()
            .context("serializing command new value")?;
        let result = result.to_owned();
        let created_at = Utc::now().to_rfc3339();

        self.with_conn(move |conn| {
            conn.execute(
                "
                INSERT INTO command_audit (id, actor_chat_id, command, old_value_json, new_value_json, result, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ",
                params![id, actor_chat_id, command, old_json, new_json, result, created_at],
            )
            .context("inserting command audit")?;
            Ok(())
        })
        .await
    }

    pub async fn tail_events(&self, limit: usize) -> Result<Vec<EventEnvelope>> {
        let limit = i64::try_from(limit).unwrap_or(100);

        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "
                    SELECT id, class, event_type, payload_json, created_at
                    FROM execution_events
                    ORDER BY created_at DESC
                    LIMIT ?1
                    ",
                )
                .context("preparing tail_events query")?;

            let rows = stmt
                .query_map(params![limit], |row| {
                    let payload_json: String = row.get(3)?;
                    let created_at_raw: String = row.get(4)?;
                    let payload: Value = serde_json::from_str(&payload_json)
                        .unwrap_or_else(|_| json!({ "raw": payload_json }));

                    Ok(EventEnvelope {
                        id: row.get(0)?,
                        ts: parse_ts(&created_at_raw),
                        class: EventClass::from_str(row.get::<_, String>(1)?.as_str()),
                        event_type: row.get(2)?,
                        payload,
                    })
                })
                .context("querying tail_events")?;

            let mut events = Vec::new();
            for row in rows {
                events.push(row.context("reading tail_events row")?);
            }
            events.reverse();
            Ok(events)
        })
        .await
    }

    pub async fn list_pending_suggestions(&self, limit: usize) -> Result<Vec<RotationSuggestion>> {
        let limit = i64::try_from(limit).unwrap_or(25);
        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "
                    SELECT id, wallet, score, reason, expected_impact, status, created_at, decided_at
                    FROM rotation_suggestions
                    WHERE status = 'pending'
                    ORDER BY created_at DESC
                    LIMIT ?1
                    ",
                )
                .context("preparing list_pending_suggestions query")?;

            let rows = stmt
                .query_map(params![limit], |row| {
                    let created_raw: String = row.get(6)?;
                    let decided_raw: Option<String> = row.get(7)?;
                    Ok(RotationSuggestion {
                        id: row.get(0)?,
                        wallet: row.get(1)?,
                        score: row.get(2)?,
                        reason: row.get(3)?,
                        expected_impact: row.get(4)?,
                        status: parse_suggestion_status(row.get::<_, String>(5)?.as_str()),
                        created_at: parse_ts(&created_raw),
                        decided_at: decided_raw.map(|v| parse_ts(&v)),
                    })
                })
                .context("querying pending suggestions")?;

            let mut out = Vec::new();
            for row in rows {
                out.push(row.context("reading suggestion row")?);
            }
            Ok(out)
        })
        .await
    }
}

fn suggestion_status_to_str(status: SuggestionStatus) -> &'static str {
    match status {
        SuggestionStatus::Pending => "pending",
        SuggestionStatus::Approved => "approved",
        SuggestionStatus::Rejected => "rejected",
    }
}

fn parse_suggestion_status(value: &str) -> SuggestionStatus {
    match value {
        "approved" => SuggestionStatus::Approved,
        "rejected" => SuggestionStatus::Rejected,
        _ => SuggestionStatus::Pending,
    }
}

fn parse_ts(value: &str) -> DateTime<Utc> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(value) {
        return ts.with_timezone(&Utc);
    }

    if let Ok(naive) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
    }

    tracing::warn!(raw = %value, "unparseable timestamp, falling back to Utc::now()");
    Utc::now()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn stores_and_loads_runtime_settings() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let db = Db::new(db_path.display().to_string());

        db.init().await.expect("db init");

        let mut settings = RuntimeSettings::default();
        settings.multiplier = 1.42;
        db.save_runtime_settings(&settings)
            .await
            .expect("save runtime settings");

        let loaded = db
            .load_runtime_settings()
            .await
            .expect("load runtime settings")
            .expect("settings should exist");

        assert!((loaded.multiplier - 1.42).abs() < f64::EPSILON);
    }
}
