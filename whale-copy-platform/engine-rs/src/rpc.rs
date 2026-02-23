use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::RwLock;

use crate::config::EngineConfig;
use crate::db::Db;
use crate::signals::{compute_backoff_delay, normalize_wallet};
use crate::state::EngineState;
use crate::types::{
    EngineMode, EventClass, RiskProfile, RotationSuggestion, RpcRequest, RpcResponse,
    SuggestionStatus,
};

#[derive(Clone)]
pub struct AppContext {
    pub state: Arc<RwLock<EngineState>>,
    pub db: Db,
    pub config: EngineConfig,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug)]
struct AuditRecord {
    command: String,
    old_value: Option<Value>,
    new_value: Option<Value>,
    result: String,
    actor_chat_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SetModeParams {
    mode: String,
}

#[derive(Debug, Deserialize)]
struct SetRiskProfileParams {
    risk_profile: String,
}

#[derive(Debug, Deserialize)]
struct SetMultiplierParams {
    multiplier: f64,
}

#[derive(Debug, Deserialize)]
struct SetCapsParams {
    max_copy_notional_per_trade: Option<f64>,
    max_market_exposure_usd: Option<f64>,
    max_daily_notional_usd: Option<f64>,
    max_open_positions: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct WalletParams {
    wallet: String,
}

#[derive(Debug, Deserialize)]
struct SuggestionParams {
    id: String,
}

#[derive(Debug, Deserialize)]
struct TailParams {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct HeartbeatParams {
    service: String,
    status: Option<String>,
    details: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct SetFollowerEquityParams {
    follower_equity_usd: f64,
}

#[derive(Debug, Deserialize)]
struct SetCopySellsParams {
    copy_sells: bool,
}

pub async fn run_rpc_server(ctx: AppContext) -> Result<()> {
    let socket_path = ctx.config.socket_path.clone();
    if Path::new(&socket_path).exists() {
        fs::remove_file(&socket_path)
            .with_context(|| format!("removing stale socket {}", socket_path))?;
    }

    if let Some(parent) = Path::new(&socket_path).parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating socket dir {}", parent.display()))?;
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("binding unix socket at {}", socket_path))?;

    // Restrict socket permissions to owner only
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&socket_path)?.permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(&socket_path, perms)?;
    }

    tracing::info!(socket_path = %socket_path, "rpc listener started");

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("accepting rpc socket client")?;
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, ctx_clone).await {
                tracing::warn!(error = %err, "rpc client handler failed");
            }
        });
    }
}

async fn handle_client(stream: UnixStream, ctx: AppContext) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    while let Some(line) = lines.next_line().await.context("reading rpc line")? {
        if line.trim().is_empty() {
            continue;
        }

        let request = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(value) => value,
            Err(err) => {
                let response = RpcResponse::err(format!("invalid json request: {err}"), None);
                write_response(&mut write_half, &response).await?;
                continue;
            }
        };

        let response = handle_rpc_request(request, &ctx).await;
        write_response(&mut write_half, &response).await?;
    }

    Ok(())
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &RpcResponse,
) -> Result<()> {
    let encoded = serde_json::to_string(response).context("serializing rpc response")?;
    writer
        .write_all(format!("{encoded}\n").as_bytes())
        .await
        .context("writing rpc response")?;
    Ok(())
}

pub async fn handle_rpc_request(req: RpcRequest, ctx: &AppContext) -> RpcResponse {
    let request_id = req.request_id.clone();
    let method = req.method.trim().to_ascii_lowercase();

    match process_rpc_method(req, &method, ctx).await {
        Ok((result, audit)) => {
            if let Some(audit_record) = audit {
                if let Err(err) = ctx
                    .db
                    .audit_command(
                        audit_record.actor_chat_id.as_deref(),
                        &audit_record.command,
                        audit_record.old_value.as_ref(),
                        audit_record.new_value.as_ref(),
                        &audit_record.result,
                    )
                    .await
                {
                    tracing::warn!(error = %err, command = %audit_record.command, "failed to persist command audit");
                }
            }
            RpcResponse::ok(result, request_id)
        }
        Err(err) => RpcResponse::err(err.to_string(), request_id),
    }
}

async fn process_rpc_method(
    req: RpcRequest,
    method: &str,
    ctx: &AppContext,
) -> Result<(Value, Option<AuditRecord>)> {
    match method {
        "ping" => {
            let payload = json!({
                "pong": true,
                "ts": Utc::now(),
            });
            Ok((payload, None))
        }
        "get_status" => {
            let state = ctx.state.read().await;
            let health = state.health_snapshot();
            let payload = json!({
                "health": health,
                "settings": state.settings,
                "tracked_wallets": state.tracked_wallets,
                "pending_suggestions": state.pending_suggestions,
                "started_at": ctx.started_at,
            });
            Ok((payload, None))
        }
        "list_wallets" => {
            let state = ctx.state.read().await;
            Ok((json!({ "wallets": state.tracked_wallets }), None))
        }
        "list_suggestions" => {
            let state = ctx.state.read().await;
            let pending: Vec<&RotationSuggestion> = state.pending_suggestions.values().collect();
            Ok((json!({ "pending": pending }), None))
        }
        "tail_events" => {
            let params: TailParams = parse_params(req.params)?;
            let limit = params
                .limit
                .filter(|v| *v > 0)
                .unwrap_or(ctx.config.command_tail_default)
                .min(500);

            let events = {
                let state = ctx.state.read().await;
                if !state.event_buffer.is_empty() {
                    state
                        .event_buffer
                        .iter()
                        .rev()
                        .take(limit)
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            if !events.is_empty() {
                let mut ordered = events;
                ordered.reverse();
                return Ok((json!({ "events": ordered }), None));
            }

            let db_events = ctx.db.tail_events(limit).await?;
            Ok((json!({ "events": db_events }), None))
        }
        "set_mode" => {
            let params: SetModeParams = parse_params(req.params)?;
            let new_mode = parse_mode(&params.mode)?;

            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "mode": state.settings.mode.as_str() });
                state.settings.mode = new_mode;
                let mode_value = state.settings.mode.as_str().to_string();
                let new_value = json!({ "mode": mode_value });
                let event = state.push_event(
                    EventClass::Info,
                    "mode_changed",
                    json!({ "mode": mode_value }),
                );
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "mode": settings_snapshot.mode.as_str() }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "set_risk_profile" => {
            let params: SetRiskProfileParams = parse_params(req.params)?;
            let new_profile = parse_risk_profile(&params.risk_profile)?;

            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "risk_profile": state.settings.risk_profile.as_str() });
                state.settings.risk_profile = new_profile;
                let risk_profile_value = state.settings.risk_profile.as_str().to_string();
                let new_value = json!({ "risk_profile": risk_profile_value });
                let event = state.push_event(
                    EventClass::Info,
                    "risk_profile_changed",
                    json!({ "risk_profile": risk_profile_value }),
                );
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "risk_profile": settings_snapshot.risk_profile.as_str() }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "set_multiplier" => {
            let params: SetMultiplierParams = parse_params(req.params)?;
            if params.multiplier <= 0.0 {
                anyhow::bail!("multiplier must be > 0");
            }

            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "multiplier": state.settings.multiplier });
                state.settings.multiplier = params.multiplier;
                let multiplier_value = state.settings.multiplier;
                let new_value = json!({ "multiplier": multiplier_value });
                let event = state.push_event(
                    EventClass::Info,
                    "multiplier_changed",
                    json!({ "multiplier": multiplier_value }),
                );
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "multiplier": settings_snapshot.multiplier }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "set_caps" => {
            let params: SetCapsParams = parse_params(req.params)?;

            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "caps": state.settings.caps.clone() });

                if let Some(v) = params.max_copy_notional_per_trade {
                    if v <= 0.0 {
                        anyhow::bail!("max_copy_notional_per_trade must be > 0");
                    }
                    state.settings.caps.max_copy_notional_per_trade = v;
                }
                if let Some(v) = params.max_market_exposure_usd {
                    if v <= 0.0 {
                        anyhow::bail!("max_market_exposure_usd must be > 0");
                    }
                    state.settings.caps.max_market_exposure_usd = v;
                }
                if let Some(v) = params.max_daily_notional_usd {
                    if v <= 0.0 {
                        anyhow::bail!("max_daily_notional_usd must be > 0");
                    }
                    state.settings.caps.max_daily_notional_usd = v;
                }
                if let Some(v) = params.max_open_positions {
                    if v == 0 {
                        anyhow::bail!("max_open_positions must be > 0");
                    }
                    state.settings.caps.max_open_positions = v;
                }

                let new_value = json!({ "caps": state.settings.caps.clone() });
                let event = state.push_event(EventClass::Info, "caps_changed", new_value.clone());
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "caps": settings_snapshot.caps }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "set_follower_equity" => {
            let params: SetFollowerEquityParams = parse_params(req.params)?;
            if params.follower_equity_usd <= 0.0 {
                anyhow::bail!("follower_equity_usd must be > 0");
            }

            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value =
                    json!({ "follower_equity_usd": state.settings.follower_equity_usd });
                state.settings.follower_equity_usd = params.follower_equity_usd;
                let new_value =
                    json!({ "follower_equity_usd": state.settings.follower_equity_usd });
                let event = state.push_event(
                    EventClass::Info,
                    "follower_equity_changed",
                    new_value.clone(),
                );
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "follower_equity_usd": settings_snapshot.follower_equity_usd }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "set_copy_sells" => {
            let params: SetCopySellsParams = parse_params(req.params)?;

            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "copy_sells": state.settings.copy_sells });
                state.settings.copy_sells = params.copy_sells;
                let new_value = json!({ "copy_sells": state.settings.copy_sells });
                let event =
                    state.push_event(EventClass::Info, "copy_sells_changed", new_value.clone());
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "copy_sells": settings_snapshot.copy_sells }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "add_wallet" => {
            let params: WalletParams = parse_params(req.params)?;
            let normalized = normalize_wallet(&params.wallet)
                .ok_or_else(|| anyhow!("wallet must be a valid 0x-prefixed EVM address"))?;

            let (old_wallets, new_wallets, event, inserted) = {
                let mut state = ctx.state.write().await;
                let old_wallets = json!({ "wallets": state.tracked_wallets.clone() });

                let inserted = if state.tracked_wallets.len() >= state.settings.max_active_wallets {
                    false
                } else {
                    state.tracked_wallets.insert(normalized.clone())
                };

                let new_wallets = json!({ "wallets": state.tracked_wallets.clone() });
                let event = state.push_event(
                    EventClass::Rotation,
                    if inserted {
                        "wallet_added"
                    } else {
                        "wallet_add_rejected"
                    },
                    json!({ "wallet": normalized, "inserted": inserted }),
                );

                (old_wallets, new_wallets, event, inserted)
            };

            if inserted {
                ctx.db
                    .set_wallet_active(&normalized, "manual", true)
                    .await?;
            }
            ctx.db
                .insert_event(&event, None, Some(&normalized), None)
                .await?;

            let result = if inserted {
                "ok"
            } else {
                "rejected:max_active_wallets"
            }
            .to_string();

            Ok((
                json!({ "wallet": normalized, "inserted": inserted }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_wallets),
                    new_value: Some(new_wallets),
                    result,
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "remove_wallet" => {
            let params: WalletParams = parse_params(req.params)?;
            let normalized = normalize_wallet(&params.wallet)
                .ok_or_else(|| anyhow!("wallet must be a valid 0x-prefixed EVM address"))?;

            let (old_wallets, new_wallets, event, removed) = {
                let mut state = ctx.state.write().await;
                let old_wallets = json!({ "wallets": state.tracked_wallets.clone() });
                let removed = state.tracked_wallets.remove(&normalized);
                let new_wallets = json!({ "wallets": state.tracked_wallets.clone() });
                let event = state.push_event(
                    EventClass::Rotation,
                    if removed {
                        "wallet_removed"
                    } else {
                        "wallet_remove_noop"
                    },
                    json!({ "wallet": normalized, "removed": removed }),
                );
                (old_wallets, new_wallets, event, removed)
            };

            if removed {
                ctx.db
                    .set_wallet_active(&normalized, "manual_remove", false)
                    .await?;
            }
            ctx.db
                .insert_event(&event, None, Some(&normalized), None)
                .await?;

            let result = if removed { "ok" } else { "noop:not_found" }.to_string();

            Ok((
                json!({ "wallet": normalized, "removed": removed }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_wallets),
                    new_value: Some(new_wallets),
                    result,
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "pause_trading" => {
            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "paused": state.settings.paused });
                state.settings.paused = true;
                let new_value = json!({ "paused": state.settings.paused });
                let event = state.push_event(
                    EventClass::Risk,
                    "trading_paused",
                    json!({ "paused": true }),
                );
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "paused": true }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "resume_trading" => {
            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "paused": state.settings.paused });
                state.settings.paused = false;
                let new_value = json!({ "paused": state.settings.paused });
                let event = state.push_event(
                    EventClass::Info,
                    "trading_resumed",
                    json!({ "paused": false }),
                );
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "paused": false }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "panic_close" => {
            let (old_value, new_value, settings_snapshot, event) = {
                let mut state = ctx.state.write().await;
                let old_value = json!({ "paused": state.settings.paused });
                state.settings.paused = true;
                let new_value = json!({ "paused": state.settings.paused });
                let event = state.push_event(
                    EventClass::Critical,
                    "panic_close_triggered",
                    json!({ "paused": true, "action": "manual panic close trigger" }),
                );
                (old_value, new_value, state.settings.clone(), event)
            };

            ctx.db.save_runtime_settings(&settings_snapshot).await?;
            ctx.db.insert_event(&event, None, None, None).await?;

            Ok((
                json!({ "paused": true, "panic": true }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(old_value),
                    new_value: Some(new_value),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "ack_suggestion" => {
            let params: SuggestionParams = parse_params(req.params)?;
            let suggestion_id = params.id;

            let suggestion = {
                let mut state = ctx.state.write().await;
                state.pending_suggestions.remove(&suggestion_id)
            }
            .ok_or_else(|| anyhow!("unknown suggestion id"))?;

            let (replaced_wallet, tracked_after) =
                approve_suggestion_and_update_wallets(ctx, &suggestion).await?;

            ctx.db
                .set_rotation_suggestion_status(
                    &suggestion.id,
                    SuggestionStatus::Approved,
                    Utc::now(),
                )
                .await?;

            let event = {
                let mut state = ctx.state.write().await;
                state.last_rotation_at = Some(Utc::now());
                state.push_event(
                    EventClass::Rotation,
                    "rotation_approved",
                    json!({
                        "suggestion_id": suggestion.id,
                        "wallet": suggestion.wallet,
                        "replaced_wallet": replaced_wallet,
                    }),
                )
            };
            ctx.db
                .insert_event(&event, None, Some(&suggestion.wallet), None)
                .await?;

            Ok((
                json!({
                    "approved": true,
                    "suggestion_id": suggestion.id,
                    "wallet": suggestion.wallet,
                    "replaced_wallet": replaced_wallet,
                    "tracked_wallets": tracked_after,
                }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(json!({ "suggestion_status": "pending" })),
                    new_value: Some(json!({ "suggestion_status": "approved" })),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "reject_suggestion" => {
            let params: SuggestionParams = parse_params(req.params)?;
            let suggestion_id = params.id;

            let suggestion = {
                let mut state = ctx.state.write().await;
                state.pending_suggestions.remove(&suggestion_id)
            }
            .ok_or_else(|| anyhow!("unknown suggestion id"))?;

            ctx.db
                .set_rotation_suggestion_status(
                    &suggestion.id,
                    SuggestionStatus::Rejected,
                    Utc::now(),
                )
                .await?;

            let event = {
                let mut state = ctx.state.write().await;
                state.push_event(
                    EventClass::Rotation,
                    "rotation_rejected",
                    json!({ "suggestion_id": suggestion.id, "wallet": suggestion.wallet }),
                )
            };
            ctx.db
                .insert_event(&event, None, Some(&suggestion.wallet), None)
                .await?;

            Ok((
                json!({ "rejected": true, "suggestion_id": suggestion.id }),
                Some(AuditRecord {
                    command: method.to_string(),
                    old_value: Some(json!({ "suggestion_status": "pending" })),
                    new_value: Some(json!({ "suggestion_status": "rejected" })),
                    result: "ok".to_string(),
                    actor_chat_id: req.actor_chat_id,
                }),
            ))
        }
        "report_heartbeat" => {
            let params: HeartbeatParams = parse_params(req.params)?;
            let status = params.status.unwrap_or_else(|| "ok".to_string());
            let details = params.details.unwrap_or_else(|| json!({}));
            ctx.db
                .write_heartbeat(&params.service, &status, &details)
                .await
                .with_context(|| format!("writing heartbeat for {}", params.service))?;

            Ok((json!({ "ack": true }), None))
        }
        _ => anyhow::bail!("unsupported rpc method '{method}'"),
    }
}

async fn approve_suggestion_and_update_wallets(
    ctx: &AppContext,
    suggestion: &RotationSuggestion,
) -> Result<(Option<String>, Vec<String>)> {
    let target_wallet = suggestion.wallet.clone();
    let mut replaced_wallet: Option<String> = None;

    let (max_wallets, tracked_before) = {
        let state = ctx.state.read().await;
        (
            state.settings.max_active_wallets,
            state.tracked_wallets.iter().cloned().collect::<Vec<_>>(),
        )
    };

    if tracked_before.len() >= max_wallets {
        let scores = ctx
            .db
            .load_wallet_scores()
            .await
            .unwrap_or_else(|_| HashMap::new());
        let wallet_to_remove = tracked_before
            .iter()
            .min_by(|a, b| {
                let score_a = scores.get(*a).map(|s| s.score).unwrap_or(0.0);
                let score_b = scores.get(*b).map(|s| s.score).unwrap_or(0.0);
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned();

        if let Some(wallet) = wallet_to_remove {
            replaced_wallet = Some(wallet.clone());
            let mut state = ctx.state.write().await;
            state.tracked_wallets.remove(&wallet);
        }
    }

    {
        let mut state = ctx.state.write().await;
        state.tracked_wallets.insert(target_wallet.clone());
    }

    if let Some(wallet) = &replaced_wallet {
        ctx.db
            .set_wallet_active(wallet, "rotation_replace", false)
            .await?;
    }
    ctx.db
        .set_wallet_active(&target_wallet, "rotation_approved", true)
        .await?;

    let tracked_after = {
        let state = ctx.state.read().await;
        state.tracked_wallets.iter().cloned().collect::<Vec<_>>()
    };

    Ok((replaced_wallet, tracked_after))
}

fn parse_mode(raw: &str) -> Result<EngineMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "dry_run" => Ok(EngineMode::DryRun),
        "live" => Ok(EngineMode::Live),
        _ => anyhow::bail!("mode must be dry_run or live"),
    }
}

fn parse_risk_profile(raw: &str) -> Result<RiskProfile> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "conservative" => Ok(RiskProfile::Conservative),
        "balanced" => Ok(RiskProfile::Balanced),
        "aggressive" => Ok(RiskProfile::Aggressive),
        _ => anyhow::bail!("risk_profile must be conservative|balanced|aggressive"),
    }
}

fn parse_params<T: DeserializeOwned>(params: Value) -> Result<T> {
    serde_json::from_value(params).context("invalid params")
}

pub async fn run_rpc_server_with_backoff(ctx: AppContext) -> Result<()> {
    let mut attempt: u32 = 0;

    loop {
        match run_rpc_server(ctx.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                attempt = attempt.saturating_add(1);
                let delay_ms = compute_backoff_delay(250, 5_000, attempt);
                tracing::error!(error = %err, attempt, delay_ms, "rpc server failed; retrying");
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;
    use tokio::sync::RwLock;

    use crate::config::EngineConfig;
    use crate::db::Db;
    use crate::state::EngineState;
    use crate::types::{EventClass, RpcRequest, RuntimeSettings};

    use super::*;

    async fn make_ctx(dir: &tempfile::TempDir) -> AppContext {
        let db_path = dir.path().join("test.db");
        let socket_path = dir.path().join("engine.sock");
        let db = Db::new(db_path.display().to_string());
        db.init().await.expect("db init");
        AppContext {
            state: Arc::new(RwLock::new(EngineState::new(
                RuntimeSettings::default(),
                BTreeSet::new(),
            ))),
            db,
            config: EngineConfig {
                db_path: db_path.display().to_string(),
                socket_path: socket_path.display().to_string(),
                data_api_base: "https://data-api.example.com".to_string(),
                execution_api_base: None,
                allow_live_simulation: false,
                fetch_interval_ms: 1_500,
                rotation_interval_ms: 60_000,
                heartbeat_interval_ms: 5_000,
                request_timeout_ms: 2_500,
                network_retry_limit: 3,
                rotation_rank_delta_threshold: 8.0,
                rotation_confidence_floor: 52.0,
                rotation_cooldown_secs: 1_800,
                source_equity_fallback_usd: 100_000.0,
                log_dir: dir.path().join("logs").display().to_string(),
                log_retention_days: 7,
                rpcs: vec![],
                log_level: "info".to_string(),
                command_tail_default: 50,
                runtime_settings: RuntimeSettings::default(),
            },
            started_at: Utc::now(),
        }
    }

    #[test]
    fn parse_mode_rejects_invalid_values() {
        assert!(parse_mode("paper").is_err());
        assert!(matches!(parse_mode("dry_run"), Ok(EngineMode::DryRun)));
    }

    #[tokio::test]
    async fn rpc_add_wallet_and_list_wallets() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("rpc_test.db");
        let socket_path = dir.path().join("engine.sock");
        let db = Db::new(db_path.display().to_string());
        db.init().await.expect("db init");

        let ctx = AppContext {
            state: Arc::new(RwLock::new(EngineState::new(
                RuntimeSettings::default(),
                BTreeSet::new(),
            ))),
            db,
            config: EngineConfig {
                db_path: db_path.display().to_string(),
                socket_path: socket_path.display().to_string(),
                data_api_base: "https://data-api.polymarket.com".to_string(),
                execution_api_base: None,
                allow_live_simulation: false,
                fetch_interval_ms: 1_500,
                rotation_interval_ms: 60_000,
                heartbeat_interval_ms: 5_000,
                request_timeout_ms: 2_500,
                network_retry_limit: 3,
                rotation_rank_delta_threshold: 8.0,
                rotation_confidence_floor: 52.0,
                rotation_cooldown_secs: 1_800,
                source_equity_fallback_usd: 100_000.0,
                log_dir: dir.path().join("logs").display().to_string(),
                log_retention_days: 7,
                rpcs: vec![],
                log_level: "info".to_string(),
                command_tail_default: 50,
                runtime_settings: RuntimeSettings::default(),
            },
            started_at: Utc::now(),
        };

        let add_response = handle_rpc_request(
            RpcRequest {
                method: "add_wallet".to_string(),
                params: json!({ "wallet": "0x1111111111111111111111111111111111111111" }),
                actor_chat_id: Some("123".to_string()),
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(add_response.ok);

        let list_response = handle_rpc_request(
            RpcRequest {
                method: "list_wallets".to_string(),
                params: json!({}),
                actor_chat_id: Some("123".to_string()),
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(list_response.ok);
        let wallets = list_response
            .result
            .and_then(|value| value.get("wallets").cloned())
            .expect("wallets field present");
        assert!(
            wallets
                .to_string()
                .contains("0x1111111111111111111111111111111111111111")
        );
    }

    #[tokio::test]
    async fn rpc_set_copy_sells_toggles_and_persists() {
        let dir = tempdir().expect("tempdir");
        let ctx = make_ctx(&dir).await;

        let response = handle_rpc_request(
            RpcRequest {
                method: "set_copy_sells".to_string(),
                params: json!({ "copy_sells": false }),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;

        assert!(response.ok);
        let state = ctx.state.read().await;
        assert!(!state.settings.copy_sells);
    }

    #[tokio::test]
    async fn rpc_pause_and_resume_trading() {
        let dir = tempdir().expect("tempdir");
        let ctx = make_ctx(&dir).await;

        let pause_response = handle_rpc_request(
            RpcRequest {
                method: "pause_trading".to_string(),
                params: json!({}),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(pause_response.ok);
        {
            let state = ctx.state.read().await;
            assert!(state.settings.paused);
        }

        let resume_response = handle_rpc_request(
            RpcRequest {
                method: "resume_trading".to_string(),
                params: json!({}),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(resume_response.ok);
        {
            let state = ctx.state.read().await;
            assert!(!state.settings.paused);
        }
    }

    #[tokio::test]
    async fn rpc_set_multiplier_rejects_non_positive() {
        let dir = tempdir().expect("tempdir");
        let ctx = make_ctx(&dir).await;

        // multiplier = 0 → error
        let r0 = handle_rpc_request(
            RpcRequest {
                method: "set_multiplier".to_string(),
                params: json!({ "multiplier": 0.0 }),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(!r0.ok);

        // multiplier = -1 → error
        let r_neg = handle_rpc_request(
            RpcRequest {
                method: "set_multiplier".to_string(),
                params: json!({ "multiplier": -1.0 }),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(!r_neg.ok);

        // multiplier = 2.0 → ok
        let r_ok = handle_rpc_request(
            RpcRequest {
                method: "set_multiplier".to_string(),
                params: json!({ "multiplier": 2.0 }),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(r_ok.ok);
    }

    #[tokio::test]
    async fn rpc_set_follower_equity_rejects_non_positive() {
        let dir = tempdir().expect("tempdir");
        let ctx = make_ctx(&dir).await;

        // follower_equity_usd = 0 → error
        let r0 = handle_rpc_request(
            RpcRequest {
                method: "set_follower_equity".to_string(),
                params: json!({ "follower_equity_usd": 0.0 }),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(!r0.ok);

        // follower_equity_usd = 5000 → ok
        let r_ok = handle_rpc_request(
            RpcRequest {
                method: "set_follower_equity".to_string(),
                params: json!({ "follower_equity_usd": 5_000.0 }),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;
        assert!(r_ok.ok);
    }

    #[tokio::test]
    async fn rpc_remove_wallet_returns_noop_for_unknown() {
        let dir = tempdir().expect("tempdir");
        let ctx = make_ctx(&dir).await;

        // Wallet is not in tracked_wallets → removed = false
        let response = handle_rpc_request(
            RpcRequest {
                method: "remove_wallet".to_string(),
                params: json!({ "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;

        assert!(response.ok);
        let result = response.result.expect("result present");
        assert_eq!(result.get("removed").and_then(|v| v.as_bool()), Some(false));
    }

    #[tokio::test]
    async fn rpc_panic_close_emits_critical_event() {
        let dir = tempdir().expect("tempdir");
        let ctx = make_ctx(&dir).await;

        let response = handle_rpc_request(
            RpcRequest {
                method: "panic_close".to_string(),
                params: json!({}),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;

        assert!(response.ok);

        let state = ctx.state.read().await;
        assert!(state.settings.paused);

        let has_critical = state
            .event_buffer
            .iter()
            .any(|e| matches!(e.class, EventClass::Critical));
        assert!(has_critical);
    }

    #[tokio::test]
    async fn rpc_unknown_method_returns_error() {
        let dir = tempdir().expect("tempdir");
        let ctx = make_ctx(&dir).await;

        let response = handle_rpc_request(
            RpcRequest {
                method: "not_a_real_method".to_string(),
                params: json!({}),
                actor_chat_id: None,
                request_id: None,
            },
            &ctx,
        )
        .await;

        assert!(!response.ok);
        let error = response.error.expect("error message present");
        assert!(error.contains("not_a_real_method"));
    }
}
