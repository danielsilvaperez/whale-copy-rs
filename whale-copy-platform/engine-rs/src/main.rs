use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::time::SystemTime;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde_json::json;
use tokio::sync::RwLock;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, prelude::*};
use whale_copy_engine::config::{EngineConfig, load_config};
use whale_copy_engine::db::Db;
use whale_copy_engine::rpc::{AppContext, run_rpc_server_with_backoff};
use whale_copy_engine::signals::{
    build_copy_signal, build_execution_intent, evaluate_signal, execute_intent, fetch_wallet_activity,
    fetch_wallet_candidates, maybe_create_rotation_suggestion, score_wallet,
};
use whale_copy_engine::state::EngineState;
use whale_copy_engine::types::{EventClass, RiskDecision, RuntimeSettings, SourceFillEvent, TradeSide};

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    let _log_guard = init_logging(&config)?;

    let db = Db::new(config.db_path.clone());
    db.init().await?;

    let settings = resolve_settings(&db, &config).await?;
    let tracked_wallets = resolve_tracked_wallets(&db).await?;
    let pending_suggestions = db.load_pending_suggestions().await?;

    let mut state = EngineState::new(settings, tracked_wallets);
    state.pending_suggestions = pending_suggestions;

    let ctx = AppContext {
        state: std::sync::Arc::new(RwLock::new(state)),
        db: db.clone(),
        config: config.clone(),
        started_at: Utc::now(),
    };

    let client = Client::builder()
        .tcp_nodelay(true)
        .pool_max_idle_per_host(32)
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .build()
        .context("building reqwest client")?;

    let heartbeat_task = tokio::spawn(heartbeat_loop(ctx.clone()));
    let signal_task = tokio::spawn(signal_loop(ctx.clone(), client.clone()));
    let rotation_task = tokio::spawn(rotation_loop(ctx.clone(), client.clone()));
    let log_retention_task = tokio::spawn(log_retention_loop(
        config.log_dir.clone(),
        config.log_retention_days,
    ));
    let rpc_task = tokio::spawn(run_rpc_server_with_backoff(ctx.clone()));

    tracing::info!("engine started");

    tokio::signal::ctrl_c().await.context("listening for ctrl-c")?;
    tracing::info!("shutdown signal received");

    heartbeat_task.abort();
    signal_task.abort();
    rotation_task.abort();
    log_retention_task.abort();
    rpc_task.abort();

    let _ = tokio::join!(
        heartbeat_task,
        signal_task,
        rotation_task,
        log_retention_task,
        rpc_task
    );
    tracing::info!("engine stopped");
    Ok(())
}

fn init_logging(config: &EngineConfig) -> Result<WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.log_level.clone()))
        .context("configuring tracing filter")?;

    fs::create_dir_all(&config.log_dir)
        .with_context(|| format!("creating log dir {}", config.log_dir))?;

    prune_old_logs(&config.log_dir, config.log_retention_days)?;

    let file_appender = tracing_appender::rolling::hourly(&config.log_dir, "engine.jsonl");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let stdout_layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false);

    let file_layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(file_writer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(guard)
}

async fn resolve_settings(db: &Db, config: &EngineConfig) -> Result<RuntimeSettings> {
    match db.load_runtime_settings().await? {
        Some(settings) => Ok(settings),
        None => {
            db.save_runtime_settings(&config.runtime_settings).await?;
            Ok(config.runtime_settings.clone())
        }
    }
}

async fn resolve_tracked_wallets(db: &Db) -> Result<BTreeSet<String>> {
    db.load_wallet_allowlist().await
}

async fn heartbeat_loop(ctx: AppContext) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(ctx.config.heartbeat_interval_ms));

    loop {
        interval.tick().await;

        let snapshot = {
            let mut state = ctx.state.write().await;
            state.last_heartbeat_at = Utc::now();
            state.health_snapshot()
        };

        ctx.db
            .write_heartbeat("engine", "ok", &json!({ "snapshot": snapshot }))
            .await
            .context("writing engine heartbeat")?;
    }
}

async fn signal_loop(ctx: AppContext, client: Client) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(ctx.config.fetch_interval_ms));

    loop {
        interval.tick().await;

        let wallets = {
            let state = ctx.state.read().await;
            state.tracked_wallets.iter().cloned().collect::<Vec<_>>()
        };

        if wallets.is_empty() {
            continue;
        }

        for wallet in wallets {
            let events = match fetch_wallet_activity(
                &client,
                &ctx.config.data_api_base,
                &wallet,
                ctx.config.source_equity_fallback_usd,
            )
            .await
            {
                Ok(events) => events,
                Err(err) => {
                    tracing::debug!(wallet = %wallet, error = %err, "wallet activity fetch failed");
                    continue;
                }
            };

            for fill_event in events {
                if let Err(err) = process_fill_event(&ctx, &client, fill_event).await {
                    tracing::warn!(error = %err, "failed processing source fill");
                }
            }
        }
    }
}

async fn process_fill_event(ctx: &AppContext, client: &Client, fill_event: SourceFillEvent) -> Result<()> {
    let (settings, market_exposure, daily_notional, open_positions, is_duplicate) = {
        let mut state = ctx.state.write().await;
        let duplicate = !state.dedupe_cache.add(&fill_event.execution_id);
        (
            state.settings.clone(),
            *state
                .market_exposure_usd
                .get(&fill_event.market_slug)
                .unwrap_or(&0.0),
            state.daily_notional_usd,
            state.open_positions_count,
            duplicate,
        )
    };

    if is_duplicate {
        return Ok(());
    }

    record_event(
        ctx,
        EventClass::Trade,
        "signal_detected",
        json!({
            "execution_id": fill_event.execution_id,
            "wallet": fill_event.source_wallet,
            "market_slug": fill_event.market_slug,
            "notional_usd": fill_event.source_trade_notional_usd,
        }),
        Some(&fill_event.execution_id),
        Some(&fill_event.source_wallet),
        Some(&fill_event.market_slug),
    )
    .await?;

    if settings.paused {
        let payload = json!({
            "execution_id": fill_event.execution_id,
            "reason": "trading_paused",
        });
        ctx.db
            .insert_risk_event(Some(&fill_event.execution_id), "block", "trading_paused", &payload)
            .await?;

        record_event(
            ctx,
            EventClass::Risk,
            "signal_blocked",
            payload,
            Some(&fill_event.execution_id),
            Some(&fill_event.source_wallet),
            Some(&fill_event.market_slug),
        )
        .await?;

        return Ok(());
    }

    let Some(raw_signal) = build_copy_signal(&fill_event, &settings, ctx.config.source_equity_fallback_usd) else {
        return Ok(());
    };

    let risk_eval = evaluate_signal(
        &raw_signal,
        &settings,
        market_exposure,
        daily_notional,
        open_positions,
    );

    let allowed_signal = match (&risk_eval.decision, &risk_eval.adjusted_signal) {
        (RiskDecision::Allow { .. }, Some(signal)) => signal.clone(),
        (RiskDecision::Block { reason }, _) => {
            let payload = json!({
                "execution_id": raw_signal.execution_id,
                "reason": reason,
                "edge_bps": raw_signal.expected_edge_bps,
                "cost_buffer_bps": risk_eval.total_cost_buffer_bps,
            });

            ctx.db
                .insert_risk_event(
                    Some(&raw_signal.execution_id),
                    "block",
                    reason,
                    &payload,
                )
                .await?;

            record_event(
                ctx,
                EventClass::Risk,
                "signal_blocked",
                payload,
                Some(&raw_signal.execution_id),
                Some(&raw_signal.source_wallet),
                Some(&raw_signal.market_slug),
            )
            .await?;
            return Ok(());
        }
        _ => {
            return Ok(());
        }
    };

    let intent = build_execution_intent(&allowed_signal, &settings);
    let request_json = serde_json::to_value(&intent).context("serializing execution intent")?;

    let result = execute_intent(
        client,
        settings.mode,
        ctx.config.execution_api_base.as_deref(),
        &settings,
        &intent,
    )
    .await;

    let order_id = format!("order-{}", &intent.execution_id);
    let side = match intent.side {
        TradeSide::Buy => "buy",
        TradeSide::Sell => "sell",
    };

    ctx.db
        .insert_order(
            &order_id,
            &intent.execution_id,
            &intent.market_slug,
            side,
            "LIMIT_IOC",
            &request_json,
            &result,
        )
        .await?;

    if result.success {
        {
            let mut state = ctx.state.write().await;
            state.daily_notional_usd += allowed_signal.copy_notional_usd;
            let exposure = state
                .market_exposure_usd
                .entry(allowed_signal.market_slug.clone())
                .or_insert(0.0);

            match allowed_signal.side {
                TradeSide::Buy => {
                    *exposure += allowed_signal.copy_notional_usd;
                    state.open_positions_count = state.open_positions_count.saturating_add(1);
                }
                TradeSide::Sell => {
                    *exposure = (*exposure - allowed_signal.copy_notional_usd).max(0.0);
                    state.open_positions_count = state.open_positions_count.saturating_sub(1);
                }
            }
        }

        record_event(
            ctx,
            EventClass::Trade,
            "order_executed",
            json!({
                "execution_id": intent.execution_id,
                "market_slug": intent.market_slug,
                "side": side,
                "quantity": intent.quantity,
                "filled_quantity": result.filled_quantity,
                "latency_ms": result.latency_ms,
                "attempts": result.attempts,
                "message": result.message,
            }),
            Some(&intent.execution_id),
            Some(&allowed_signal.source_wallet),
            Some(&intent.market_slug),
        )
        .await?;
    } else {
        record_event(
            ctx,
            EventClass::Trade,
            "order_failed",
            json!({
                "execution_id": intent.execution_id,
                "market_slug": intent.market_slug,
                "side": side,
                "quantity": intent.quantity,
                "latency_ms": result.latency_ms,
                "attempts": result.attempts,
                "message": result.message,
            }),
            Some(&intent.execution_id),
            Some(&allowed_signal.source_wallet),
            Some(&intent.market_slug),
        )
        .await?;
    }

    Ok(())
}

async fn rotation_loop(ctx: AppContext, client: Client) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(ctx.config.rotation_interval_ms));

    loop {
        interval.tick().await;

        let candidates = match fetch_wallet_candidates(&client, &ctx.config.data_api_base).await {
            Ok(candidates) => candidates,
            Err(err) => {
                tracing::debug!(error = %err, "wallet candidate fetch failed");
                continue;
            }
        };

        if candidates.is_empty() {
            continue;
        }

        let now = Utc::now();
        let wallet_scores = candidates
            .iter()
            .map(|candidate| score_wallet(candidate, now))
            .collect::<Vec<_>>();

        ctx.db.upsert_wallet_scores(&wallet_scores).await?;

        let score_map: HashMap<String, whale_copy_engine::types::WalletScore> = wallet_scores
            .into_iter()
            .map(|score| (score.wallet.clone(), score))
            .collect();

        let suggestion = {
            let state = ctx.state.read().await;
            maybe_create_rotation_suggestion(
                &state.tracked_wallets,
                &state.pending_suggestions,
                &score_map,
                state.settings.max_active_wallets,
                state.last_rotation_at,
                now,
                ctx.config.rotation_rank_delta_threshold,
                ctx.config.rotation_confidence_floor,
                Duration::from_secs(ctx.config.rotation_cooldown_secs),
            )
        };

        let Some(suggestion) = suggestion else {
            continue;
        };

        ctx.db.save_rotation_suggestion(&suggestion).await?;

        {
            let mut state = ctx.state.write().await;
            state
                .pending_suggestions
                .insert(suggestion.id.clone(), suggestion.clone());
            state.last_rotation_at = Some(now);
        }

        record_event(
            &ctx,
            EventClass::Rotation,
            "rotation_suggestion_created",
            json!({
                "id": suggestion.id,
                "wallet": suggestion.wallet,
                "score": suggestion.score,
                "reason": suggestion.reason,
                "expected_impact": suggestion.expected_impact,
            }),
            None,
            Some(&suggestion.wallet),
            None,
        )
        .await?;
    }
}

async fn log_retention_loop(log_dir: String, retention_days: u32) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
    loop {
        interval.tick().await;
        if let Err(err) = prune_old_logs(&log_dir, retention_days) {
            tracing::warn!(error = %err, log_dir = %log_dir, "failed pruning old logs");
        }
    }
}

fn prune_old_logs(log_dir: &str, retention_days: u32) -> Result<()> {
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(retention_days as u64 * 24 * 60 * 60))
        .unwrap_or(SystemTime::UNIX_EPOCH);

    for entry in fs::read_dir(log_dir).with_context(|| format!("reading log dir {log_dir}"))? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(filename) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        if !filename.contains("engine.jsonl") {
            continue;
        }

        let metadata = entry.metadata()?;
        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if modified < cutoff {
            fs::remove_file(&path).with_context(|| format!("removing old log {}", path.display()))?;
        }
    }

    Ok(())
}

async fn record_event(
    ctx: &AppContext,
    class: EventClass,
    event_type: &str,
    payload: serde_json::Value,
    execution_id: Option<&str>,
    wallet: Option<&str>,
    market_slug: Option<&str>,
) -> Result<()> {
    let event = {
        let mut state = ctx.state.write().await;
        state.push_event(class, event_type, payload)
    };

    ctx.db
        .insert_event(&event, execution_id, wallet, market_slug)
        .await
        .with_context(|| format!("persisting event {}", event.event_type))?;

    Ok(())
}
