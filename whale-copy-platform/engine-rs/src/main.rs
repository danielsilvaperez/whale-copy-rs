use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::time::{Duration, Instant};
use std::time::SystemTime;

use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde_json::json;
use tokio::sync::{mpsc, RwLock};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, prelude::*};
use whale_copy_engine::config::{EngineConfig, load_config};
use whale_copy_engine::db::Db;
use whale_copy_engine::rpc::{AppContext, run_rpc_server_with_backoff};
use whale_copy_engine::signals::{
    build_copy_signal, build_execution_intent, evaluate_signal, execute_intent,
    fetch_wallet_activity, fetch_wallet_candidates, maybe_create_rotation_suggestion, score_wallet,
};
use whale_copy_engine::state::EngineState;
use whale_copy_engine::types::{
    EventClass, RiskDecision, RuntimeSettings, SourceFillEvent, TradeSide, WsChannel, WsEvent, WsMessage,
};
use whale_copy_engine::reconciliation::reconciliation_loop;
use whale_copy_engine::ws_client::{WsClient, WsCommand, trade_fill_to_source_event};

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

    // Create WebSocket event channel
    let (ws_event_tx, ws_event_rx) = mpsc::channel::<WsEvent>(1000);
    let (ws_client, ws_command_tx) = WsClient::new(config.ws_config.clone(), ws_event_tx);

    let heartbeat_task = tokio::spawn(heartbeat_loop(ctx.clone()));

    // Choose between WebSocket and REST polling for signals
    let (signal_task, ws_task): (
        tokio::task::JoinHandle<Result<()>>,
        Option<tokio::task::JoinHandle<()>>,
    ) = if config.use_websocket {
        tracing::info!("Using WebSocket for real-time signal ingestion");

        // Start WebSocket client
        let ws_handle = tokio::spawn(ws_client.run());

        // Subscribe to tracked wallets' trade channels
        let wallets: Vec<String> = {
            let state = ctx.state.read().await;
            state.tracked_wallets.iter().cloned().collect()
        };

        let channels: Vec<WsChannel> = wallets
            .iter()
            .map(|w| WsChannel::User { wallet: w.clone() })
            .collect();

        if !channels.is_empty() {
            let _ = ws_command_tx.send(WsCommand::Subscribe(channels)).await;
        }

        // Spawn WebSocket event processor
        let ws_event_task = tokio::spawn(ws_event_processor(
            ctx.clone(),
            client.clone(),
            ws_event_rx,
            ws_command_tx.clone(),
        ));

        (ws_event_task, Some(ws_handle))
    } else {
        tracing::info!("Using REST polling for signal ingestion");
        (tokio::spawn(signal_loop(ctx.clone(), client.clone())), None)
    };

    let rotation_task = tokio::spawn(rotation_loop(ctx.clone(), client.clone()));
    let log_retention_task = tokio::spawn(log_retention_loop(
        config.log_dir.clone(),
        config.log_retention_days,
    ));
    
    // Start position reconciliation loop
    let reconciliation_task = tokio::spawn(reconciliation_loop(
        ctx.state.clone(),
        client.clone(),
        config.reconciliation_config.clone(),
        std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        config.execution_api_base.clone(),
    ));
    
    let rpc_task = tokio::spawn(run_rpc_server_with_backoff(ctx.clone()));

    tracing::info!("engine started");

    tokio::signal::ctrl_c()
        .await
        .context("listening for ctrl-c")?;
    tracing::info!("shutdown signal received");

    // Send shutdown to WebSocket if running
    let _ = ws_command_tx.send(WsCommand::Shutdown).await;

    heartbeat_task.abort();
    signal_task.abort();
    rotation_task.abort();
    log_retention_task.abort();
    reconciliation_task.abort();
    rpc_task.abort();
    if let Some(ws) = ws_task {
        ws.abort();
    }

    let _ = tokio::join!(
        heartbeat_task,
        signal_task,
        rotation_task,
        log_retention_task,
        reconciliation_task,
        rpc_task
    );
    tracing::info!("engine stopped");
    Ok(())
}

/// Process WebSocket events and convert to signals
async fn ws_event_processor(
    ctx: AppContext,
    client: Client,
    mut event_rx: mpsc::Receiver<WsEvent>,
    command_tx: mpsc::Sender<WsCommand>,
) -> Result<()> {
    let mut last_wallet_sync = Instant::now();
    let wallet_sync_interval = Duration::from_secs(60);

    while let Some(event) = event_rx.recv().await {
        match event {
            WsEvent::Connected => {
                tracing::info!("WebSocket connected, syncing subscriptions");

                // Re-subscribe to current tracked wallets
                let wallets: Vec<String> = {
                    let state = ctx.state.read().await;
                    state.tracked_wallets.iter().cloned().collect()
                };

                let channels: Vec<WsChannel> = wallets
                    .iter()
                    .map(|w| WsChannel::User { wallet: w.clone() })
                    .collect();

                if !channels.is_empty() {
                    let _ = command_tx.send(WsCommand::Subscribe(channels)).await;
                }
            }

            WsEvent::Disconnected { reason } => {
                tracing::warn!(reason = %reason, "WebSocket disconnected, will reconnect");
            }

            WsEvent::Message(msg) => {
                match msg {
                    WsMessage::Trade { market_slug, trades, .. } => {
                        // Get current tracked wallets
                        let tracked: BTreeSet<String> = {
                            let state = ctx.state.read().await;
                            state.tracked_wallets.clone()
                        };

                        // Process each trade that involves a tracked wallet
                        for trade in trades {
                            let is_maker_tracked = tracked.contains(&trade.maker_address);
                            let is_taker_tracked = tracked.contains(&trade.taker_address);

                            if is_maker_tracked || is_taker_tracked {
                                // Use the tracked wallet as the source
                                let source_wallet = if is_taker_tracked {
                                    &trade.taker_address
                                } else {
                                    &trade.maker_address
                                };

                                let fallback_equity = ctx.config.source_equity_fallback_usd;

                                if let Some(fill_event) =
                                    trade_fill_to_source_event(&market_slug, &trade, fallback_equity)
                                {
                                    // Adjust source wallet to the tracked one
                                    let mut fill_event = fill_event;
                                    fill_event.source_wallet = source_wallet.clone();

                                    if let Err(e) =
                                        process_fill_event(&ctx, &client, fill_event).await
                                    {
                                        tracing::warn!(error = %e, "Failed to process WS fill event");
                                    }
                                }
                            }
                        }
                    }

                    WsMessage::UserFill { fills, .. } => {
                        // Process our own fill events for position tracking
                        for fill in fills {
                            tracing::info!(
                                order_id = %fill.order_id,
                                market = %fill.market_slug,
                                side = %fill.side.as_str(),
                                size = fill.size,
                                price = fill.price,
                                "Own order filled"
                            );

                            // Update position tracking
                            let mut state = ctx.state.write().await;
                            let exposure = state
                                .market_exposure_usd
                                .entry(fill.market_slug.clone())
                                .or_insert(0.0);

                            match fill.side {
                                TradeSide::Buy => {
                                    *exposure += fill.size * fill.price;
                                }
                                TradeSide::Sell => {
                                    *exposure = (*exposure - fill.size * fill.price).max(0.0);
                                }
                            }
                        }
                    }

                    WsMessage::Orderbook { market_slug, bids, asks, .. } => {
                        // Could use for price improvement or slippage estimation
                        tracing::trace!(
                            market = %market_slug,
                            bid_count = bids.len(),
                            ask_count = asks.len(),
                            "Orderbook update"
                        );
                    }

                    WsMessage::Error { message, code } => {
                        tracing::error!(message = %message, ?code, "WebSocket error message");
                    }

                    _ => {}
                }
            }

            WsEvent::Error(error) => {
                tracing::error!(error = %error, "WebSocket client error");
            }
        }

        // Periodic wallet list sync
        if last_wallet_sync.elapsed() > wallet_sync_interval {
            last_wallet_sync = Instant::now();

            let current_wallets: Vec<String> = {
                let state = ctx.state.read().await;
                state.tracked_wallets.iter().cloned().collect()
            };

            // Subscribe to any new wallets
            let channels: Vec<WsChannel> = current_wallets
                .iter()
                .map(|w| WsChannel::User { wallet: w.clone() })
                .collect();

            if !channels.is_empty() {
                let _ = command_tx.send(WsCommand::Subscribe(channels)).await;
            }
        }
    }

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
    let mut interval =
        tokio::time::interval(Duration::from_millis(ctx.config.heartbeat_interval_ms));

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

        {
            let mut state = ctx.state.write().await;
            state.maybe_reset_daily_notional();
        }

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

async fn process_fill_event(
    ctx: &AppContext,
    client: &Client,
    fill_event: SourceFillEvent,
) -> Result<()> {
    let (settings, market_exposure, daily_notional, open_positions, is_duplicate, auto_stop_check) = {
        let mut state = ctx.state.write().await;
        let duplicate = !state.dedupe_cache.add(&fill_event.execution_id);
        
        // Check if wallet has been auto-stopped
        let auto_stop_reason = state.performance_tracker.check_auto_stop(&fill_event.source_wallet);
        
        (
            state.settings.clone(),
            *state
                .market_exposure_usd
                .get(&fill_event.market_slug)
                .unwrap_or(&0.0),
            state.daily_notional_usd,
            state.open_positions_count,
            duplicate,
            auto_stop_reason,
        )
    };

    if is_duplicate {
        return Ok(());
    }

    // Check auto-stop before processing
    if let Some(reason) = auto_stop_check {
        let payload = json!({
            "execution_id": fill_event.execution_id,
            "wallet": fill_event.source_wallet,
            "reason": reason.message(),
        });
        
        ctx.db
            .insert_risk_event(
                Some(&fill_event.execution_id),
                "block",
                "auto_stop_triggered",
                &payload,
            )
            .await?;

        record_event(
            ctx,
            EventClass::Risk,
            "signal_blocked_auto_stop",
            payload,
            Some(&fill_event.execution_id),
            Some(&fill_event.source_wallet),
            Some(&fill_event.market_slug),
        )
        .await?;

        // Mark wallet as stopped if not already
        {
            let mut state = ctx.state.write().await;
            if !state.performance_tracker.is_stopped(&fill_event.source_wallet) {
                state.performance_tracker.mark_stopped(&fill_event.source_wallet);
                tracing::warn!(
                    wallet = %fill_event.source_wallet,
                    reason = %reason.message(),
                    "Auto-stopped copying wallet due to poor performance"
                );
            }
        }

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
            .insert_risk_event(
                Some(&fill_event.execution_id),
                "block",
                "trading_paused",
                &payload,
            )
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

    if !settings.copy_sells && matches!(fill_event.side, TradeSide::Sell) {
        let payload = json!({
            "execution_id": fill_event.execution_id,
            "reason": "copy_sells_disabled",
        });
        ctx.db
            .insert_risk_event(
                Some(&fill_event.execution_id),
                "block",
                "copy_sells_disabled",
                &payload,
            )
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

    let Some(raw_signal) = build_copy_signal(
        &fill_event,
        &settings,
        ctx.config.source_equity_fallback_usd,
    ) else {
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
                .insert_risk_event(Some(&raw_signal.execution_id), "block", reason, &payload)
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
        ctx.config.allow_live_simulation,
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
        // Calculate P&L for performance tracking (simplified - in real implementation
        // you'd track actual fill price vs entry price for closed positions)
        let estimated_pnl = if let Some(avg_price) = result.average_fill_price {
            let price_diff = avg_price - allowed_signal.source_price;
            let pnl = price_diff * result.filled_quantity;
            match allowed_signal.side {
                TradeSide::Buy => -pnl, // Buy = paying, negative until sold
                TradeSide::Sell => pnl,  // Sell = receiving
            }
        } else {
            0.0
        };

        {
            let mut state = ctx.state.write().await;
            state.daily_notional_usd += allowed_signal.copy_notional_usd;
            
            // Record trade performance
            state.performance_tracker.record_trade(
                &allowed_signal.source_wallet,
                estimated_pnl,
            );
            
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
                "estimated_pnl": estimated_pnl,
            }),
            Some(&intent.execution_id),
            Some(&allowed_signal.source_wallet),
            Some(&intent.market_slug),
        )
        .await?;
    } else {
        let failure_payload = json!({
            "execution_id": intent.execution_id,
            "market_slug": intent.market_slug,
            "side": side,
            "quantity": intent.quantity,
            "attempts": result.attempts,
            "message": result.message,
        });

        ctx.db
            .insert_risk_event(
                Some(&intent.execution_id),
                "block",
                "execution_failed",
                &failure_payload,
            )
            .await?;

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
    let mut interval =
        tokio::time::interval(Duration::from_millis(ctx.config.rotation_interval_ms));

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
            fs::remove_file(&path)
                .with_context(|| format!("removing old log {}", path.display()))?;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use chrono::Utc;
    use reqwest::Client;
    use rusqlite::Connection;
    use tempfile::tempdir;
    use tokio::sync::RwLock;

    use super::*;
    use whale_copy_engine::types::WsConfig;

    fn test_engine_config(db_path: String) -> EngineConfig {
        EngineConfig {
            db_path,
            socket_path: "/tmp/whale-copy-engine-test.sock".to_string(),
            data_api_base: "https://data-api.polymarket.com".to_string(),
            execution_api_base: None,
            allow_live_simulation: false,
            fetch_interval_ms: 1_500,
            rotation_interval_ms: 120_000,
            heartbeat_interval_ms: 5_000,
            request_timeout_ms: 3_500,
            network_retry_limit: 3,
            rotation_rank_delta_threshold: 8.0,
            rotation_confidence_floor: 52.0,
            rotation_cooldown_secs: 1_800,
            source_equity_fallback_usd: 100_000.0,
            log_dir: "./logs".to_string(),
            log_retention_days: 7,
            rpcs: Vec::new(),
            log_level: "info".to_string(),
            command_tail_default: 50,
            runtime_settings: RuntimeSettings::default(),
            use_websocket: true,
            ws_config: WsConfig::default(),
            reconciliation_config: whale_copy_engine::reconciliation::ReconciliationConfig::default(),
        }
    }

    #[tokio::test]
    async fn process_fill_event_records_failed_order_and_risk_event_when_live_api_missing() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("engine-test.db");
        let db_path_str = db_path.display().to_string();
        let db = Db::new(db_path_str.clone());
        db.init().await.expect("init db");

        let mut settings = RuntimeSettings::default();
        settings.mode = whale_copy_engine::types::EngineMode::Live;
        settings.follower_equity_usd = 20_000.0;

        let state = EngineState::new(settings.clone(), BTreeSet::new());
        let ctx = AppContext {
            state: Arc::new(RwLock::new(state)),
            db: db.clone(),
            config: test_engine_config(db_path_str.clone()),
            started_at: Utc::now(),
        };

        let fill_event = SourceFillEvent {
            execution_id: "exec-live-guard-1".to_string(),
            source_wallet: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            market_slug: "market-live-guard".to_string(),
            token_id: "token-live-guard".to_string(),
            side: TradeSide::Buy,
            source_trade_notional_usd: 1_000.0,
            source_price: 0.5,
            source_quantity: 2_000.0,
            source_equity_usd: 100_000.0,
            expected_edge_bps: 120.0,
            observed_at: Utc::now(),
            transaction_hash: None,
        };

        let client = Client::new();
        process_fill_event(&ctx, &client, fill_event)
            .await
            .expect("process fill event");

        let conn = Connection::open(&db_path_str).expect("open sqlite");
        let (status, response_json): (String, String) = conn
            .query_row(
                "SELECT status, response_json FROM orders WHERE execution_id = ?1 LIMIT 1",
                ["exec-live-guard-1"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("failed order row");
        assert_eq!(status, "failed");
        assert!(response_json.contains("live_mode_requires_execution_api"));

        let (reason, payload_json): (String, String) = conn
            .query_row(
                "SELECT reason, payload_json FROM risk_events WHERE execution_id = ?1 ORDER BY created_at DESC LIMIT 1",
                ["exec-live-guard-1"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("risk event row");
        assert_eq!(reason, "execution_failed");
        assert!(payload_json.contains("live_mode_requires_execution_api"));
    }
}
