use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::types::{
    CopySignal, EngineMode, ExecutionIntent, ExecutionResult, OrderStyle, RiskDecision, RotationSuggestion,
    RuntimeSettings, SourceFillEvent, SuggestionStatus, TradeSide, WalletScore,
};

const DEFAULT_EVENT_LOOKBACK_LIMIT: usize = 25;

#[derive(Debug, Clone)]
pub struct CandidateWalletMetrics {
    pub wallet: String,
    pub pnl_consistency: f64,
    pub win_rate: f64,
    pub drawdown_penalty: f64,
    pub recent_activity: f64,
    pub fill_quality: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct RiskEvaluation {
    pub decision: RiskDecision,
    pub adjusted_signal: Option<CopySignal>,
    pub total_cost_buffer_bps: f64,
}

pub fn compute_backoff_delay(base_ms: u64, max_ms: u64, attempt: u32) -> u64 {
    let growth = base_ms.saturating_mul(2_u64.saturating_pow(attempt));
    growth.min(max_ms)
}

pub fn normalize_wallet(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let normalized = if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        trimmed.to_ascii_lowercase()
    } else {
        format!("0x{}", trimmed.to_ascii_lowercase())
    };

    if normalized.len() != 42 {
        return None;
    }

    if normalized
        .chars()
        .skip(2)
        .all(|c| c.is_ascii_hexdigit())
    {
        Some(normalized)
    } else {
        None
    }
}

pub fn build_copy_signal(event: &SourceFillEvent, settings: &RuntimeSettings, fallback_source_equity: f64) -> Option<CopySignal> {
    if event.source_trade_notional_usd <= 0.0 || event.source_price <= 0.0 {
        return None;
    }

    let source_equity = if event.source_equity_usd > 0.0 {
        event.source_equity_usd
    } else {
        fallback_source_equity
    };

    if source_equity <= 0.0 {
        return None;
    }

    let raw_copy_notional = event.source_trade_notional_usd
        * (settings.follower_equity_usd / source_equity)
        * settings.multiplier;

    if raw_copy_notional <= 0.0 {
        return None;
    }

    let copy_notional_usd = round_down(raw_copy_notional, 4);
    let copy_quantity = round_down(copy_notional_usd / event.source_price, 4);

    if copy_quantity <= 0.0 {
        return None;
    }

    Some(CopySignal {
        execution_id: event.execution_id.clone(),
        source_wallet: event.source_wallet.clone(),
        market_slug: event.market_slug.clone(),
        token_id: event.token_id.clone(),
        side: event.side.clone(),
        source_trade_notional_usd: event.source_trade_notional_usd,
        source_price: event.source_price,
        copy_notional_usd,
        copy_quantity,
        expected_edge_bps: event.expected_edge_bps,
        observed_at: event.observed_at,
    })
}

pub fn evaluate_signal(
    signal: &CopySignal,
    settings: &RuntimeSettings,
    current_market_exposure_usd: f64,
    current_daily_notional_usd: f64,
    open_positions_count: u32,
) -> RiskEvaluation {
    if open_positions_count >= settings.caps.max_open_positions {
        return RiskEvaluation {
            decision: RiskDecision::Block {
                reason: "blocked:max_open_positions".to_string(),
            },
            adjusted_signal: None,
            total_cost_buffer_bps: cost_buffer_bps(settings),
        };
    }

    let mut adjusted_notional = signal
        .copy_notional_usd
        .min(settings.caps.max_copy_notional_per_trade);

    let market_remaining = settings.caps.max_market_exposure_usd - current_market_exposure_usd.max(0.0);
    if market_remaining <= 0.0 {
        return RiskEvaluation {
            decision: RiskDecision::Block {
                reason: "blocked:max_market_exposure_usd".to_string(),
            },
            adjusted_signal: None,
            total_cost_buffer_bps: cost_buffer_bps(settings),
        };
    }
    adjusted_notional = adjusted_notional.min(market_remaining);

    let daily_remaining = settings.caps.max_daily_notional_usd - current_daily_notional_usd.max(0.0);
    if daily_remaining <= 0.0 {
        return RiskEvaluation {
            decision: RiskDecision::Block {
                reason: "blocked:max_daily_notional_usd".to_string(),
            },
            adjusted_signal: None,
            total_cost_buffer_bps: cost_buffer_bps(settings),
        };
    }
    adjusted_notional = adjusted_notional.min(daily_remaining);

    if adjusted_notional <= 0.0 {
        return RiskEvaluation {
            decision: RiskDecision::Block {
                reason: "blocked:notional_reduced_to_zero".to_string(),
            },
            adjusted_signal: None,
            total_cost_buffer_bps: cost_buffer_bps(settings),
        };
    }

    let total_cost = cost_buffer_bps(settings);
    if signal.expected_edge_bps <= total_cost {
        return RiskEvaluation {
            decision: RiskDecision::Block {
                reason: format!(
                    "blocked:edge_below_cost edge_bps={:.2} cost_bps={:.2}",
                    signal.expected_edge_bps, total_cost
                ),
            },
            adjusted_signal: None,
            total_cost_buffer_bps: total_cost,
        };
    }

    let mut adjusted_signal = signal.clone();
    adjusted_signal.copy_notional_usd = round_down(adjusted_notional, 4);
    adjusted_signal.copy_quantity = round_down(adjusted_signal.copy_notional_usd / adjusted_signal.source_price, 4);

    if adjusted_signal.copy_quantity <= 0.0 {
        return RiskEvaluation {
            decision: RiskDecision::Block {
                reason: "blocked:quantity_rounded_to_zero".to_string(),
            },
            adjusted_signal: None,
            total_cost_buffer_bps: total_cost,
        };
    }

    RiskEvaluation {
        decision: RiskDecision::Allow {
            reason: "allow:risk_checks_passed".to_string(),
        },
        adjusted_signal: Some(adjusted_signal),
        total_cost_buffer_bps: total_cost,
    }
}

pub fn build_execution_intent(signal: &CopySignal, settings: &RuntimeSettings) -> ExecutionIntent {
    let base_limit_price = match signal.side {
        TradeSide::Buy => (signal.source_price + settings.limit_price_offset).min(0.999),
        TradeSide::Sell => (signal.source_price - settings.limit_price_offset).max(0.001),
    };

    ExecutionIntent {
        execution_id: signal.execution_id.clone(),
        market_slug: signal.market_slug.clone(),
        token_id: signal.token_id.clone(),
        side: signal.side.clone(),
        order_style: OrderStyle::LimitIoc,
        quantity: signal.copy_quantity,
        limit_price: round_down(base_limit_price, 4),
        max_attempts: settings.risk_profile.max_retry_attempts(),
        max_price_deviation_bps: settings.risk_profile.max_price_deviation_bps(),
    }
}

pub async fn execute_intent(
    client: &Client,
    mode: EngineMode,
    execution_api_base: Option<&str>,
    settings: &RuntimeSettings,
    intent: &ExecutionIntent,
) -> ExecutionResult {
    let start = std::time::Instant::now();

    if matches!(mode, EngineMode::DryRun) {
        return ExecutionResult {
            execution_id: intent.execution_id.clone(),
            success: true,
            attempts: 1,
            filled_quantity: intent.quantity,
            average_fill_price: Some(intent.limit_price),
            latency_ms: start.elapsed().as_micros() as f64 / 1_000.0,
            message: "dry_run_simulated_fill".to_string(),
        };
    }

    let Some(base_url) = execution_api_base.filter(|v| !v.is_empty()) else {
        return ExecutionResult {
            execution_id: intent.execution_id.clone(),
            success: true,
            attempts: 1,
            filled_quantity: intent.quantity,
            average_fill_price: Some(intent.limit_price),
            latency_ms: start.elapsed().as_micros() as f64 / 1_000.0,
            message: "live_mode_no_execution_api_simulated_fill".to_string(),
        };
    };

    let mut attempt_price = intent.limit_price;
    let step = (settings.limit_price_offset / 2.0).max(0.001);

    for attempt in 1..=intent.max_attempts {
        if exceeds_price_deviation(intent.limit_price, attempt_price, intent.max_price_deviation_bps) {
            return ExecutionResult {
                execution_id: intent.execution_id.clone(),
                success: false,
                attempts: attempt,
                filled_quantity: 0.0,
                average_fill_price: None,
                latency_ms: start.elapsed().as_micros() as f64 / 1_000.0,
                message: "max_price_deviation_exceeded".to_string(),
            };
        }

        let payload = json!({
            "execution_id": intent.execution_id,
            "market_slug": intent.market_slug,
            "token_id": intent.token_id,
            "side": intent.side.as_str(),
            "style": "LIMIT_IOC",
            "quantity": intent.quantity,
            "limit_price": attempt_price,
            "attempt": attempt,
        });

        let response = client
            .post(format!("{}/orders", base_url.trim_end_matches('/')))
            .json(&payload)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.json::<Value>().await.unwrap_or_else(|_| json!({}));
                let filled = body
                    .get("filled_quantity")
                    .and_then(Value::as_f64)
                    .unwrap_or(intent.quantity);
                let avg_price = body
                    .get("average_fill_price")
                    .and_then(Value::as_f64)
                    .or(Some(attempt_price));

                return ExecutionResult {
                    execution_id: intent.execution_id.clone(),
                    success: true,
                    attempts: attempt,
                    filled_quantity: round_down(filled, 4),
                    average_fill_price: avg_price,
                    latency_ms: start.elapsed().as_micros() as f64 / 1_000.0,
                    message: "execution_api_fill".to_string(),
                };
            }
            Ok(resp) => {
                let status = resp.status();
                let msg = resp.text().await.unwrap_or_else(|_| "<empty>".to_string());
                tracing::warn!(
                    attempt,
                    status = %status,
                    body = %msg,
                    execution_id = %intent.execution_id,
                    "execution attempt rejected"
                );
            }
            Err(err) => {
                tracing::warn!(
                    attempt,
                    error = %err,
                    execution_id = %intent.execution_id,
                    "execution attempt request failed"
                );
            }
        }

        attempt_price = match intent.side {
            TradeSide::Buy => (attempt_price + step).min(0.999),
            TradeSide::Sell => (attempt_price - step).max(0.001),
        };
    }

    ExecutionResult {
        execution_id: intent.execution_id.clone(),
        success: false,
        attempts: intent.max_attempts,
        filled_quantity: 0.0,
        average_fill_price: None,
        latency_ms: start.elapsed().as_micros() as f64 / 1_000.0,
        message: "all_attempts_failed".to_string(),
    }
}

pub async fn fetch_wallet_activity(
    client: &Client,
    data_api_base: &str,
    wallet: &str,
    fallback_source_equity_usd: f64,
) -> Result<Vec<SourceFillEvent>> {
    let endpoints = [
        format!(
            "{}/activity?wallet={}&limit={}",
            data_api_base.trim_end_matches('/'),
            wallet,
            DEFAULT_EVENT_LOOKBACK_LIMIT
        ),
        format!(
            "{}/trades?wallet={}&limit={}",
            data_api_base.trim_end_matches('/'),
            wallet,
            DEFAULT_EVENT_LOOKBACK_LIMIT
        ),
    ];

    let mut last_error: Option<anyhow::Error> = None;

    for endpoint in endpoints {
        match client.get(&endpoint).send().await {
            Ok(response) if response.status().is_success() => {
                let body: Value = response
                    .json()
                    .await
                    .with_context(|| format!("parsing wallet activity response from {}", endpoint))?;

                let events = parse_source_fill_events(wallet, &body, fallback_source_equity_usd);
                if !events.is_empty() {
                    return Ok(events);
                }
            }
            Ok(response) => {
                last_error = Some(anyhow!("{} returned status {}", endpoint, response.status()));
            }
            Err(err) => {
                last_error = Some(anyhow!("{} request failed: {}", endpoint, err));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no wallet activity endpoint returned usable events")))
}

pub async fn fetch_wallet_candidates(client: &Client, data_api_base: &str) -> Result<Vec<CandidateWalletMetrics>> {
    let endpoints = [
        format!("{}/leaderboard?limit=100", data_api_base.trim_end_matches('/')),
        format!("{}/users/leaderboard?limit=100", data_api_base.trim_end_matches('/')),
    ];

    let mut last_error: Option<anyhow::Error> = None;

    for endpoint in endpoints {
        match client.get(&endpoint).send().await {
            Ok(response) if response.status().is_success() => {
                let payload: Value = response
                    .json()
                    .await
                    .with_context(|| format!("parsing wallet candidate response from {}", endpoint))?;
                let candidates = parse_wallet_candidates(&payload);
                if !candidates.is_empty() {
                    return Ok(candidates);
                }
            }
            Ok(response) => {
                last_error = Some(anyhow!("{} returned status {}", endpoint, response.status()));
            }
            Err(err) => {
                last_error = Some(anyhow!("{} request failed: {}", endpoint, err));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no leaderboard endpoint returned candidates")))
}

pub fn parse_source_fill_events(
    source_wallet: &str,
    payload: &Value,
    fallback_source_equity_usd: f64,
) -> Vec<SourceFillEvent> {
    let wallet = normalize_wallet(source_wallet).unwrap_or_else(|| source_wallet.to_ascii_lowercase());
    let mut out = Vec::new();

    for item in collect_array_items(payload) {
        let execution_id = field_str(item, &["execution_id", "id", "event_id", "order_id"])
            .or_else(|| field_str(item, &["transaction_hash", "tx_hash"]))
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let market_slug = field_str(item, &["market_slug", "market", "slug"])
            .unwrap_or_else(|| "unknown-market".to_string());
        let token_id = field_str(item, &["token_id", "asset_id", "outcome_id"]) 
            .unwrap_or_else(|| market_slug.clone());

        let source_price = field_f64(item, &["source_price", "price", "avg_price", "last_price"])
            .unwrap_or(0.0);
        let source_quantity = field_f64(item, &["source_quantity", "quantity", "shares", "size"])
            .unwrap_or(0.0);

        let source_trade_notional_usd = field_f64(
            item,
            &[
                "source_trade_notional_usd",
                "trade_notional_usd",
                "notional_usd",
                "size_usd",
                "usd_value",
                "amount_usd",
            ],
        )
        .unwrap_or_else(|| source_price * source_quantity);

        if source_trade_notional_usd <= 0.0 || source_price <= 0.0 || source_quantity <= 0.0 {
            continue;
        }

        let source_equity_usd = field_f64(item, &["source_equity_usd", "wallet_equity_usd", "equity_usd"])
            .unwrap_or(fallback_source_equity_usd)
            .max(1.0);

        let expected_edge_bps = field_f64(item, &["expected_edge_bps", "edge_bps", "alpha_bps"]) 
            .unwrap_or_else(|| {
                let moneyness = (0.5 - source_price).abs() * 100.0;
                (moneyness * 1.5).max(12.0)
            });

        let side = match field_str(item, &["side", "intent", "direction"])
            .unwrap_or_else(|| "buy".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            value if value.contains("sell") || value.contains("short") => TradeSide::Sell,
            _ => TradeSide::Buy,
        };

        let observed_at = field_str(item, &["observed_at", "timestamp", "created_at", "transact_time"])
            .and_then(|raw| parse_time(&raw))
            .unwrap_or_else(Utc::now);

        out.push(SourceFillEvent {
            execution_id,
            source_wallet: wallet.clone(),
            market_slug: market_slug.to_ascii_lowercase(),
            token_id,
            side,
            source_trade_notional_usd,
            source_price,
            source_quantity,
            source_equity_usd,
            expected_edge_bps,
            observed_at,
            transaction_hash: field_str(item, &["transaction_hash", "tx_hash"]),
        });
    }

    out
}

pub fn parse_wallet_candidates(payload: &Value) -> Vec<CandidateWalletMetrics> {
    let mut out = Vec::new();

    for item in collect_array_items(payload) {
        let Some(wallet_raw) = field_str(item, &["wallet", "address", "user", "trader"]) else {
            continue;
        };
        let Some(wallet) = normalize_wallet(&wallet_raw) else {
            continue;
        };

        let pnl_consistency = normalize_metric(field_f64(item, &["pnl_consistency", "consistency", "pnl_score"]).unwrap_or(0.5));
        let win_rate = normalize_metric(field_f64(item, &["win_rate", "winrate", "accuracy"]).unwrap_or(0.5));
        let drawdown_penalty = normalize_metric(field_f64(item, &["drawdown_penalty", "drawdown", "max_drawdown"]).unwrap_or(0.2));
        let recent_activity = normalize_metric(field_f64(item, &["recent_activity", "activity", "volume_activity"]).unwrap_or(0.5));
        let fill_quality = normalize_metric(field_f64(item, &["fill_quality", "quality", "execution_quality"]).unwrap_or(0.5));

        let confidence = field_f64(item, &["confidence", "confidence_score"])
            .map(normalize_metric)
            .unwrap_or_else(|| ((recent_activity + fill_quality + (1.0 - drawdown_penalty)) / 3.0).clamp(0.0, 1.0));

        out.push(CandidateWalletMetrics {
            wallet,
            pnl_consistency,
            win_rate,
            drawdown_penalty,
            recent_activity,
            fill_quality,
            confidence,
        });
    }

    out
}

pub fn score_wallet(metrics: &CandidateWalletMetrics, now: DateTime<Utc>) -> WalletScore {
    let raw_score = (metrics.pnl_consistency * 0.35)
        + (metrics.win_rate * 0.25)
        + (metrics.recent_activity * 0.20)
        + (metrics.fill_quality * 0.20)
        - (metrics.drawdown_penalty * 0.30);
    let confidence_multiplier = 0.5 + (metrics.confidence * 0.5);

    WalletScore {
        wallet: metrics.wallet.clone(),
        score: (raw_score * 100.0 * confidence_multiplier).clamp(0.0, 100.0),
        pnl_consistency: metrics.pnl_consistency,
        win_rate: metrics.win_rate,
        drawdown_penalty: metrics.drawdown_penalty,
        recent_activity: metrics.recent_activity,
        fill_quality: metrics.fill_quality,
        updated_at: now,
    }
}

pub fn maybe_create_rotation_suggestion(
    tracked_wallets: &BTreeSet<String>,
    pending_suggestions: &BTreeMap<String, RotationSuggestion>,
    scores: &HashMap<String, WalletScore>,
    max_active_wallets: usize,
    last_rotation_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    rank_delta_threshold: f64,
    confidence_floor: f64,
    cooldown: Duration,
) -> Option<RotationSuggestion> {
    if !pending_suggestions.is_empty() {
        return None;
    }

    if let Some(last) = last_rotation_at {
        if now.signed_duration_since(last).to_std().unwrap_or_default() < cooldown {
            return None;
        }
    }

    let mut ranked: Vec<&WalletScore> = scores.values().collect();
    ranked.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    let candidate = ranked
        .iter()
        .copied()
        .find(|score| !tracked_wallets.contains(&score.wallet) && score.score >= confidence_floor)?;

    if tracked_wallets.len() < max_active_wallets {
        return Some(RotationSuggestion {
            id: Uuid::new_v4().to_string(),
            wallet: candidate.wallet.clone(),
            score: candidate.score,
            reason: "capacity_available_for_new_wallet".to_string(),
            expected_impact: format!("Add high-ranked wallet score={:.2}", candidate.score),
            status: SuggestionStatus::Pending,
            created_at: now,
            decided_at: None,
        });
    }

    let worst_tracked = tracked_wallets
        .iter()
        .map(|wallet| scores.get(wallet).map(|s| s.score).unwrap_or(0.0))
        .fold(f64::INFINITY, f64::min);

    let rank_delta = candidate.score - worst_tracked;
    if rank_delta < rank_delta_threshold {
        return None;
    }

    Some(RotationSuggestion {
        id: Uuid::new_v4().to_string(),
        wallet: candidate.wallet.clone(),
        score: candidate.score,
        reason: format!("rank_delta_triggered delta={:.2} threshold={:.2}", rank_delta, rank_delta_threshold),
        expected_impact: format!("Replace weakest tracked wallet by +{:.2} score", rank_delta),
        status: SuggestionStatus::Pending,
        created_at: now,
        decided_at: None,
    })
}

fn cost_buffer_bps(settings: &RuntimeSettings) -> f64 {
    settings.fee_bps + settings.slippage_bps + settings.risk_profile.execution_premium_bps()
}

fn exceeds_price_deviation(base_price: f64, current_price: f64, max_deviation_bps: f64) -> bool {
    if base_price <= 0.0 {
        return false;
    }
    let deviation_bps = ((current_price - base_price).abs() / base_price) * 10_000.0;
    deviation_bps > max_deviation_bps
}

fn normalize_metric(value: f64) -> f64 {
    if value > 1.0 {
        (value / 100.0).clamp(0.0, 1.0)
    } else {
        value.clamp(0.0, 1.0)
    }
}

fn round_down(value: f64, decimals: i32) -> f64 {
    let factor = 10_f64.powi(decimals);
    (value * factor).floor() / factor
}

fn collect_array_items(payload: &Value) -> Vec<&Value> {
    if let Some(array) = payload.as_array() {
        return array.iter().collect();
    }

    if let Some(obj) = payload.as_object() {
        for key in ["data", "results", "events", "trades", "wallets", "items"] {
            if let Some(array) = obj.get(key).and_then(Value::as_array) {
                return array.iter().collect();
            }
        }
    }

    Vec::new()
}

fn field_str(item: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        let Some(value) = item.get(key) else {
            continue;
        };
        if let Some(s) = value.as_str() {
            if !s.trim().is_empty() {
                return Some(s.trim().to_string());
            }
        }

        if value.is_number() {
            return Some(value.to_string());
        }
    }
    None
}

fn field_f64(item: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let Some(value) = item.get(key) else {
            continue;
        };
        if let Some(n) = value.as_f64() {
            return Some(n);
        }
        if let Some(s) = value.as_str() {
            if let Ok(n) = s.parse::<f64>() {
                return Some(n);
            }
        }
    }
    None
}

fn parse_time(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|v| v.with_timezone(&Utc))
        .ok()
}

#[cfg(test)]
mod tests {
    use chrono::Duration as ChronoDuration;

    use super::*;

    #[test]
    fn computes_proportional_signal_with_multiplier() {
        let event = SourceFillEvent {
            execution_id: "e1".to_string(),
            source_wallet: "0xabc0000000000000000000000000000000000000".to_string(),
            market_slug: "market-a".to_string(),
            token_id: "token-a".to_string(),
            side: TradeSide::Buy,
            source_trade_notional_usd: 100_000.0,
            source_price: 0.5,
            source_quantity: 200_000.0,
            source_equity_usd: 500_000.0,
            expected_edge_bps: 80.0,
            observed_at: Utc::now(),
            transaction_hash: None,
        };

        let mut settings = RuntimeSettings::default();
        settings.follower_equity_usd = 10_000.0;
        settings.multiplier = 1.25;

        let signal = build_copy_signal(&event, &settings, 100_000.0).expect("signal should be created");
        assert!((signal.copy_notional_usd - 2_500.0).abs() < 0.0001);
        assert!((signal.copy_quantity - 5_000.0).abs() < 0.0001);
    }

    #[test]
    fn applies_caps_in_expected_order() {
        let signal = CopySignal {
            execution_id: "e2".to_string(),
            source_wallet: "0xabc0000000000000000000000000000000000000".to_string(),
            market_slug: "market-a".to_string(),
            token_id: "token-a".to_string(),
            side: TradeSide::Buy,
            source_trade_notional_usd: 100_000.0,
            source_price: 0.4,
            copy_notional_usd: 5_000.0,
            copy_quantity: 12_500.0,
            expected_edge_bps: 120.0,
            observed_at: Utc::now(),
        };

        let mut settings = RuntimeSettings::default();
        settings.caps.max_copy_notional_per_trade = 4_000.0;
        settings.caps.max_market_exposure_usd = 3_000.0;
        settings.caps.max_daily_notional_usd = 2_000.0;

        let result = evaluate_signal(&signal, &settings, 1_500.0, 500.0, 1);
        let adjusted = result.adjusted_signal.expect("signal should be allowed");

        assert!((adjusted.copy_notional_usd - 1_500.0).abs() < 0.0001);
    }

    #[test]
    fn blocks_when_edge_is_below_cost_buffer() {
        let signal = CopySignal {
            execution_id: "e3".to_string(),
            source_wallet: "0xabc0000000000000000000000000000000000000".to_string(),
            market_slug: "market-b".to_string(),
            token_id: "token-b".to_string(),
            side: TradeSide::Buy,
            source_trade_notional_usd: 1_000.0,
            source_price: 0.5,
            copy_notional_usd: 100.0,
            copy_quantity: 200.0,
            expected_edge_bps: 10.0,
            observed_at: Utc::now(),
        };

        let mut settings = RuntimeSettings::default();
        settings.fee_bps = 8.0;
        settings.slippage_bps = 12.0;

        let result = evaluate_signal(&signal, &settings, 0.0, 0.0, 0);
        assert!(matches!(result.decision, RiskDecision::Block { .. }));
    }

    #[test]
    fn parses_wallet_fill_events() {
        let payload = json!({
            "data": [
                {
                    "execution_id": "exec-1",
                    "market_slug": "btc-up",
                    "token_id": "token-1",
                    "side": "buy",
                    "price": 0.42,
                    "shares": 100.0,
                    "notional_usd": 42.0,
                    "timestamp": "2026-02-20T12:00:00Z"
                }
            ]
        });

        let events = parse_source_fill_events(
            "0x1234567890123456789012345678901234567890",
            &payload,
            100_000.0,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].execution_id, "exec-1");
        assert_eq!(events[0].market_slug, "btc-up");
    }

    #[test]
    fn creates_rotation_suggestion_only_for_meaningful_delta() {
        let tracked_wallets: BTreeSet<String> = [
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        ]
        .into_iter()
        .collect();

        let mut scores = HashMap::new();
        scores.insert(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            WalletScore {
                wallet: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                score: 55.0,
                pnl_consistency: 0.5,
                win_rate: 0.5,
                drawdown_penalty: 0.2,
                recent_activity: 0.5,
                fill_quality: 0.5,
                updated_at: Utc::now(),
            },
        );
        scores.insert(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            WalletScore {
                wallet: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                score: 50.0,
                pnl_consistency: 0.5,
                win_rate: 0.5,
                drawdown_penalty: 0.2,
                recent_activity: 0.5,
                fill_quality: 0.5,
                updated_at: Utc::now(),
            },
        );
        scores.insert(
            "0xcccccccccccccccccccccccccccccccccccccccc".to_string(),
            WalletScore {
                wallet: "0xcccccccccccccccccccccccccccccccccccccccc".to_string(),
                score: 65.0,
                pnl_consistency: 0.7,
                win_rate: 0.6,
                drawdown_penalty: 0.1,
                recent_activity: 0.8,
                fill_quality: 0.7,
                updated_at: Utc::now(),
            },
        );

        let suggestion = maybe_create_rotation_suggestion(
            &tracked_wallets,
            &BTreeMap::new(),
            &scores,
            2,
            Some(Utc::now() - ChronoDuration::hours(2)),
            Utc::now(),
            8.0,
            52.0,
            Duration::from_secs(600),
        );

        assert!(suggestion.is_some());
        assert_eq!(suggestion.expect("suggestion").wallet, "0xcccccccccccccccccccccccccccccccccccccccc");
    }
}
