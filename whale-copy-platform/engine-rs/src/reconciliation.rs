use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tracing;

use crate::state::EngineState;
use crate::types::{EventClass, EventEnvelope};

/// Represents a position as reported by the exchange/Polymarket API
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangePosition {
    pub market_slug: String,
    pub token_id: String,
    pub side: PositionSide,
    pub size: f64,
    pub avg_entry_price: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Yes,
    No,
}

impl PositionSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Yes => "yes",
            Self::No => "no",
        }
    }
}

/// Internal position tracking (derived from our executed orders)
#[derive(Debug, Clone)]
pub struct InternalPosition {
    pub market_slug: String,
    pub token_id: String,
    pub side: PositionSide,
    pub size: f64,
    pub avg_entry_price: f64,
    pub last_updated: DateTime<Utc>,
}

/// Result of a reconciliation check
#[derive(Debug, Clone, Serialize)]
pub struct ReconciliationReport {
    pub checked_at: DateTime<Utc>,
    pub exchange_positions_count: usize,
    pub internal_positions_count: usize,
    pub matched: Vec<PositionMatch>,
    pub discrepancies: Vec<PositionDiscrepancy>,
    pub exchange_only: Vec<ExchangePosition>,
    pub internal_only: Vec<String>, // market_slugs we think we have but exchange doesn't show
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionMatch {
    pub market_slug: String,
    pub exchange_size: f64,
    pub internal_size: f64,
    pub size_diff: f64,
    pub size_diff_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionDiscrepancy {
    pub market_slug: String,
    pub discrepancy_type: DiscrepancyType,
    pub exchange_value: f64,
    pub internal_value: f64,
    pub severity: DiscrepancySeverity,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscrepancyType {
    SizeMismatch,
    SideMismatch,
    MissingOnExchange,
    MissingInternally,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscrepancySeverity {
    Critical, // >10% size diff or side mismatch
    Warning,  // 5-10% size diff
    Minor,    // <5% size diff
}

/// Configuration for reconciliation behavior
#[derive(Debug, Clone)]
pub struct ReconciliationConfig {
    /// How often to run reconciliation (seconds)
    pub interval_secs: u64,
    /// Threshold for warning discrepancy (percentage)
    pub warning_threshold_pct: f64,
    /// Threshold for critical discrepancy (percentage)
    pub critical_threshold_pct: f64,
    /// Auto-correct internal state when discrepancy found
    pub auto_correct: bool,
    /// Pause trading if critical discrepancy detected
    pub pause_on_critical: bool,
    /// API endpoint for fetching positions
    pub positions_api_url: String,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            interval_secs: 300, // 5 minutes
            warning_threshold_pct: 5.0,
            critical_threshold_pct: 10.0,
            auto_correct: false,
            pause_on_critical: true,
            positions_api_url: "https://api.polymarket.com/positions".to_string(),
        }
    }
}

/// Fetch current positions from exchange API
pub async fn fetch_exchange_positions(
    client: &Client,
    api_url: &str,
    api_key: Option<&str>,
) -> Result<Vec<ExchangePosition>> {
    let mut request = client.get(api_url);
    
    if let Some(key) = api_key {
        request = request.header("Authorization", format!("Bearer {}", key));
    }
    
    let response = request
        .send()
        .await
        .context("fetching positions from exchange")?;
    
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("positions API error: {} - {}", status, body);
    }
    
    let positions: Vec<ExchangePosition> = response
        .json()
        .await
        .context("parsing positions response")?;
    
    Ok(positions)
}

/// Build internal position map from engine state
pub fn build_internal_positions(state: &EngineState) -> HashMap<String, InternalPosition> {
    let mut positions = HashMap::new();
    
    // Convert market_exposure_usd to position estimates
    // In a full implementation, we'd track actual positions from executed orders
    for (market_slug, exposure) in &state.market_exposure_usd {
        // This is simplified - real implementation would track actual token IDs and sizes
        let position = InternalPosition {
            market_slug: market_slug.clone(),
            token_id: format!("{}", market_slug), // Placeholder
            side: PositionSide::Yes, // Would be determined from order history
            size: *exposure,
            avg_entry_price: 0.5, // Would be tracked from fills
            last_updated: Utc::now(),
        };
        positions.insert(market_slug.clone(), position);
    }
    
    positions
}

/// Reconcile exchange positions against internal state
pub fn reconcile_positions(
    exchange_positions: &[ExchangePosition],
    internal_positions: &HashMap<String, InternalPosition>,
    config: &ReconciliationConfig,
) -> ReconciliationReport {
    let checked_at = Utc::now();
    let mut matched = Vec::new();
    let mut discrepancies = Vec::new();
    let mut exchange_only = Vec::new();
    let mut internal_only = Vec::new();
    
    let mut matched_markets: std::collections::HashSet<String> = std::collections::HashSet::new();
    
    // Check exchange positions against internal
    for exch_pos in exchange_positions {
        matched_markets.insert(exch_pos.market_slug.clone());
        
        match internal_positions.get(&exch_pos.market_slug) {
            Some(int_pos) => {
                // Check side mismatch
                if exch_pos.side != int_pos.side {
                    discrepancies.push(PositionDiscrepancy {
                        market_slug: exch_pos.market_slug.clone(),
                        discrepancy_type: DiscrepancyType::SideMismatch,
                        exchange_value: if exch_pos.side == PositionSide::Yes { 1.0 } else { 0.0 },
                        internal_value: if int_pos.side == PositionSide::Yes { 1.0 } else { 0.0 },
                        severity: DiscrepancySeverity::Critical,
                    });
                    continue;
                }
                
                // Check size mismatch
                let size_diff = (exch_pos.size - int_pos.size).abs();
                let size_diff_pct = if int_pos.size > 0.0 {
                    (size_diff / int_pos.size) * 100.0
                } else if exch_pos.size > 0.0 {
                    100.0
                } else {
                    0.0
                };
                
                let severity = if size_diff_pct >= config.critical_threshold_pct {
                    DiscrepancySeverity::Critical
                } else if size_diff_pct >= config.warning_threshold_pct {
                    DiscrepancySeverity::Warning
                } else {
                    DiscrepancySeverity::Minor
                };
                
                if severity != DiscrepancySeverity::Minor {
                    discrepancies.push(PositionDiscrepancy {
                        market_slug: exch_pos.market_slug.clone(),
                        discrepancy_type: DiscrepancyType::SizeMismatch,
                        exchange_value: exch_pos.size,
                        internal_value: int_pos.size,
                        severity,
                    });
                }
                
                matched.push(PositionMatch {
                    market_slug: exch_pos.market_slug.clone(),
                    exchange_size: exch_pos.size,
                    internal_size: int_pos.size,
                    size_diff,
                    size_diff_pct,
                });
            }
            None => {
                // Position exists on exchange but not in our tracking
                exchange_only.push(exch_pos.clone());
                discrepancies.push(PositionDiscrepancy {
                    market_slug: exch_pos.market_slug.clone(),
                    discrepancy_type: DiscrepancyType::MissingInternally,
                    exchange_value: exch_pos.size,
                    internal_value: 0.0,
                    severity: DiscrepancySeverity::Critical,
                });
            }
        }
    }
    
    // Find positions we track but exchange doesn't show
    for (market_slug, _) in internal_positions {
        if !matched_markets.contains(market_slug) {
            internal_only.push(market_slug.clone());
            discrepancies.push(PositionDiscrepancy {
                market_slug: market_slug.clone(),
                discrepancy_type: DiscrepancyType::MissingOnExchange,
                exchange_value: 0.0,
                internal_value: 1.0,
                severity: DiscrepancySeverity::Warning,
            });
        }
    }
    
    ReconciliationReport {
        checked_at,
        exchange_positions_count: exchange_positions.len(),
        internal_positions_count: internal_positions.len(),
        matched,
        discrepancies,
        exchange_only,
        internal_only,
    }
}

/// Main reconciliation loop task
pub async fn reconciliation_loop(
    state: std::sync::Arc<tokio::sync::RwLock<EngineState>>,
    client: Client,
    config: ReconciliationConfig,
    event_buffer: std::sync::Arc<tokio::sync::Mutex<std::collections::VecDeque<EventEnvelope>>>,
    api_key: Option<String>,
) {
    let mut ticker = interval(Duration::from_secs(config.interval_secs));
    
    tracing::info!(
        interval_secs = config.interval_secs,
        auto_correct = config.auto_correct,
        "Starting position reconciliation loop"
    );
    
    loop {
        ticker.tick().await;
        
        match run_reconciliation(&state, &client, &config, &event_buffer, api_key.as_deref()).await {
            Ok(report) => {
                let critical_count = report.discrepancies.iter()
                    .filter(|d| d.severity == DiscrepancySeverity::Critical)
                    .count();
                
                if critical_count > 0 {
                    tracing::error!(
                        critical_discrepancies = critical_count,
                        total_discrepancies = report.discrepancies.len(),
                        "CRITICAL: Position reconciliation found significant discrepancies"
                    );
                    
                    if config.pause_on_critical {
                        tracing::warn!("Auto-pausing trading due to critical position discrepancy");
                        let mut state_guard = state.write().await;
                        state_guard.settings.paused = true;
                    }
                } else if !report.discrepancies.is_empty() {
                    tracing::warn!(
                        discrepancies = report.discrepancies.len(),
                        "Position reconciliation found minor discrepancies"
                    );
                } else {
                    tracing::debug!("Position reconciliation: all positions match");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "Position reconciliation failed");
            }
        }
    }
}

async fn run_reconciliation(
    state: &std::sync::Arc<tokio::sync::RwLock<EngineState>>,
    client: &Client,
    config: &ReconciliationConfig,
    event_buffer: &std::sync::Arc<tokio::sync::Mutex<std::collections::VecDeque<EventEnvelope>>>,
    api_key: Option<&str>,
) -> Result<ReconciliationReport> {
    // Fetch exchange positions
    let exchange_positions = fetch_exchange_positions(client, &config.positions_api_url, api_key)
        .await?;
    
    // Build internal position map
    let internal_positions = {
        let state_guard = state.read().await;
        build_internal_positions(&state_guard)
    };
    
    // Run reconciliation
    let report = reconcile_positions(&exchange_positions, &internal_positions, config);
    
    // Emit event for significant discrepancies
    if !report.discrepancies.is_empty() {
        let event = EventEnvelope {
            id: uuid::Uuid::new_v4().to_string(),
            class: EventClass::Risk,
            event_type: "position_reconciliation".to_string(),
            payload: serde_json::to_value(&report)?,
            ts: Utc::now(),
        };
        
        let mut buffer = event_buffer.lock().await;
        if buffer.len() >= 5000 {
            buffer.pop_front();
        }
        buffer.push_back(event);
    }
    
    // Auto-correct if enabled and discrepancies found
    if config.auto_correct && !report.discrepancies.is_empty() {
        tracing::info!("Auto-correcting internal position state");
        let mut state_guard = state.write().await;
        
        // Update market_exposure_usd to match exchange
        state_guard.market_exposure_usd.clear();
        for pos in &exchange_positions {
            state_guard.market_exposure_usd.insert(
                pos.market_slug.clone(),
                pos.size * pos.current_price,
            );
        }
        
        // Update open positions count
        state_guard.open_positions_count = exchange_positions.len() as u32;
    }
    
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ReconciliationConfig {
        ReconciliationConfig {
            interval_secs: 60,
            warning_threshold_pct: 5.0,
            critical_threshold_pct: 10.0,
            auto_correct: false,
            pause_on_critical: true,
            positions_api_url: "https://test.api/positions".to_string(),
        }
    }

    #[test]
    fn test_reconcile_perfect_match() {
        let config = create_test_config();
        
        let exchange_positions = vec![ExchangePosition {
            market_slug: "market-1".to_string(),
            token_id: "token-1".to_string(),
            side: PositionSide::Yes,
            size: 100.0,
            avg_entry_price: 0.5,
            current_price: 0.6,
            unrealized_pnl: 10.0,
            updated_at: Utc::now(),
        }];
        
        let mut internal_positions = HashMap::new();
        internal_positions.insert(
            "market-1".to_string(),
            InternalPosition {
                market_slug: "market-1".to_string(),
                token_id: "token-1".to_string(),
                side: PositionSide::Yes,
                size: 100.0,
                avg_entry_price: 0.5,
                last_updated: Utc::now(),
            },
        );
        
        let report = reconcile_positions(&exchange_positions, &internal_positions, &config);
        
        assert_eq!(report.matched.len(), 1);
        assert!(report.discrepancies.is_empty());
        assert!(report.exchange_only.is_empty());
        assert!(report.internal_only.is_empty());
    }

    #[test]
    fn test_reconcile_size_mismatch_warning() {
        let config = create_test_config();
        
        let exchange_positions = vec![ExchangePosition {
            market_slug: "market-1".to_string(),
            token_id: "token-1".to_string(),
            side: PositionSide::Yes,
            size: 106.0, // 6% diff
            avg_entry_price: 0.5,
            current_price: 0.6,
            unrealized_pnl: 10.0,
            updated_at: Utc::now(),
        }];
        
        let mut internal_positions = HashMap::new();
        internal_positions.insert(
            "market-1".to_string(),
            InternalPosition {
                market_slug: "market-1".to_string(),
                token_id: "token-1".to_string(),
                side: PositionSide::Yes,
                size: 100.0,
                avg_entry_price: 0.5,
                last_updated: Utc::now(),
            },
        );
        
        let report = reconcile_positions(&exchange_positions, &internal_positions, &config);
        
        assert_eq!(report.discrepancies.len(), 1);
        assert_eq!(report.discrepancies[0].severity, DiscrepancySeverity::Warning);
        assert_eq!(report.discrepancies[0].discrepancy_type, DiscrepancyType::SizeMismatch);
    }

    #[test]
    fn test_reconcile_size_mismatch_critical() {
        let config = create_test_config();
        
        let exchange_positions = vec![ExchangePosition {
            market_slug: "market-1".to_string(),
            token_id: "token-1".to_string(),
            side: PositionSide::Yes,
            size: 115.0, // 15% diff
            avg_entry_price: 0.5,
            current_price: 0.6,
            unrealized_pnl: 10.0,
            updated_at: Utc::now(),
        }];
        
        let mut internal_positions = HashMap::new();
        internal_positions.insert(
            "market-1".to_string(),
            InternalPosition {
                market_slug: "market-1".to_string(),
                token_id: "token-1".to_string(),
                side: PositionSide::Yes,
                size: 100.0,
                avg_entry_price: 0.5,
                last_updated: Utc::now(),
            },
        );
        
        let report = reconcile_positions(&exchange_positions, &internal_positions, &config);
        
        assert_eq!(report.discrepancies.len(), 1);
        assert_eq!(report.discrepancies[0].severity, DiscrepancySeverity::Critical);
    }

    #[test]
    fn test_reconcile_side_mismatch() {
        let config = create_test_config();
        
        let exchange_positions = vec![ExchangePosition {
            market_slug: "market-1".to_string(),
            token_id: "token-1".to_string(),
            side: PositionSide::No,
            size: 100.0,
            avg_entry_price: 0.5,
            current_price: 0.6,
            unrealized_pnl: 10.0,
            updated_at: Utc::now(),
        }];
        
        let mut internal_positions = HashMap::new();
        internal_positions.insert(
            "market-1".to_string(),
            InternalPosition {
                market_slug: "market-1".to_string(),
                token_id: "token-1".to_string(),
                side: PositionSide::Yes,
                size: 100.0,
                avg_entry_price: 0.5,
                last_updated: Utc::now(),
            },
        );
        
        let report = reconcile_positions(&exchange_positions, &internal_positions, &config);
        
        assert_eq!(report.discrepancies.len(), 1);
        assert_eq!(report.discrepancies[0].discrepancy_type, DiscrepancyType::SideMismatch);
        assert_eq!(report.discrepancies[0].severity, DiscrepancySeverity::Critical);
    }

    #[test]
    fn test_reconcile_missing_internally() {
        let config = create_test_config();
        
        let exchange_positions = vec![ExchangePosition {
            market_slug: "market-1".to_string(),
            token_id: "token-1".to_string(),
            side: PositionSide::Yes,
            size: 100.0,
            avg_entry_price: 0.5,
            current_price: 0.6,
            unrealized_pnl: 10.0,
            updated_at: Utc::now(),
        }];
        
        let internal_positions = HashMap::new(); // Empty
        
        let report = reconcile_positions(&exchange_positions, &internal_positions, &config);
        
        assert_eq!(report.discrepancies.len(), 1);
        assert_eq!(report.discrepancies[0].discrepancy_type, DiscrepancyType::MissingInternally);
        assert_eq!(report.exchange_only.len(), 1);
    }

    #[test]
    fn test_reconcile_missing_on_exchange() {
        let config = create_test_config();
        
        let exchange_positions = vec![]; // Empty
        
        let mut internal_positions = HashMap::new();
        internal_positions.insert(
            "market-1".to_string(),
            InternalPosition {
                market_slug: "market-1".to_string(),
                token_id: "token-1".to_string(),
                side: PositionSide::Yes,
                size: 100.0,
                avg_entry_price: 0.5,
                last_updated: Utc::now(),
            },
        );
        
        let report = reconcile_positions(&exchange_positions, &internal_positions, &config);
        
        assert_eq!(report.discrepancies.len(), 1);
        assert_eq!(report.discrepancies[0].discrepancy_type, DiscrepancyType::MissingOnExchange);
        assert_eq!(report.internal_only.len(), 1);
    }
}
