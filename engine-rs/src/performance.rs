use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tracks performance metrics for a copied wallet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPerformance {
    pub wallet: String,
    pub total_trades: u32,
    pub winning_trades: u32,
    pub losing_trades: u32,
    pub total_pnl_usd: f64,
    pub win_rate: f64,
    pub avg_trade_pnl_usd: f64,
    pub max_drawdown_usd: f64,
    pub current_drawdown_usd: f64,
    pub peak_equity_usd: f64,
    pub last_trade_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WalletPerformance {
    pub fn new(wallet: String) -> Self {
        let now = Utc::now();
        Self {
            wallet,
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            total_pnl_usd: 0.0,
            win_rate: 0.0,
            avg_trade_pnl_usd: 0.0,
            max_drawdown_usd: 0.0,
            current_drawdown_usd: 0.0,
            peak_equity_usd: 0.0,
            last_trade_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Record a trade outcome and recalculate metrics
    pub fn record_trade(&mut self, pnl_usd: f64) {
        self.total_trades += 1;
        self.total_pnl_usd += pnl_usd;
        
        if pnl_usd > 0.0 {
            self.winning_trades += 1;
        } else if pnl_usd < 0.0 {
            self.losing_trades += 1;
        }
        
        self.win_rate = if self.total_trades > 0 {
            (self.winning_trades as f64 / self.total_trades as f64) * 100.0
        } else {
            0.0
        };
        
        self.avg_trade_pnl_usd = self.total_pnl_usd / self.total_trades as f64;
        
        // Track drawdown
        if self.total_pnl_usd > self.peak_equity_usd {
            self.peak_equity_usd = self.total_pnl_usd;
            self.current_drawdown_usd = 0.0;
        } else {
            self.current_drawdown_usd = self.peak_equity_usd - self.total_pnl_usd;
            if self.current_drawdown_usd > self.max_drawdown_usd {
                self.max_drawdown_usd = self.current_drawdown_usd;
            }
        }
        
        self.last_trade_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    /// Check if this wallet should be auto-stopped based on performance thresholds
    pub fn should_auto_stop(&self, min_win_rate: f64, max_drawdown_usd: f64, min_trades: u32) -> bool {
        // Need minimum sample size before making decisions
        if self.total_trades < min_trades {
            return false;
        }
        
        // Stop if win rate is below threshold
        if self.win_rate < min_win_rate {
            return true;
        }
        
        // Stop if max drawdown exceeded
        if self.max_drawdown_usd > max_drawdown_usd {
            return true;
        }
        
        false
    }
}

/// Auto-stop configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoStopConfig {
    pub enabled: bool,
    pub min_win_rate_percent: f64,
    pub max_drawdown_usd: f64,
    pub min_trades_before_stop: u32,
    pub lookback_hours: u64,
}

impl Default for AutoStopConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_win_rate_percent: 40.0,  // Stop if win rate drops below 40%
            max_drawdown_usd: 1000.0,     // Stop if drawdown exceeds $1000
            min_trades_before_stop: 5,    // Need at least 5 trades before stopping
            lookback_hours: 24,           // Look at last 24 hours
        }
    }
}

/// Performance tracker for all copied wallets
#[derive(Debug, Clone, Default)]
pub struct PerformanceTracker {
    performances: HashMap<String, WalletPerformance>,
    auto_stop_config: AutoStopConfig,
    stopped_wallets: HashMap<String, DateTime<Utc>>, // wallet -> stopped_at
}

impl PerformanceTracker {
    pub fn new(auto_stop_config: AutoStopConfig) -> Self {
        Self {
            performances: HashMap::new(),
            auto_stop_config,
            stopped_wallets: HashMap::new(),
        }
    }

    pub fn get_or_create(&mut self, wallet: &str) -> &mut WalletPerformance {
        self.performances
            .entry(wallet.to_string())
            .or_insert_with(|| WalletPerformance::new(wallet.to_string()))
    }

    pub fn get(&self, wallet: &str) -> Option<&WalletPerformance> {
        self.performances.get(wallet)
    }

    pub fn record_trade(&mut self, wallet: &str, pnl_usd: f64) {
        let perf = self.get_or_create(wallet);
        perf.record_trade(pnl_usd);
    }

    /// Check if wallet should be auto-stopped
    pub fn check_auto_stop(&self, wallet: &str) -> Option<AutoStopReason> {
        if !self.auto_stop_config.enabled {
            return None;
        }

        let perf = self.performances.get(wallet)?;
        
        if perf.should_auto_stop(
            self.auto_stop_config.min_win_rate_percent,
            self.auto_stop_config.max_drawdown_usd,
            self.auto_stop_config.min_trades_before_stop,
        ) {
            let reason = if perf.win_rate < self.auto_stop_config.min_win_rate_percent {
                AutoStopReason::LowWinRate {
                    current: perf.win_rate,
                    threshold: self.auto_stop_config.min_win_rate_percent,
                }
            } else if perf.max_drawdown_usd > self.auto_stop_config.max_drawdown_usd {
                AutoStopReason::MaxDrawdown {
                    current: perf.max_drawdown_usd,
                    threshold: self.auto_stop_config.max_drawdown_usd,
                }
            } else {
                AutoStopReason::Unknown
            };
            return Some(reason);
        }
        
        None
    }

    /// Mark wallet as stopped
    pub fn mark_stopped(&mut self, wallet: &str) {
        self.stopped_wallets.insert(wallet.to_string(), Utc::now());
    }

    /// Check if wallet is currently stopped
    pub fn is_stopped(&self, wallet: &str) -> bool {
        self.stopped_wallets.contains_key(wallet)
    }

    /// Get all stopped wallets
    pub fn get_stopped_wallets(&self) -> &HashMap<String, DateTime<Utc>> {
        &self.stopped_wallets
    }

    /// Resume a stopped wallet
    pub fn resume_wallet(&mut self, wallet: &str) -> bool {
        self.stopped_wallets.remove(wallet).is_some()
    }

    /// Get performance summary for all wallets
    pub fn get_summary(&self) -> Vec<&WalletPerformance> {
        self.performances.values().collect()
    }

    pub fn set_auto_stop_config(&mut self, config: AutoStopConfig) {
        self.auto_stop_config = config;
    }

    pub fn get_auto_stop_config(&self) -> &AutoStopConfig {
        &self.auto_stop_config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoStopReason {
    LowWinRate { current: f64, threshold: f64 },
    MaxDrawdown { current: f64, threshold: f64 },
    Unknown,
}

impl AutoStopReason {
    pub fn message(&self) -> String {
        match self {
            AutoStopReason::LowWinRate { current, threshold } => {
                format!(
                    "Auto-stop: Win rate {:.1}% below threshold {:.1}%",
                    current, threshold
                )
            }
            AutoStopReason::MaxDrawdown { current, threshold } => {
                format!(
                    "Auto-stop: Drawdown ${:.2} exceeds threshold ${:.2}",
                    current, threshold
                )
            }
            AutoStopReason::Unknown => "Auto-stop: Performance threshold breached".to_string(),
        }
    }
}
