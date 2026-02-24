use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// Represents a completed trade (buy + sell pair)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedTrade {
    pub id: String,
    pub market_slug: String,
    pub token_id: String,
    pub side: TradeSide,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub fees_paid: f64,
    pub entry_time: DateTime<Utc>,
    pub exit_time: DateTime<Utc>,
    pub source_wallet: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TradeSide {
    Long,  // Bought YES / Sold NO
    Short, // Bought NO / Sold YES
}

impl TradeSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }
}

/// Realized PnL for a completed trade
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealizedPnL {
    pub trade_id: String,
    pub market_slug: String,
    pub gross_pnl: f64,
    pub fees: f64,
    pub net_pnl: f64,
    pub return_pct: f64,
    pub holding_period_hours: f64,
    pub closed_at: DateTime<Utc>,
}

/// Unrealized PnL for an open position
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnrealizedPnL {
    pub market_slug: String,
    pub token_id: String,
    pub side: TradeSide,
    pub entry_price: f64,
    pub current_price: f64,
    pub size: f64,
    pub gross_pnl: f64,
    pub return_pct: f64,
    pub fees_estimate: f64, // Estimated fees on exit
    pub net_pnl_estimate: f64,
}

/// Summary of PnL performance
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PnLSummary {
    pub total_realized_pnl: f64,
    pub total_unrealized_pnl: f64,
    pub total_fees_paid: f64,
    pub total_fees_estimated: f64,
    pub net_pnl: f64,
    pub win_count: u32,
    pub loss_count: u32,
    pub win_rate: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub profit_factor: f64,
    pub sharpe_ratio: Option<f64>,
    pub max_drawdown_pct: f64,
    pub trading_days: u32,
}

/// Daily PnL snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyPnL {
    pub date: NaiveDate,
    pub realized_pnl: f64,
    pub fees: f64,
    pub net_pnl: f64,
    pub trade_count: u32,
    pub win_count: u32,
    pub loss_count: u32,
}

/// PnL tracker that maintains running calculations
#[derive(Debug, Clone, Default)]
pub struct PnLTracker {
    /// Completed trades with realized PnL
    pub completed_trades: Vec<CompletedTrade>,
    /// Realized PnL records
    pub realized_pnls: Vec<RealizedPnL>,
    /// Current open positions (market_slug -> position)
    pub open_positions: HashMap<String, OpenPosition>,
    /// Daily PnL history
    pub daily_pnls: HashMap<NaiveDate, DailyPnL>,
    /// Running total of fees paid
    pub total_fees_paid: f64,
    /// High water mark for drawdown calculation
    pub high_water_mark: f64,
    /// Current drawdown from peak
    pub current_drawdown: f64,
    /// Maximum drawdown seen
    pub max_drawdown: f64,
}

/// An open position being tracked
#[derive(Debug, Clone)]
pub struct OpenPosition {
    pub market_slug: String,
    pub token_id: String,
    pub side: TradeSide,
    pub entry_price: f64,
    pub size: f64,
    pub fees_paid_entry: f64,
    pub entry_time: DateTime<Utc>,
}

impl PnLTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a new position entry (buy)
    pub fn record_entry(
        &mut self,
        market_slug: String,
        token_id: String,
        side: TradeSide,
        price: f64,
        size: f64,
        fees: f64,
        source_wallet: Option<String>,
    ) {
        let position = OpenPosition {
            market_slug: market_slug.clone(),
            token_id,
            side,
            entry_price: price,
            size,
            fees_paid_entry: fees,
            entry_time: Utc::now(),
        };
        
        self.open_positions.insert(market_slug, position);
        self.total_fees_paid += fees;
    }

    /// Record a position exit (sell) and calculate realized PnL
    pub fn record_exit(
        &mut self,
        market_slug: &str,
        exit_price: f64,
        exit_fees: f64,
    ) -> Option<RealizedPnL> {
        let position = self.open_positions.remove(market_slug)?;
        
        let gross_pnl = match position.side {
            TradeSide::Long => (exit_price - position.entry_price) * position.size,
            TradeSide::Short => (position.entry_price - exit_price) * position.size,
        };
        
        let total_fees = position.fees_paid_entry + exit_fees;
        let net_pnl = gross_pnl - total_fees;
        
        let return_pct = if position.entry_price > 0.0 {
            (net_pnl / (position.entry_price * position.size)) * 100.0
        } else {
            0.0
        };
        
        let holding_period = Utc::now().signed_duration_since(position.entry_time);
        let holding_period_hours = holding_period.num_seconds() as f64 / 3600.0;
        
        let trade = CompletedTrade {
            id: uuid::Uuid::new_v4().to_string(),
            market_slug: market_slug.to_string(),
            token_id: position.token_id,
            side: position.side,
            entry_price: position.entry_price,
            exit_price,
            size: position.size,
            fees_paid: total_fees,
            entry_time: position.entry_time,
            exit_time: Utc::now(),
            source_wallet: None, // Would be populated from signal
        };
        
        let realized = RealizedPnL {
            trade_id: trade.id.clone(),
            market_slug: market_slug.to_string(),
            gross_pnl,
            fees: total_fees,
            net_pnl,
            return_pct,
            holding_period_hours,
            closed_at: Utc::now(),
        };
        
        self.completed_trades.push(trade);
        self.realized_pnls.push(realized.clone());
        self.total_fees_paid += exit_fees;
        
        // Update daily PnL
        let today = Utc::now().date_naive();
        let daily = self.daily_pnls.entry(today).or_insert_with(|| DailyPnL {
            date: today,
            realized_pnl: 0.0,
            fees: 0.0,
            net_pnl: 0.0,
            trade_count: 0,
            win_count: 0,
            loss_count: 0,
        });
        
        daily.realized_pnl += gross_pnl;
        daily.fees += total_fees;
        daily.net_pnl += net_pnl;
        daily.trade_count += 1;
        
        if net_pnl > 0.0 {
            daily.win_count += 1;
        } else {
            daily.loss_count += 1;
        }
        
        // Update high water mark and drawdown
        let cumulative_pnl = self.total_realized_pnl();
        if cumulative_pnl > self.high_water_mark {
            self.high_water_mark = cumulative_pnl;
            self.current_drawdown = 0.0;
        } else {
            self.current_drawdown = self.high_water_mark - cumulative_pnl;
            if self.current_drawdown > self.max_drawdown {
                self.max_drawdown = self.current_drawdown;
            }
        }
        
        Some(realized)
    }

    /// Calculate unrealized PnL for open positions given current prices
    pub fn calculate_unrealized_pnls(
        &self,
        current_prices: &HashMap<String, f64>,
        fee_bps: f64,
    ) -> Vec<UnrealizedPnL> {
        self.open_positions
            .values()
            .filter_map(|pos| {
                let current_price = *current_prices.get(&pos.market_slug)?;
                
                let gross_pnl = match pos.side {
                    TradeSide::Long => (current_price - pos.entry_price) * pos.size,
                    TradeSide::Short => (pos.entry_price - current_price) * pos.size,
                };
                
                let notional_value = current_price * pos.size;
                let fees_estimate = notional_value * (fee_bps / 10_000.0);
                
                let return_pct = if pos.entry_price > 0.0 {
                    (gross_pnl / (pos.entry_price * pos.size)) * 100.0
                } else {
                    0.0
                };
                
                Some(UnrealizedPnL {
                    market_slug: pos.market_slug.clone(),
                    token_id: pos.token_id.clone(),
                    side: pos.side,
                    entry_price: pos.entry_price,
                    current_price,
                    size: pos.size,
                    gross_pnl,
                    return_pct,
                    fees_estimate,
                    net_pnl_estimate: gross_pnl - fees_estimate,
                })
            })
            .collect()
    }

    /// Get total realized PnL
    pub fn total_realized_pnl(&self) -> f64 {
        self.realized_pnls.iter().map(|r| r.net_pnl).sum()
    }

    /// Get total unrealized PnL
    pub fn total_unrealized_pnl(&self, current_prices: &HashMap<String, f64>, fee_bps: f64) -> f64 {
        self.calculate_unrealized_pnls(current_prices, fee_bps)
            .iter()
            .map(|u| u.net_pnl_estimate)
            .sum()
    }

    /// Generate comprehensive PnL summary
    pub fn generate_summary(
        &self,
        current_prices: &HashMap<String, f64>,
        fee_bps: f64,
    ) -> PnLSummary {
        let wins: Vec<&RealizedPnL> = self.realized_pnls.iter().filter(|r| r.net_pnl > 0.0).collect();
        let losses: Vec<&RealizedPnL> = self.realized_pnls.iter().filter(|r| r.net_pnl <= 0.0).collect();
        
        let total_wins: f64 = wins.iter().map(|w| w.net_pnl).sum();
        let total_losses: f64 = losses.iter().map(|l| l.net_pnl.abs()).sum();
        
        let avg_win = if !wins.is_empty() { total_wins / wins.len() as f64 } else { 0.0 };
        let avg_loss = if !losses.is_empty() { total_losses / losses.len() as f64 } else { 0.0 };
        
        let profit_factor = if total_losses > 0.0 {
            total_wins / total_losses
        } else if total_wins > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        
        let realized = self.total_realized_pnl();
        let unrealized = self.total_unrealized_pnl(current_prices, fee_bps);
        
        // Calculate max drawdown percentage
        let max_drawdown_pct = if self.high_water_mark > 0.0 {
            (self.max_drawdown / self.high_water_mark) * 100.0
        } else {
            0.0
        };
        
        PnLSummary {
            total_realized_pnl: realized,
            total_unrealized_pnl: unrealized,
            total_fees_paid: self.total_fees_paid,
            total_fees_estimated: self.calculate_unrealized_pnls(current_prices, fee_bps)
                .iter()
                .map(|u| u.fees_estimate)
                .sum(),
            net_pnl: realized + unrealized,
            win_count: wins.len() as u32,
            loss_count: losses.len() as u32,
            win_rate: if !self.realized_pnls.is_empty() {
                (wins.len() as f64 / self.realized_pnls.len() as f64) * 100.0
            } else {
                0.0
            },
            avg_win,
            avg_loss,
            profit_factor,
            sharpe_ratio: self.calculate_sharpe_ratio(),
            max_drawdown_pct,
            trading_days: self.daily_pnls.len() as u32,
        }
    }

    /// Calculate Sharpe ratio from daily returns
    fn calculate_sharpe_ratio(&self) -> Option<f64> {
        if self.daily_pnls.len() < 2 {
            return None;
        }
        
        let returns: Vec<f64> = self.daily_pnls.values().map(|d| d.net_pnl).collect();
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / (returns.len() - 1) as f64;
        
        let std_dev = variance.sqrt();
        
        if std_dev > 0.0 {
            // Annualized Sharpe (assuming 252 trading days, risk-free rate = 0)
            Some((mean / std_dev) * (252.0_f64.sqrt()))
        } else {
            None
        }
    }

    /// Get recent trades (last N)
    pub fn recent_trades(&self, n: usize) -> Vec<&CompletedTrade> {
        self.completed_trades.iter().rev().take(n).collect()
    }

    /// Get best and worst trades
    pub fn best_and_worst_trades(&self) -> (Option<&RealizedPnL>, Option<&RealizedPnL>) {
        let best = self.realized_pnls.iter().max_by(|a, b| {
            a.net_pnl.partial_cmp(&b.net_pnl).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        let worst = self.realized_pnls.iter().min_by(|a, b| {
            a.net_pnl.partial_cmp(&b.net_pnl).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        (best, worst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_entry_and_exit() {
        let mut tracker = PnLTracker::new();
        
        // Enter long position
        tracker.record_entry(
            "market-1".to_string(),
            "token-1".to_string(),
            TradeSide::Long,
            0.50,
            100.0,
            0.40, // $0.40 fees
            None,
        );
        
        assert_eq!(tracker.open_positions.len(), 1);
        assert_eq!(tracker.total_fees_paid, 0.40);
        
        // Exit at profit
        let realized = tracker.record_exit("market-1", 0.60, 0.48);
        
        assert!(realized.is_some());
        let r = realized.unwrap();
        assert!((r.gross_pnl - 10.0).abs() < 0.001); // (0.60 - 0.50) * 100
        assert!((r.fees - 0.88).abs() < 0.001); // 0.40 + 0.48
        assert!((r.net_pnl - 9.12).abs() < 0.001); // 10.0 - 0.88
        assert_eq!(tracker.open_positions.len(), 0);
        assert_eq!(tracker.realized_pnls.len(), 1);
    }

    #[test]
    fn test_short_position_pnl() {
        let mut tracker = PnLTracker::new();
        
        // Enter short position (betting NO, price goes down)
        tracker.record_entry(
            "market-1".to_string(),
            "token-1".to_string(),
            TradeSide::Short,
            0.60,
            100.0,
            0.48,
            None,
        );
        
        // Exit as price drops (profit for short)
        let realized = tracker.record_exit("market-1", 0.40, 0.32);
        
        let r = realized.unwrap();
        assert!((r.gross_pnl - 20.0).abs() < 0.001); // (0.60 - 0.40) * 100
        assert!(r.net_pnl > 0.0);
    }

    #[test]
    fn test_short_position_loss() {
        let mut tracker = PnLTracker::new();
        
        // Enter short position
        tracker.record_entry(
            "market-1".to_string(),
            "token-1".to_string(),
            TradeSide::Short,
            0.40,
            100.0,
            0.32,
            None,
        );
        
        // Exit as price rises (loss for short)
        let realized = tracker.record_exit("market-1", 0.60, 0.48);
        
        let r = realized.unwrap();
        assert!((r.gross_pnl - (-20.0)).abs() < 0.001); // (0.40 - 0.60) * 100
        assert!(r.net_pnl < 0.0);
    }

    #[test]
    fn test_unrealized_pnl_calculation() {
        let mut tracker = PnLTracker::new();
        
        tracker.record_entry(
            "market-1".to_string(),
            "token-1".to_string(),
            TradeSide::Long,
            0.50,
            100.0,
            0.40,
            None,
        );
        
        let mut prices = HashMap::new();
        prices.insert("market-1".to_string(), 0.65);
        
        let unrealized = tracker.calculate_unrealized_pnls(&prices, 8.0);
        
        assert_eq!(unrealized.len(), 1);
        assert!((unrealized[0].gross_pnl - 15.0).abs() < 0.001); // (0.65 - 0.50) * 100
    }

    #[test]
    fn test_pnl_summary() {
        let mut tracker = PnLTracker::new();
        
        // Two winning trades
        tracker.record_entry("m1".to_string(), "t1".to_string(), TradeSide::Long, 0.50, 100.0, 0.40, None);
        tracker.record_exit("m1", 0.60, 0.48);
        
        tracker.record_entry("m2".to_string(), "t2".to_string(), TradeSide::Long, 0.40, 100.0, 0.32, None);
        tracker.record_exit("m2", 0.55, 0.44);
        
        // One losing trade
        tracker.record_entry("m3".to_string(), "t3".to_string(), TradeSide::Long, 0.70, 100.0, 0.56, None);
        tracker.record_exit("m3", 0.60, 0.48);
        
        let prices = HashMap::new();
        let summary = tracker.generate_summary(&prices, 8.0);
        
        assert_eq!(summary.win_count, 2);
        assert_eq!(summary.loss_count, 1);
        assert_eq!(summary.win_rate, 66.66666666666666);
        assert!(summary.profit_factor > 1.0); // More wins than losses
    }

    #[test]
    fn test_daily_pnl_tracking() {
        let mut tracker = PnLTracker::new();
        
        tracker.record_entry("m1".to_string(), "t1".to_string(), TradeSide::Long, 0.50, 100.0, 0.40, None);
        tracker.record_exit("m1", 0.60, 0.48);
        
        let today = Utc::now().date_naive();
        let daily = tracker.daily_pnls.get(&today);
        
        assert!(daily.is_some());
        let d = daily.unwrap();
        assert_eq!(d.trade_count, 1);
        assert_eq!(d.win_count, 1);
        assert_eq!(d.loss_count, 0);
    }

    #[test]
    fn test_drawdown_calculation() {
        let mut tracker = PnLTracker::new();
        
        // Win
        tracker.record_entry("m1".to_string(), "t1".to_string(), TradeSide::Long, 0.50, 100.0, 0.40, None);
        tracker.record_exit("m1", 0.70, 0.56);
        
        // Loss (drawdown from peak)
        tracker.record_entry("m2".to_string(), "t2".to_string(), TradeSide::Long, 0.60, 100.0, 0.48, None);
        tracker.record_exit("m2", 0.40, 0.32);
        
        assert!(tracker.max_drawdown > 0.0);
        assert!(tracker.current_drawdown > 0.0 || tracker.current_drawdown == 0.0);
    }

    #[test]
    fn test_best_and_worst_trades() {
        let mut tracker = PnLTracker::new();
        
        tracker.record_entry("m1".to_string(), "t1".to_string(), TradeSide::Long, 0.50, 100.0, 0.40, None);
        tracker.record_exit("m1", 0.80, 0.64); // Big win
        
        tracker.record_entry("m2".to_string(), "t2".to_string(), TradeSide::Long, 0.60, 100.0, 0.48, None);
        tracker.record_exit("m2", 0.30, 0.24); // Big loss
        
        tracker.record_entry("m3".to_string(), "t3".to_string(), TradeSide::Long, 0.50, 100.0, 0.40, None);
        tracker.record_exit("m3", 0.55, 0.44); // Small win
        
        let (best, worst) = tracker.best_and_worst_trades();
        
        assert!(best.is_some());
        assert!(worst.is_some());
        assert!(best.unwrap().net_pnl > 0.0);
        assert!(worst.unwrap().net_pnl < 0.0);
    }
}
