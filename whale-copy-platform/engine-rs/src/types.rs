use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EngineMode {
    DryRun,
    Live,
}

impl EngineMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Live => "live",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskProfile {
    Conservative,
    Balanced,
    Aggressive,
}

impl RiskProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conservative => "conservative",
            Self::Balanced => "balanced",
            Self::Aggressive => "aggressive",
        }
    }

    pub fn execution_premium_bps(self) -> f64 {
        match self {
            Self::Conservative => 8.0,
            Self::Balanced => 12.0,
            Self::Aggressive => 16.0,
        }
    }

    pub fn max_retry_attempts(self) -> u8 {
        match self {
            Self::Conservative => 1,
            Self::Balanced => 2,
            Self::Aggressive => 3,
        }
    }

    pub fn max_price_deviation_bps(self) -> f64 {
        match self {
            Self::Conservative => 20.0,
            Self::Balanced => 30.0,
            Self::Aggressive => 45.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Caps {
    pub max_copy_notional_per_trade: f64,
    pub max_market_exposure_usd: f64,
    pub max_daily_notional_usd: f64,
    pub max_open_positions: u32,
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            max_copy_notional_per_trade: 1_500.0,
            max_market_exposure_usd: 7_500.0,
            max_daily_notional_usd: 45_000.0,
            max_open_positions: 25,
        }
    }
}

use crate::performance::AutoStopConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSettings {
    pub mode: EngineMode,
    pub risk_profile: RiskProfile,
    pub multiplier: f64,
    pub caps: Caps,
    pub paused: bool,
    pub follower_equity_usd: f64,
    pub fee_bps: f64,
    pub slippage_bps: f64,
    pub limit_price_offset: f64,
    pub copy_sells: bool,
    pub max_active_wallets: usize,
    pub auto_stop_config: AutoStopConfig,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            mode: EngineMode::DryRun,
            risk_profile: RiskProfile::Aggressive,
            multiplier: 1.25,
            caps: Caps::default(),
            paused: false,
            follower_equity_usd: 10_000.0,
            fee_bps: 8.0,
            slippage_bps: 12.0,
            limit_price_offset: 0.01,
            copy_sells: true,
            max_active_wallets: 5,
            auto_stop_config: AutoStopConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

impl TradeSide {
    pub fn is_buy(&self) -> bool {
        matches!(self, Self::Buy)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFillEvent {
    pub execution_id: String,
    pub source_wallet: String,
    pub market_slug: String,
    pub token_id: String,
    pub side: TradeSide,
    pub source_trade_notional_usd: f64,
    pub source_price: f64,
    pub source_quantity: f64,
    pub source_equity_usd: f64,
    pub expected_edge_bps: f64,
    pub observed_at: DateTime<Utc>,
    pub transaction_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopySignal {
    pub execution_id: String,
    pub source_wallet: String,
    pub market_slug: String,
    pub token_id: String,
    pub side: TradeSide,
    pub source_trade_notional_usd: f64,
    pub source_price: f64,
    pub copy_notional_usd: f64,
    pub copy_quantity: f64,
    pub expected_edge_bps: f64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskDecision {
    Allow { reason: String },
    Block { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStyle {
    LimitIoc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionIntent {
    pub execution_id: String,
    pub market_slug: String,
    pub token_id: String,
    pub side: TradeSide,
    pub order_style: OrderStyle,
    pub quantity: f64,
    pub limit_price: f64,
    pub max_attempts: u8,
    pub max_price_deviation_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub execution_id: String,
    pub success: bool,
    pub attempts: u8,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub latency_ms: f64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletScore {
    pub wallet: String,
    pub score: f64,
    pub pnl_consistency: f64,
    pub win_rate: f64,
    pub drawdown_penalty: f64,
    pub recent_activity: f64,
    pub fill_quality: f64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionStatus {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotationSuggestion {
    pub id: String,
    pub wallet: String,
    pub score: f64,
    pub reason: String,
    pub expected_impact: String,
    pub status: SuggestionStatus,
    pub created_at: DateTime<Utc>,
    pub decided_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventClass {
    Critical,
    Trade,
    Risk,
    Rotation,
    Info,
}

impl EventClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Trade => "trade",
            Self::Risk => "risk",
            Self::Rotation => "rotation",
            Self::Info => "info",
        }
    }

    pub fn from_str(value: &str) -> Self {
        match value {
            "critical" => Self::Critical,
            "trade" => Self::Trade,
            "risk" => Self::Risk,
            "rotation" => Self::Rotation,
            _ => Self::Info,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: String,
    pub ts: DateTime<Utc>,
    pub class: EventClass,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineHealthSnapshot {
    pub mode: EngineMode,
    pub risk_profile: RiskProfile,
    pub paused: bool,
    pub tracked_wallet_count: usize,
    pub pending_suggestion_count: usize,
    pub daily_notional_usd: f64,
    pub follower_equity_usd: f64,
    pub recent_event_count: usize,
    pub last_heartbeat_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub method: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub actor_chat_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl RpcResponse {
    pub fn ok(result: Value, request_id: Option<String>) -> Self {
        Self {
            ok: true,
            result: Some(result),
            error: None,
            request_id,
        }
    }

    pub fn err(error: impl Into<String>, request_id: Option<String>) -> Self {
        Self {
            ok: false,
            result: None,
            error: Some(error.into()),
            request_id,
        }
    }
}
