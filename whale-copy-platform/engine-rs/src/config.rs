use std::env;

use anyhow::Result;

use crate::types::{EngineMode, RiskProfile, RuntimeSettings};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub db_path: String,
    pub socket_path: String,
    pub data_api_base: String,
    pub execution_api_base: Option<String>,
    pub fetch_interval_ms: u64,
    pub rotation_interval_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub request_timeout_ms: u64,
    pub network_retry_limit: u32,
    pub rotation_rank_delta_threshold: f64,
    pub rotation_confidence_floor: f64,
    pub rotation_cooldown_secs: u64,
    pub source_equity_fallback_usd: f64,
    pub log_dir: String,
    pub log_retention_days: u32,
    pub rpcs: Vec<String>,
    pub log_level: String,
    pub command_tail_default: usize,
    pub runtime_settings: RuntimeSettings,
}

fn parse_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

fn parse_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

pub fn load_config() -> Result<EngineConfig> {
    dotenvy::dotenv().ok();

    let mode = match env::var("ENGINE_MODE").unwrap_or_else(|_| "dry_run".to_string()).to_ascii_lowercase().as_str() {
        "live" => EngineMode::Live,
        _ => EngineMode::DryRun,
    };

    let risk_profile = match env::var("RISK_PROFILE").unwrap_or_else(|_| "aggressive".to_string()).to_ascii_lowercase().as_str() {
        "conservative" => RiskProfile::Conservative,
        "balanced" => RiskProfile::Balanced,
        _ => RiskProfile::Aggressive,
    };

    let mut runtime_settings = RuntimeSettings {
        mode,
        risk_profile,
        multiplier: parse_or("COPY_MULTIPLIER", 1.25_f64),
        paused: parse_bool("START_PAUSED", false),
        follower_equity_usd: parse_or("FOLLOWER_EQUITY_USD", 10_000_f64),
        fee_bps: parse_or("FEE_BPS", 8_f64),
        slippage_bps: parse_or("SLIPPAGE_BPS", 12_f64),
        limit_price_offset: parse_or("LIMIT_PRICE_OFFSET", 0.01_f64),
        copy_sells: parse_bool("COPY_SELLS", true),
        max_active_wallets: parse_or("MAX_ACTIVE_WALLETS", 5_usize),
        ..RuntimeSettings::default()
    };

    runtime_settings.caps.max_copy_notional_per_trade = parse_or("MAX_COPY_NOTIONAL_PER_TRADE", 1_500_f64);
    runtime_settings.caps.max_market_exposure_usd = parse_or("MAX_MARKET_EXPOSURE_USD", 7_500_f64);
    runtime_settings.caps.max_daily_notional_usd = parse_or("MAX_DAILY_NOTIONAL_USD", 45_000_f64);
    runtime_settings.caps.max_open_positions = parse_or("MAX_OPEN_POSITIONS", 25_u32);

    let raw_rpcs = env::var("POLYGON_RPC_URLS").unwrap_or_default();
    let rpcs: Vec<String> = raw_rpcs
        .split(',')
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    if runtime_settings.multiplier <= 0.0 {
        anyhow::bail!("COPY_MULTIPLIER must be > 0");
    }

    if runtime_settings.limit_price_offset < 0.0 || runtime_settings.limit_price_offset >= 1.0 {
        anyhow::bail!("LIMIT_PRICE_OFFSET must be in [0, 1)");
    }

    if runtime_settings.follower_equity_usd <= 0.0 {
        anyhow::bail!("FOLLOWER_EQUITY_USD must be > 0");
    }

    if runtime_settings.max_active_wallets == 0 {
        anyhow::bail!("MAX_ACTIVE_WALLETS must be >= 1");
    }

    let db_path = env::var("ENGINE_DB_PATH").unwrap_or_else(|_| "./data/whale_copy_platform.db".to_string());
    let socket_path = env::var("ENGINE_SOCKET_PATH").unwrap_or_else(|_| "/tmp/whale-copy-engine.sock".to_string());

    let cfg = EngineConfig {
        db_path,
        socket_path,
        data_api_base: env::var("POLYMARKET_DATA_API_BASE")
            .unwrap_or_else(|_| "https://data-api.polymarket.com".to_string()),
        execution_api_base: env::var("EXECUTION_API_BASE")
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty()),
        fetch_interval_ms: parse_or("FETCH_INTERVAL_MS", 1_500_u64),
        rotation_interval_ms: parse_or("ROTATION_INTERVAL_MS", 120_000_u64),
        heartbeat_interval_ms: parse_or("HEARTBEAT_INTERVAL_MS", 5_000_u64),
        request_timeout_ms: parse_or("REQUEST_TIMEOUT_MS", 3_500_u64),
        network_retry_limit: parse_or("NETWORK_RETRY_LIMIT", 3_u32),
        rotation_rank_delta_threshold: parse_or("ROTATION_RANK_DELTA_THRESHOLD", 8.0_f64),
        rotation_confidence_floor: parse_or("ROTATION_CONFIDENCE_FLOOR", 52.0_f64),
        rotation_cooldown_secs: parse_or("ROTATION_COOLDOWN_SECS", 1_800_u64),
        source_equity_fallback_usd: parse_or("SOURCE_EQUITY_FALLBACK_USD", 100_000.0_f64),
        log_dir: env::var("ENGINE_LOG_DIR").unwrap_or_else(|_| "./logs".to_string()),
        log_retention_days: parse_or("LOG_RETENTION_DAYS", 7_u32),
        rpcs,
        log_level: env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        command_tail_default: parse_or("COMMAND_TAIL_DEFAULT", 50_usize),
        runtime_settings,
    };

    if cfg.fetch_interval_ms < 300 {
        anyhow::bail!("FETCH_INTERVAL_MS too low; minimum is 300ms");
    }

    if cfg.rotation_rank_delta_threshold <= 0.0 {
        anyhow::bail!("ROTATION_RANK_DELTA_THRESHOLD must be > 0");
    }

    if !(0.0..=100.0).contains(&cfg.rotation_confidence_floor) {
        anyhow::bail!("ROTATION_CONFIDENCE_FLOOR must be in [0, 100]");
    }

    if cfg.source_equity_fallback_usd <= 0.0 {
        anyhow::bail!("SOURCE_EQUITY_FALLBACK_USD must be > 0");
    }

    if cfg.log_retention_days == 0 {
        anyhow::bail!("LOG_RETENTION_DAYS must be >= 1");
    }

    Ok(cfg)
}
