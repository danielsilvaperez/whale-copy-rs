use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use chrono::{NaiveDate, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::performance::PerformanceTracker;
use crate::types::{
    EngineHealthSnapshot, EventClass, EventEnvelope, RotationSuggestion, RuntimeSettings,
};

#[derive(Debug)]
pub struct DedupeCache {
    max_size: usize,
    seen: HashSet<String>,
    queue: VecDeque<String>,
}

impl DedupeCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            seen: HashSet::with_capacity(max_size),
            queue: VecDeque::with_capacity(max_size),
        }
    }

    pub fn add(&mut self, key: &str) -> bool {
        if self.seen.contains(key) {
            return false;
        }

        self.seen.insert(key.to_owned());
        self.queue.push_back(key.to_owned());

        while self.queue.len() > self.max_size {
            if let Some(oldest) = self.queue.pop_front() {
                self.seen.remove(&oldest);
            }
        }

        true
    }
}

#[derive(Debug)]
pub struct EngineState {
    pub settings: RuntimeSettings,
    pub tracked_wallets: BTreeSet<String>,
    pub pending_suggestions: BTreeMap<String, RotationSuggestion>,
    pub event_buffer: VecDeque<EventEnvelope>,
    pub dedupe_cache: DedupeCache,
    pub daily_notional_usd: f64,
    pub open_positions_count: u32,
    pub market_exposure_usd: HashMap<String, f64>,
    pub last_heartbeat_at: chrono::DateTime<chrono::Utc>,
    pub last_rotation_at: Option<chrono::DateTime<chrono::Utc>>,
    pub current_trading_day: NaiveDate,
    pub performance_tracker: PerformanceTracker,
}

impl EngineState {
    pub fn new(settings: RuntimeSettings, tracked_wallets: BTreeSet<String>) -> Self {
        let auto_stop_config = settings.auto_stop_config.clone();
        Self {
            settings,
            tracked_wallets,
            pending_suggestions: BTreeMap::new(),
            event_buffer: VecDeque::with_capacity(5_000),
            dedupe_cache: DedupeCache::new(100_000),
            daily_notional_usd: 0.0,
            open_positions_count: 0,
            market_exposure_usd: HashMap::new(),
            last_heartbeat_at: Utc::now(),
            last_rotation_at: None,
            current_trading_day: Utc::now().date_naive(),
            performance_tracker: PerformanceTracker::new(auto_stop_config),
        }
    }

    pub fn push_event(
        &mut self,
        class: EventClass,
        event_type: impl Into<String>,
        payload: Value,
    ) -> EventEnvelope {
        let event = EventEnvelope {
            id: Uuid::new_v4().to_string(),
            ts: Utc::now(),
            class,
            event_type: event_type.into(),
            payload,
        };

        self.event_buffer.push_back(event.clone());
        while self.event_buffer.len() > 5_000 {
            self.event_buffer.pop_front();
        }

        event
    }

    pub fn maybe_reset_daily_notional(&mut self) {
        let today = Utc::now().date_naive();
        if today != self.current_trading_day {
            tracing::info!(
                old_day = %self.current_trading_day,
                new_day = %today,
                reset_amount = self.daily_notional_usd,
                "daily notional counter reset"
            );
            self.daily_notional_usd = 0.0;
            self.current_trading_day = today;
        }
    }

    pub fn health_snapshot(&self) -> EngineHealthSnapshot {
        EngineHealthSnapshot {
            mode: self.settings.mode,
            risk_profile: self.settings.risk_profile,
            paused: self.settings.paused,
            tracked_wallet_count: self.tracked_wallets.len(),
            pending_suggestion_count: self.pending_suggestions.len(),
            daily_notional_usd: self.daily_notional_usd,
            follower_equity_usd: self.settings.follower_equity_usd,
            recent_event_count: self.event_buffer.len(),
            last_heartbeat_at: self.last_heartbeat_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::{DedupeCache, EngineState};
    use crate::types::{EventClass, RuntimeSettings};

    #[test]
    fn dedupe_cache_rejects_duplicates() {
        let mut cache = DedupeCache::new(3);
        assert!(cache.add("a"));
        assert!(!cache.add("a"));
        assert!(cache.add("b"));
        assert!(cache.add("c"));
        assert!(cache.add("d"));
        assert!(cache.add("a"));
    }

    #[test]
    fn maybe_reset_daily_notional_resets_on_new_day() {
        let mut state = EngineState::new(RuntimeSettings::default(), BTreeSet::new());
        state.daily_notional_usd = 9_500.0;
        state.current_trading_day = chrono::Utc::now().date_naive() - chrono::Duration::days(1);
        state.maybe_reset_daily_notional();
        assert_eq!(state.daily_notional_usd, 0.0);
        assert_eq!(state.current_trading_day, chrono::Utc::now().date_naive());
    }

    #[test]
    fn maybe_reset_daily_notional_noop_same_day() {
        let mut state = EngineState::new(RuntimeSettings::default(), BTreeSet::new());
        state.daily_notional_usd = 9_500.0;
        state.maybe_reset_daily_notional(); // today == today
        assert_eq!(state.daily_notional_usd, 9_500.0);
    }

    #[test]
    fn event_buffer_caps_at_5000() {
        let mut state = EngineState::new(RuntimeSettings::default(), BTreeSet::new());
        for i in 0..5_100usize {
            state.push_event(EventClass::Info, format!("e{i}"), json!({}));
        }
        assert_eq!(state.event_buffer.len(), 5_000);
    }
}
