# Reuse Notes

This implementation reuses and adapts patterns from the requested source projects:

1. `whale-copy-trader/src/copytrader.ts`
   - execution dedupe cache pattern
   - reconnect/backoff strategy (`compute_backoff_delay`)
   - signal -> guards -> order-intent pipeline shape

2. `polymarket-copy-trading-bot/rust/src/bin/mempool_monitor.rs`
   - low-latency event-processing pipeline design
   - bounded retry semantics for order submission
   - strict hot-path filtering before expensive work

3. `trading-core/scripts/watchdog.sh`
   - external watchdog process model
   - restart-if-down service behavior

4. `trading-core/src/safety/monitor.ts`
   - health/monitor loop cadence and critical alerting flow

5. `trading-core/scripts/log-forwarder.js`
   - Telegram notification formatting and event-forwarding pattern
