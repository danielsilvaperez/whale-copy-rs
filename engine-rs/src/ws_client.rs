use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, timeout};
use tokio_tungstenite::{
    connect_async, MaybeTlsStream, WebSocketStream,
};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tracing;

use crate::types::{WsChannel, WsConfig, WsEvent, WsMessage};

/// Shared state for the WebSocket client
#[derive(Debug, Clone)]
pub struct WsClientState {
    pub connected: bool,
    pub subscribed_channels: Vec<WsChannel>,
    pub last_message_at: Option<Instant>,
    pub consecutive_failures: u32,
    pub client_id: Option<String>,
}

impl WsClientState {
    pub fn new() -> Self {
        Self {
            connected: false,
            subscribed_channels: Vec::new(),
            last_message_at: None,
            consecutive_failures: 0,
            client_id: None,
        }
    }
}

/// WebSocket client for Polymarket CLOB
pub struct WsClient {
    config: WsConfig,
    state: Arc<RwLock<WsClientState>>,
    event_tx: mpsc::Sender<WsEvent>,
    command_rx: Arc<RwLock<mpsc::Receiver<WsCommand>>>,
}

#[derive(Debug, Clone)]
pub enum WsCommand {
    Subscribe(Vec<WsChannel>),
    Unsubscribe(Vec<WsChannel>),
    Shutdown,
}

impl WsClient {
    /// Create a new WebSocket client
    pub fn new(
        config: WsConfig,
        event_tx: mpsc::Sender<WsEvent>,
    ) -> (Self, mpsc::Sender<WsCommand>) {
        let (command_tx, command_rx) = mpsc::channel(100);

        let client = Self {
            config,
            state: Arc::new(RwLock::new(WsClientState::new())),
            event_tx,
            command_rx: Arc::new(RwLock::new(command_rx)),
        };

        (client, command_tx)
    }

    /// Start the WebSocket connection loop with automatic reconnection
    pub async fn run(self) {
        let mut consecutive_failures = 0u32;

        loop {
            match self.connect_and_run().await {
                Ok(()) => {
                    tracing::info!("WebSocket connection closed gracefully");
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    tracing::error!(
                        error = %e,
                        consecutive_failures,
                        "WebSocket connection failed"
                    );

                    // Check if we've exceeded max failures
                    if consecutive_failures >= self.config.max_consecutive_failures {
                        tracing::error!(
                            max_failures = self.config.max_consecutive_failures,
                            "Max consecutive failures reached, stopping WebSocket client"
                        );
                        let _ = self
                            .event_tx
                            .send(WsEvent::Error(
                                "Max consecutive failures reached".to_string(),
                            ))
                            .await;
                        break;
                    }

                    // Update state
                    {
                        let mut state = self.state.write().await;
                        state.connected = false;
                        state.consecutive_failures = consecutive_failures;
                    }

                    // Compute backoff delay
                    let delay = compute_backoff_delay(
                        self.config.reconnect_base_ms,
                        self.config.reconnect_max_ms,
                        consecutive_failures,
                    );

                    tracing::info!(delay_ms = delay, "Reconnecting after backoff");
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }

    /// Connect and run until disconnect or error
    async fn connect_and_run(&self) -> Result<()> {
        // Attempt connection with timeout
        let ws_stream = match timeout(
            Duration::from_secs(self.config.connection_timeout_secs),
            connect_async(&self.config.url),
        )
        .await
        {
            Ok(Ok((stream, _))) => stream,
            Ok(Err(e)) => return Err(anyhow::anyhow!("WebSocket connection failed: {}", e)),
            Err(_) => return Err(anyhow::anyhow!("WebSocket connection timeout")),
        };

        tracing::info!("WebSocket connected to {}", self.config.url);

        // Update state
        {
            let mut state = self.state.write().await;
            state.connected = true;
            state.last_message_at = Some(Instant::now());
        }

        // Notify engine
        self.event_tx
            .send(WsEvent::Connected)
            .await
            .context("Failed to send connected event")?;

        // Split the stream
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Resubscribe to previously subscribed channels
        let channels_to_resubscribe = {
            let state = self.state.read().await;
            state.subscribed_channels.clone()
        };

        if !channels_to_resubscribe.is_empty() {
            let channels: Vec<String> = channels_to_resubscribe
                .iter()
                .map(|c| c.as_subscribe_string())
                .collect();

            let subscribe_msg = WsMessage::Subscribe { channels };
            let msg_text =
                serde_json::to_string(&subscribe_msg).context("Failed to serialize subscribe")?;

            ws_sender
                .send(TungsteniteMessage::Text(msg_text.into()))
                .await
                .context("Failed to send resubscribe")?;

            tracing::info!(
                channel_count = channels_to_resubscribe.len(),
                "Resubscribed to channels after reconnect"
            );
        }

        // Start ping interval
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));

        // Main message loop
        loop {
            tokio::select! {
                // Handle incoming WebSocket messages
                Some(msg_result) = ws_receiver.next() => {
                    match msg_result {
                        Ok(msg) => {
                            // Update last message time
                            {
                                let mut state = self.state.write().await;
                                state.last_message_at = Some(Instant::now());
                            }

                            match msg {
                                TungsteniteMessage::Text(text) => {
                                    if let Err(e) = self.handle_text_message(&text).await {
                                        tracing::warn!(error = %e, "Failed to handle message");
                                    }
                                }
                                TungsteniteMessage::Binary(bin) => {
                                    // Try to parse as text
                                    if let Ok(text) = String::from_utf8(bin.to_vec()) {
                                        if let Err(e) = self.handle_text_message(&text).await {
                                            tracing::warn!(error = %e, "Failed to handle binary message");
                                        }
                                    }
                                }
                                TungsteniteMessage::Ping(_data) => {
                                    // Pong is handled automatically by tungstenite
                                    tracing::trace!("Received ping");
                                }
                                TungsteniteMessage::Pong(_) => {
                                    tracing::trace!("Received pong");
                                }
                                TungsteniteMessage::Close(frame) => {
                                    tracing::info!(?frame, "WebSocket closed by server");
                                    let reason = frame
                                        .as_ref()
                                        .map(|f| f.reason.to_string())
                                        .unwrap_or_else(|| "Unknown".to_string());
                                    let _ = self
                                        .event_tx
                                        .send(WsEvent::Disconnected { reason })
                                        .await;
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "WebSocket receive error");
                            let _ = self
                                .event_tx
                                .send(WsEvent::Disconnected {
                                    reason: format!("Receive error: {}", e),
                                })
                                .await;
                            return Err(anyhow::anyhow!("WebSocket error: {}", e));
                        }
                    }
                }

                // Handle ping interval
                _ = ping_interval.tick() => {
                    let ping_msg = WsMessage::Ping;
                    let msg_text = serde_json::to_string(&ping_msg)
                        .context("Failed to serialize ping")?;

                    if let Err(e) = ws_sender
                        .send(TungsteniteMessage::Text(msg_text.into()))
                        .await
                    {
                        tracing::warn!(error = %e, "Failed to send ping");
                        return Err(anyhow::anyhow!("Ping failed: {}", e));
                    }

                    // Check if we've missed messages for too long
                    let should_disconnect = {
                        let state = self.state.read().await;
                        state.last_message_at.map_or(false, |last| {
                            last.elapsed() > Duration::from_secs(self.config.ping_interval_secs * 3)
                        })
                    };

                    if should_disconnect {
                        tracing::warn!("No messages received for too long, reconnecting");
                        return Err(anyhow::anyhow!("Connection stale"));
                    }
                }

                // Handle commands from engine
                cmd = async {
                    let mut rx = self.command_rx.write().await;
                    rx.recv().await
                } => {
                    match cmd {
                        Some(WsCommand::Subscribe(channels)) => {
                            if let Err(e) = self.handle_subscribe(&mut ws_sender, channels).await {
                                tracing::warn!(error = %e, "Subscribe failed");
                            }
                        }
                        Some(WsCommand::Unsubscribe(channels)) => {
                            if let Err(e) = self.handle_unsubscribe(&mut ws_sender, channels).await {
                                tracing::warn!(error = %e, "Unsubscribe failed");
                            }
                        }
                        Some(WsCommand::Shutdown) => {
                            tracing::info!("Shutdown command received");
                            let _ = ws_sender.close().await;
                            break;
                        }
                        None => {
                            tracing::info!("Command channel closed");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_text_message(&self, text: &str) -> Result<()> {
        // Try to parse as WsMessage
        match serde_json::from_str::<WsMessage>(text) {
            Ok(ws_msg) => {
                // Update client_id if this is a connected message
                if let WsMessage::Connected { client_id } = &ws_msg {
                    let mut state = self.state.write().await;
                    state.client_id = Some(client_id.clone());
                    tracing::info!(client_id, "WebSocket client registered");
                }

                // Forward to engine
                self.event_tx
                    .send(WsEvent::Message(ws_msg))
                    .await
                    .context("Failed to send message event")?;
            }
            Err(e) => {
                // Could be a raw message we don't recognize, log at debug
                tracing::debug!(error = %e, text = %text, "Failed to parse WebSocket message");
            }
        }

        Ok(())
    }

    async fn handle_subscribe(
        &self,
        ws_sender: &mut futures::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
            TungsteniteMessage,
        >,
        channels: Vec<WsChannel>,
    ) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        let channel_strings: Vec<String> = channels
            .iter()
            .map(|c| c.as_subscribe_string())
            .collect();

        let subscribe_msg = WsMessage::Subscribe {
            channels: channel_strings.clone(),
        };

        let msg_text =
            serde_json::to_string(&subscribe_msg).context("Failed to serialize subscribe")?;

        ws_sender
            .send(TungsteniteMessage::Text(msg_text.into()))
            .await
            .context("Failed to send subscribe")?;

        // Update state
        {
            let mut state = self.state.write().await;
            for channel in channels {
                if !state.subscribed_channels.contains(&channel) {
                    state.subscribed_channels.push(channel);
                }
            }
        }

        tracing::info!(channels = ?channel_strings, "Subscribed to channels");
        Ok(())
    }

    async fn handle_unsubscribe(
        &self,
        ws_sender: &mut futures::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
            TungsteniteMessage,
        >,
        channels: Vec<WsChannel>,
    ) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        let channel_strings: Vec<String> = channels
            .iter()
            .map(|c| c.as_subscribe_string())
            .collect();

        let unsubscribe_msg = WsMessage::Unsubscribe {
            channels: channel_strings.clone(),
        };

        let msg_text =
            serde_json::to_string(&unsubscribe_msg).context("Failed to serialize unsubscribe")?;

        ws_sender
            .send(TungsteniteMessage::Text(msg_text.into()))
            .await
            .context("Failed to send unsubscribe")?;

        // Update state
        {
            let mut state = self.state.write().await;
            state
                .subscribed_channels
                .retain(|c| !channels.contains(c));
        }

        tracing::info!(channels = ?channel_strings, "Unsubscribed from channels");
        Ok(())
    }

    /// Get current client state
    pub async fn get_state(&self) -> WsClientState {
        self.state.read().await.clone()
    }
}

/// Compute exponential backoff delay with jitter
pub fn compute_backoff_delay(base_ms: u64, max_ms: u64, attempt: u32) -> u64 {
    // Exponential growth: base * 2^attempt
    let growth = base_ms.saturating_mul(2_u64.saturating_pow(attempt.min(10)));
    let capped = growth.min(max_ms);

    // Add jitter: ±25%
    let jitter_range = (capped / 4).max(1);
    let jitter = fastrand::i64(-(jitter_range as i64)..=jitter_range as i64) as u64;

    if capped > jitter {
        capped + jitter
    } else {
        capped
    }
}

/// Convert a TradeFill to a SourceFillEvent for processing
pub fn trade_fill_to_source_event(
    market_slug: &str,
    fill: &crate::types::TradeFill,
    source_equity_fallback: f64,
) -> Option<crate::types::SourceFillEvent> {
    use crate::types::{SourceFillEvent, TradeFill, TradeSide};
    use chrono::Utc;

    if fill.size <= 0.0 || fill.price <= 0.0 {
        return None;
    }

    let notional = fill.size * fill.price;

    Some(SourceFillEvent {
        execution_id: fill.trade_id.clone(),
        source_wallet: fill.taker_address.clone(),
        market_slug: market_slug.to_string(),
        token_id: fill.token_id.clone(),
        side: fill.side.clone(),
        source_trade_notional_usd: notional,
        source_price: fill.price,
        source_quantity: fill.size,
        source_equity_usd: source_equity_fallback,
        expected_edge_bps: 50.0, // Default, would need orderbook data for real calc
        observed_at: fill.timestamp,
        transaction_hash: Some(fill.trade_id.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TradeFill, TradeSide};
    use chrono::Utc;

    #[test]
    fn ws_channel_subscribe_string_roundtrip() {
        let channels = vec![
            WsChannel::Trade {
                market_slug: "btc-up".to_string(),
            },
            WsChannel::Orderbook {
                market_slug: "eth-down".to_string(),
            },
            WsChannel::User {
                wallet: "0x1234567890123456789012345678901234567890".to_string(),
            },
        ];

        for original in channels {
            let s = original.as_subscribe_string();
            let parsed = WsChannel::from_subscribe_string(&s);
            assert_eq!(Some(original), parsed);
        }
    }

    #[test]
    fn ws_channel_from_invalid_string() {
        assert!(WsChannel::from_subscribe_string("").is_none());
        assert!(WsChannel::from_subscribe_string("invalid").is_none());
        assert!(WsChannel::from_subscribe_string("unknown:channel").is_none());
        assert!(WsChannel::from_subscribe_string("trade").is_none()); // missing colon
    }

    #[test]
    fn compute_backoff_delay_saturates_at_max() {
        // Very high attempt should still return max_ms (with jitter)
        let delay = compute_backoff_delay(250, 5_000, 100);
        assert!(delay >= 3_750 && delay <= 6_250); // max ±25% jitter
    }

    #[test]
    fn compute_backoff_delay_increases_with_attempts() {
        let d1 = compute_backoff_delay(100, 10_000, 0);
        let d2 = compute_backoff_delay(100, 10_000, 1);
        let d3 = compute_backoff_delay(100, 10_000, 2);

        // Should generally increase (allowing for jitter)
        assert!(d2 >= d1 || (d2 as i64 - d1 as i64).abs() < 100);
        assert!(d3 >= d2 || (d3 as i64 - d2 as i64).abs() < 200);
    }

    #[test]
    fn trade_fill_to_source_event_valid() {
        let fill = TradeFill {
            trade_id: "trade-123".to_string(),
            maker_address: "0x1111111111111111111111111111111111111111".to_string(),
            taker_address: "0x2222222222222222222222222222222222222222".to_string(),
            side: TradeSide::Buy,
            size: 100.0,
            price: 0.5,
            token_id: "token-abc".to_string(),
            timestamp: Utc::now(),
        };

        let event = trade_fill_to_source_event("market-xyz", &fill, 50_000.0);
        assert!(event.is_some());

        let event = event.unwrap();
        assert_eq!(event.execution_id, "trade-123");
        assert_eq!(event.source_wallet, "0x2222222222222222222222222222222222222222");
        assert_eq!(event.market_slug, "market-xyz");
        assert_eq!(event.token_id, "token-abc");
        assert_eq!(event.source_trade_notional_usd, 50.0); // 100 * 0.5
        assert_eq!(event.source_price, 0.5);
    }

    #[test]
    fn trade_fill_to_source_event_invalid() {
        // Zero size should return None
        let fill_zero_size = TradeFill {
            trade_id: "trade-1".to_string(),
            maker_address: "0x1111".to_string(),
            taker_address: "0x2222".to_string(),
            side: TradeSide::Buy,
            size: 0.0,
            price: 0.5,
            token_id: "token".to_string(),
            timestamp: Utc::now(),
        };
        assert!(trade_fill_to_source_event("market", &fill_zero_size, 1000.0).is_none());

        // Zero price should return None
        let fill_zero_price = TradeFill {
            trade_id: "trade-2".to_string(),
            maker_address: "0x1111".to_string(),
            taker_address: "0x2222".to_string(),
            side: TradeSide::Buy,
            size: 100.0,
            price: 0.0,
            token_id: "token".to_string(),
            timestamp: Utc::now(),
        };
        assert!(trade_fill_to_source_event("market", &fill_zero_price, 1000.0).is_none());

        // Negative values should return None
        let fill_negative = TradeFill {
            trade_id: "trade-3".to_string(),
            maker_address: "0x1111".to_string(),
            taker_address: "0x2222".to_string(),
            side: TradeSide::Buy,
            size: -10.0,
            price: 0.5,
            token_id: "token".to_string(),
            timestamp: Utc::now(),
        };
        assert!(trade_fill_to_source_event("market", &fill_negative, 1000.0).is_none());
    }

    #[test]
    fn ws_message_serialization_ping() {
        let msg = WsMessage::Ping;
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"ping\""));

        // Verify roundtrip
        let parsed: WsMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, WsMessage::Ping));
    }

    #[test]
    fn ws_message_serialization_subscribe() {
        let msg = WsMessage::Subscribe {
            channels: vec!["trade:btc-up".to_string(), "user:0x1234".to_string()],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"subscribe\""));
        assert!(json.contains("trade:btc-up"));
        assert!(json.contains("user:0x1234"));

        // Verify roundtrip
        let parsed: WsMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            WsMessage::Subscribe { channels } => {
                assert_eq!(channels.len(), 2);
                assert!(channels.contains(&"trade:btc-up".to_string()));
            }
            _ => panic!("Expected Subscribe variant"),
        }
    }

    #[test]
    fn ws_message_serialization_trade() {
        let trade = TradeFill {
            trade_id: "t1".to_string(),
            maker_address: "0xaaa".to_string(),
            taker_address: "0xbbb".to_string(),
            side: TradeSide::Sell,
            size: 50.0,
            price: 0.75,
            token_id: "token-xyz".to_string(),
            timestamp: Utc::now(),
        };

        let msg = WsMessage::Trade {
            market_slug: "market-abc".to_string(),
            transaction_hash: "tx-123".to_string(),
            timestamp: Utc::now(),
            trades: vec![trade],
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"trade\""));
        assert!(json.contains("market-abc"));
        assert!(json.contains("tx-123"));
    }

    #[test]
    fn ws_client_state_default() {
        let state = WsClientState::new();
        assert!(!state.connected);
        assert!(state.subscribed_channels.is_empty());
        assert!(state.last_message_at.is_none());
        assert_eq!(state.consecutive_failures, 0);
        assert!(state.client_id.is_none());
    }

    #[test]
    fn ws_config_default() {
        let config = WsConfig::default();
        assert_eq!(config.url, "wss://clob.polymarket.com/ws");
        assert_eq!(config.reconnect_base_ms, 250);
        assert_eq!(config.reconnect_max_ms, 30_000);
        assert_eq!(config.ping_interval_secs, 30);
        assert_eq!(config.connection_timeout_secs, 10);
        assert_eq!(config.max_consecutive_failures, 10);
    }
}
