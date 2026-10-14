//! `server` library: wires the single-writer engine thread, the synthetic
//! feed, the WebSocket/HTTP surface and the batched persistence writer.
//!
//! ```text
//!  feed task (tokio)              engine thread (std)          broadcast (tokio)
//!  ─────────────────              ───────────────────          ─────────────────
//!  SyntheticFeed.next_command ──► EngineCommand::Feed ──► book.submit_* ──► ServerEvent ──► /ws
//!                       │                        │                              │
//!                       │                  Snapshot request ────────────────► snapshot reply
//!                       │                        │
//!                       └── order commands ──► trades ──try_send──► db writer (tokio) ──► TimescaleDB
//! ```
//!
//! Threading: the engine runs on its **own OS thread** (no Tokio, no locks on
//! the book). Everything else is Tokio. The only communication is bounded
//! channels, so a slow WebSocket or database client exerts backpressure
//! (drop + count) instead of stalling the matcher.

pub mod api;
pub mod db;
pub mod engine_thread;
pub mod metrics;
pub mod state;
pub mod ws;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use clap::Parser;
use crossbeam_channel::{bounded, Sender};
use engine::OrderBook;
use feed::{FeedConfig, SyntheticFeed};
use tokio::sync::{broadcast, watch};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::engine_thread::{EngineCommand, ServerEvent, TradeRow};
use crate::metrics::Metrics;
use crate::state::{AppState, Persistence};

/// Bound on queued engine commands. Large enough to absorb feed bursts,
/// small enough to surface sustained overload (`feed_drops`).
pub const ENGINE_CHANNEL_CAPACITY: usize = 100_000;
/// Bound on queued trade rows awaiting persistence.
pub const PERSIST_CAPACITY: usize = 100_000;
/// Per-subscriber buffer for live messages. Overflowing it decreases
/// `/health`-visible `clients` by closing lagging sockets.
pub const BROADCAST_CAPACITY: usize = 65_536;

/// Command-line configuration for the server.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "server",
    about = "Low-latency order book: engine thread + WebSocket market data + TimescaleDB"
)]
pub struct Args {
    /// HTTP/WebSocket listen port.
    #[arg(long, default_value_t = 8080)]
    pub port: u16,
    /// Target feed rate in orders/second.
    #[arg(long = "feed-rate", default_value_t = 1000)]
    pub feed_rate: u64,
    /// Seed for the deterministic synthetic feed.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Book levels returned in each bootstrap snapshot.
    #[arg(long = "snapshot-depth", default_value_t = 20)]
    pub snapshot_depth: usize,
    /// Postgres/TimescaleDB DSN. Persistence is disabled when unset.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,
}

/// Runs the server until Ctrl-C, then shuts down gracefully.
pub async fn run(args: Args) {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let metrics = Metrics::new();
    let feed_rate = Arc::new(AtomicU64::new(args.feed_rate));

    let (cmd_tx, cmd_rx) = bounded::<EngineCommand>(ENGINE_CHANNEL_CAPACITY);
    let (broadcast_tx, _) = broadcast::channel::<Arc<ServerEvent>>(BROADCAST_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let persistence: Persistence = Arc::new(tokio::sync::RwLock::new(None));

    let (db_tx, db_rx) = if args.database_url.is_some() {
        let (tx, rx) = bounded::<TradeRow>(PERSIST_CAPACITY);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Single-writer engine, on its own thread.
    let engine_handle = engine_thread::spawn(
        OrderBook::new(),
        cmd_rx,
        broadcast_tx.clone(),
        db_tx,
        metrics.clone(),
    );

    // Synthetic order flow (paced by this task, not by the feed crate).
    let feed = SyntheticFeed::new(FeedConfig {
        seed: args.seed,
        ..FeedConfig::default()
    });
    let feed_handle = tokio::spawn(feed_task(
        feed,
        cmd_tx.clone(),
        feed_rate.clone(),
        metrics.clone(),
        shutdown_rx.clone(),
    ));

    // Batched persistence writer (only when a DSN is configured).
    let db_handle = match (args.database_url.clone(), db_rx) {
        (Some(url), Some(rx)) => Some(tokio::spawn(db::run(
            url,
            rx,
            metrics.clone(),
            shutdown_rx.clone(),
            persistence.clone(),
        ))),
        _ => {
            info!("no DATABASE_URL: running without persistence");
            None
        }
    };

    let metrics_handle = tokio::spawn(metrics_task(
        metrics.clone(),
        cmd_tx.clone(),
        shutdown_rx.clone(),
    ));

    let state = AppState {
        cmd_tx: cmd_tx.clone(),
        broadcast: broadcast_tx.clone(),
        metrics: metrics.clone(),
        feed_rate: feed_rate.clone(),
        snapshot_depth: args.snapshot_depth,
        persistence: persistence.clone(),
    };

    let router = Router::new()
        .route("/health", get(api::health))
        .route("/candles", get(api::candles))
        .route(
            "/feed-rate",
            get(api::get_feed_rate).post(api::set_feed_rate),
        )
        .route("/ws", get(ws::handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", args.port))
        .await
        .expect("failed to bind listen port");
    info!(
        port = args.port,
        feed_rate = args.feed_rate,
        seed = args.seed,
        snapshot_depth = args.snapshot_depth,
        persistence = args.database_url.is_some(),
        "server listening"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");

    // ── Graceful shutdown ───────────────────────────────────────────────
    info!("shutting down");
    let _ = shutdown_tx.send(true); // stops feed + metrics + db tasks
    let _ = cmd_tx.send(EngineCommand::Shutdown);
    let _ = feed_handle.await;
    let _ = metrics_handle.await;
    let _ = engine_handle.join();
    if let Some(handle) = db_handle {
        let _ = handle.await;
    }
    info!("bye");
}

/// Drives the synthetic feed at the configured rate.
///
/// Pacing lives here (not in the `feed` crate) so the command *stream* stays
/// deterministic: a fixed seed yields the same commands regardless of rate.
/// A fractional accumulator lets rates below one command per tick (100 Hz)
/// still average out correctly.
async fn feed_task(
    mut feed: SyntheticFeed,
    cmd_tx: Sender<EngineCommand>,
    rate: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut debt: u64 = 0; // hundredths of a command

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let target = rate.load(Ordering::Relaxed);
                debt += target;
                let emit = debt / 100;
                debt -= emit * 100;
                for _ in 0..emit {
                    let command = EngineCommand::Feed(feed.next_command());
                    if cmd_tx.try_send(command).is_err() {
                        metrics.add_feed_drops(1);
                    }
                }
            }
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    info!("feed task stopped");
}

/// Logs a one-line metrics summary every second (spec §31).
async fn metrics_task(
    metrics: Arc<Metrics>,
    cmd_tx: Sender<EngineCommand>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut previous = metrics.snapshot();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = metrics.snapshot();
                info!(
                    orders_per_sec = now.orders_in - previous.orders_in,
                    events_per_sec = now.events_out - previous.events_out,
                    trades = now.trades,
                    last_seq = now.last_seq,
                    clients = now.clients,
                    feed_drops = now.feed_drops,
                    persistence_drops = now.persistence_drops,
                    persisted_rows = now.persisted_rows,
                    channel_depth = cmd_tx.len(),
                    "metrics"
                );
                previous = now;
            }
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

/// Resolves on Ctrl-C.
async fn shutdown_signal() {
    if tokio::signal::ctrl_c().await.is_ok() {
        info!("ctrl-c received");
    }
}
