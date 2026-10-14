//! Shared application state handed to every Axum handler.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crossbeam_channel::Sender;
use sqlx::PgPool;

use crate::engine_thread::{EngineCommand, ServerEvent};
use crate::metrics::Metrics;

/// Shared slot holding the DB pool once the persistence writer connects.
/// Stays `None` until then (or forever when persistence is disabled), so the
/// server can start before TimescaleDB is ready.
pub type Persistence = Arc<tokio::sync::RwLock<Option<PgPool>>>;

/// Cheap-to-clone handle bundle. Cloning shares the same channels/pool.
#[derive(Clone)]
pub struct AppState {
    /// Input channel to the single engine thread.
    pub cmd_tx: Sender<EngineCommand>,
    /// Fan-out of serialized engine events to WebSocket subscribers.
    pub broadcast: tokio::sync::broadcast::Sender<Arc<ServerEvent>>,
    /// Lock-free counters exposed via `/health`.
    pub metrics: Arc<Metrics>,
    /// Live feed rate (orders/second), adjustable at runtime via
    /// `POST /feed-rate`.
    pub feed_rate: Arc<AtomicU64>,
    /// Levels returned in a bootstrap `snapshot`.
    pub snapshot_depth: usize,
    /// Connection pool slot. `None` when persistence is disabled or the DB
    /// is not reachable yet, in which case `/candles` returns `503`.
    pub persistence: Persistence,
}
