//! Process-wide counters, updated lock-free from every task.
//!
//! All operations are `Relaxed`: these are observability numbers, not
//! synchronization. No counter is on the matching path in a way that can
//! block — a single atomic increment per command/event.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// Lock-free metrics shared through an `Arc`.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Commands accepted into the engine input channel.
    pub orders_in: AtomicU64,
    /// Wire messages produced by the engine.
    pub events_out: AtomicU64,
    /// Fill events (persisted trades).
    pub trades: AtomicU64,
    /// Feed commands dropped because the engine channel was full.
    pub feed_drops: AtomicU64,
    /// Trade rows dropped because the persistence channel was full or a
    /// batch failed to insert.
    pub persistence_drops: AtomicU64,
    /// Trade rows successfully written to TimescaleDB.
    pub persisted_rows: AtomicU64,
    /// Currently connected WebSocket clients.
    pub clients: AtomicI64,
    /// Highest sequence number emitted by the engine.
    pub last_seq: AtomicU64,
}

impl Metrics {
    /// Creates a shared metrics handle.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn add_orders_in(&self, n: u64) {
        self.orders_in.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_events_out(&self) {
        self.events_out.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_trades(&self) {
        self.trades.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_feed_drops(&self, n: u64) {
        self.feed_drops.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_persistence_drops(&self, n: u64) {
        self.persistence_drops.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_persisted(&self, n: u64) {
        self.persisted_rows.fetch_add(n, Ordering::Relaxed);
    }

    pub fn client_connected(&self) {
        self.clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_disconnected(&self) {
        self.clients.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records the latest emitted sequence number.
    pub fn set_last_seq(&self, seq: u64) {
        self.last_seq.store(seq, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            orders_in: self.orders_in.load(Ordering::Relaxed),
            events_out: self.events_out.load(Ordering::Relaxed),
            trades: self.trades.load(Ordering::Relaxed),
            feed_drops: self.feed_drops.load(Ordering::Relaxed),
            persistence_drops: self.persistence_drops.load(Ordering::Relaxed),
            persisted_rows: self.persisted_rows.load(Ordering::Relaxed),
            clients: self.clients.load(Ordering::Relaxed),
            last_seq: self.last_seq.load(Ordering::Relaxed),
        }
    }
}

/// A consistent-enough point-in-time copy of the counters.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct MetricsSnapshot {
    pub orders_in: u64,
    pub events_out: u64,
    pub trades: u64,
    pub feed_drops: u64,
    pub persistence_drops: u64,
    pub persisted_rows: u64,
    pub clients: i64,
    pub last_seq: u64,
}
