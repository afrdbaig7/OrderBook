//! The dedicated engine thread — the only place the order book is mutated.
//!
//! The book is owned exclusively by a single OS thread reached via a bounded
//! `crossbeam-channel`. No locks on the matching hot path; the borrow checker
//! guarantees exclusive ownership. Snapshot requests travel on the same
//! channel as feed commands so they're consistent by construction.
//!
//! # Performance Architecture
//! 1. **Zero-Allocation Matching Core**: The engine thread executes matching logic
//!    and pushes fixed-size binary `Event`s directly into a preallocated SPSC bounded
//!    ring buffer without JSON serialization, timestamping system calls, or Arc allocations.
//! 2. **Dedicated Broadcaster Thread**: Offloads JSON formatting (`serde_json`),
//!    wall-clock timestamping (`now_ms`), WebSocket serialization, metrics aggregation,
//!    and database trade dispatching to an independent thread.
//! 3. **Adaptive Busy-Spinning**: Employs hardware `spin_loop` (PAUSE instructions)
//!    for microsecond burst responsiveness without futex sleep/wake penalties, while
//!    falling back to timed park during prolonged idle periods.
//! 4. **Dedicated Core Pinning**: Optional thread affinity pinning on Windows
//!    (`SetThreadAffinityMask`) and Linux (`sched_setaffinity`) to eliminate OS core migrations.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver, Sender};
use engine::{Event, FlatBook, OrderBook, Side, Snapshot};
use feed::Command;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::affinity::pin_thread_to_core;
use crate::metrics::Metrics;

/// Configuration for engine and broadcaster thread execution.
#[derive(Debug, Clone, Copy)]
pub struct EngineThreadConfig {
    /// Dedicated logical CPU core to pin the matching engine thread to.
    pub pin_core: Option<usize>,
    /// If true, use 100% busy-spin polling loop without sleep backoff.
    pub busy_spin: bool,
    /// Bounded capacity of the SPSC ring buffer connecting matching to broadcasting.
    pub outbound_capacity: usize,
}

impl Default for EngineThreadConfig {
    fn default() -> Self {
        Self {
            pin_core: None,
            busy_spin: false,
            outbound_capacity: 131_072,
        }
    }
}

/// Identifies the matching engine implementation.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    clap::ValueEnum,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum EngineType {
    /// Standard dynamic BTreeMap + VecDeque reference engine.
    #[default]
    Reference,
    /// Ultra-low latency, zero-allocation preallocated arena + bitmap engine.
    Flat,
}

impl EngineType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Reference => "reference",
            Self::Flat => "flat",
        }
    }
}

impl std::fmt::Display for EngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Dynamic or flat matching engine core managed exclusively by the engine thread.
#[allow(clippy::large_enum_variant)]
pub enum EngineKind {
    Reference(OrderBook),
    Flat(FlatBook),
}

impl From<OrderBook> for EngineKind {
    fn from(book: OrderBook) -> Self {
        Self::Reference(book)
    }
}

impl From<FlatBook> for EngineKind {
    fn from(book: FlatBook) -> Self {
        Self::Flat(book)
    }
}

impl EngineKind {
    pub fn engine_type(&self) -> EngineType {
        match self {
            Self::Reference(_) => EngineType::Reference,
            Self::Flat(_) => EngineType::Flat,
        }
    }

    pub fn snapshot(&self, depth: usize) -> Snapshot {
        match self {
            Self::Reference(book) => book.snapshot(depth),
            Self::Flat(book) => book.snapshot(),
        }
    }
}

/// A fill destined for the `trades` table.
/// Schema stores prices, not order ids, so there's no `id` field here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeRow {
    pub ts_ms: i64,
    pub seq: i64,
    pub price: i64,
    pub qty: i64,
    /// `0` = bid, `1` = ask (matches `SMALLINT` in the schema).
    pub taker_side: i16,
}

/// A message broadcast to every WebSocket subscriber.
#[derive(Debug)]
pub struct ServerEvent {
    /// Engine sequence number (used for gap filtering and ordering).
    pub seq: u64,
    /// Pre-serialized JSON — encoded once, shared by all clients.
    pub json: Arc<str>,
}

/// Response to a snapshot request.
#[derive(Debug)]
pub struct SnapshotReply {
    pub seq: u64,
    pub json: Arc<str>,
}

/// Everything the engine thread accepts.
pub enum EngineCommand {
    /// Submit a synthetic-feed command.
    Feed(Command),
    /// Ask the engine to serialize a consistent point-in-time snapshot.
    Snapshot {
        depth: usize,
        respond: oneshot::Sender<SnapshotReply>,
    },
    /// Stop the thread cleanly.
    Shutdown,
}

/// Internal message passed from the matching engine thread to the broadcaster thread.
pub enum OutboundMessage {
    /// Fixed-size binary matching event.
    Event(Event),
    /// Point-in-time snapshot reply.
    Snapshot {
        snapshot: Snapshot,
        respond: oneshot::Sender<SnapshotReply>,
    },
}

/// Current wall-clock time in epoch milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Broadcaster thread execution loop.
fn run_broadcaster(
    outbound_rx: Receiver<OutboundMessage>,
    broadcast: tokio::sync::broadcast::Sender<Arc<ServerEvent>>,
    db_tx: Option<Sender<TradeRow>>,
    metrics: Arc<Metrics>,
) {
    let handle_outbound = |msg: OutboundMessage| {
        match msg {
            OutboundMessage::Event(event) => {
                let ts = now_ms();
                let seq = event.seq();

                let trade = match &event {
                    Event::Fill {
                        fill, taker_side, ..
                    } => {
                        let ts_ms = i64::try_from(ts).unwrap_or(i64::MAX);
                        let seq_i64 = i64::try_from(seq).unwrap_or(i64::MAX);
                        let price_i64 = i64::try_from(fill.price.0).unwrap_or(i64::MAX);
                        let qty_i64 = i64::try_from(fill.qty).unwrap_or(i64::MAX);
                        Some(TradeRow {
                            ts_ms,
                            seq: seq_i64,
                            price: price_i64,
                            qty: qty_i64,
                            taker_side: match taker_side {
                                Side::Bid => 0,
                                Side::Ask => 1,
                            },
                        })
                    }
                    _ => None,
                };

                if let Some(row) = trade {
                    metrics.inc_trades();
                    if let Some(tx) = &db_tx {
                        // Non-blocking try_send: if the writer task falls behind,
                        // the trade is dropped and counted. Matching never waits on DB.
                        if tx.try_send(row).is_err() {
                            metrics.add_persistence_drops(1);
                        }
                    }
                }

                metrics.inc_events_out();
                metrics.set_last_seq(seq);

                // Only perform JSON encoding and allocation if active WebSocket subscribers exist.
                if broadcast.receiver_count() > 0 {
                    let message = feed::Message::from_event(&event).with_timestamp(ts);
                    let json: Arc<str> = Arc::from(message.encode());
                    let _ = broadcast.send(Arc::new(ServerEvent { seq, json }));
                }
            }
            OutboundMessage::Snapshot { snapshot, respond } => {
                let json: Arc<str> = Arc::from(feed::Message::from_snapshot(&snapshot).encode());
                if respond
                    .send(SnapshotReply {
                        seq: snapshot.seq,
                        json,
                    })
                    .is_err()
                {
                    warn!("snapshot requester went away before reply");
                }
            }
        }
    };

    const BROADCASTER_SPIN_LIMIT: u32 = 10_000;
    let mut spins = 0u32;

    loop {
        match outbound_rx.try_recv() {
            Ok(msg) => {
                spins = 0;
                handle_outbound(msg);
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {
                if spins < BROADCASTER_SPIN_LIMIT {
                    std::hint::spin_loop();
                    spins = spins.saturating_add(1);
                } else {
                    match outbound_rx.recv_timeout(Duration::from_millis(1)) {
                        Ok(msg) => {
                            spins = 0;
                            handle_outbound(msg);
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => break,
        }
    }

    // Drain remaining messages before dropping db_tx
    while let Ok(msg) = outbound_rx.try_recv() {
        handle_outbound(msg);
    }
}

/// Spawns the engine thread with default configuration.
pub fn spawn(
    engine: impl Into<EngineKind>,
    cmd_rx: Receiver<EngineCommand>,
    broadcast: tokio::sync::broadcast::Sender<Arc<ServerEvent>>,
    db_tx: Option<Sender<TradeRow>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    spawn_with_config(
        engine,
        cmd_rx,
        broadcast,
        db_tx,
        metrics,
        EngineThreadConfig::default(),
    )
}

/// Spawns the engine thread with customized thread affinity and polling behavior.
pub fn spawn_with_config(
    engine: impl Into<EngineKind>,
    cmd_rx: Receiver<EngineCommand>,
    broadcast: tokio::sync::broadcast::Sender<Arc<ServerEvent>>,
    db_tx: Option<Sender<TradeRow>>,
    metrics: Arc<Metrics>,
    config: EngineThreadConfig,
) -> JoinHandle<()> {
    let mut engine = engine.into();
    std::thread::Builder::new()
        .name("engine".to_string())
        .spawn(move || {
            if let Some(core_id) = config.pin_core {
                if pin_thread_to_core(core_id) {
                    info!("Matching engine thread pinned to core {core_id}");
                } else {
                    warn!("Failed to pin matching engine thread to core {core_id}");
                }
            }

            let (outbound_tx, outbound_rx) = bounded::<OutboundMessage>(config.outbound_capacity);

            let broadcaster_metrics = metrics.clone();
            let broadcaster_handle = std::thread::Builder::new()
                .name("broadcaster".to_string())
                .spawn(move || {
                    run_broadcaster(outbound_rx, broadcast, db_tx, broadcaster_metrics);
                })
                .expect("failed to spawn broadcaster thread");

            let mut flat_events = match &engine {
                EngineKind::Flat(flat) => Vec::with_capacity(flat.worst_case_event_capacity()),
                EngineKind::Reference(_) => Vec::new(),
            };
            let mut ref_events = Vec::new();

            let mut handle_command = |command: EngineCommand| -> bool {
                match command {
                    EngineCommand::Feed(command) => {
                        metrics.add_orders_in(1);
                        match &mut engine {
                            EngineKind::Reference(book) => {
                                ref_events = command.apply(book);
                                for ev in ref_events.drain(..) {
                                    if outbound_tx.send(OutboundMessage::Event(ev)).is_err() {
                                        return false;
                                    }
                                }
                            }
                            EngineKind::Flat(flat) => {
                                flat_events.clear();
                                let res = match &command {
                                    Command::Limit(order) => {
                                        flat.submit_limit_into(order.clone(), &mut flat_events)
                                    }
                                    Command::Market { id, side, qty } => {
                                        flat.submit_market_into(*id, *side, *qty, &mut flat_events)
                                    }
                                    Command::Cancel(id) => flat.cancel_into(*id, &mut flat_events),
                                    Command::Modify {
                                        id,
                                        new_qty,
                                        new_price,
                                    } => flat.modify_into(
                                        *id,
                                        *new_qty,
                                        *new_price,
                                        &mut flat_events,
                                    ),
                                };
                                if let Err(err) = res {
                                    warn!("FlatBook command error: {err:?}");
                                }
                                for ev in flat_events.drain(..) {
                                    if outbound_tx.send(OutboundMessage::Event(ev)).is_err() {
                                        return false;
                                    }
                                }
                            }
                        }
                        true
                    }
                    EngineCommand::Snapshot { depth, respond } => {
                        let snapshot = engine.snapshot(depth);
                        let _ = outbound_tx.send(OutboundMessage::Snapshot { snapshot, respond });
                        true
                    }
                    EngineCommand::Shutdown => false,
                }
            };

            const ENGINE_SPIN_LIMIT: u32 = 10_000;
            let mut spins = 0u32;

            loop {
                match cmd_rx.try_recv() {
                    Ok(command) => {
                        spins = 0;
                        if !handle_command(command) {
                            break;
                        }
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        if config.busy_spin || spins < ENGINE_SPIN_LIMIT {
                            std::hint::spin_loop();
                            spins = spins.saturating_add(1);
                        } else {
                            match cmd_rx.recv_timeout(Duration::from_millis(1)) {
                                Ok(command) => {
                                    spins = 0;
                                    if !handle_command(command) {
                                        break;
                                    }
                                }
                                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                            }
                        }
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                }
            }

            // Drain any remaining queued commands before closing outbound channel
            while let Ok(command) = cmd_rx.try_recv() {
                if !handle_command(command) {
                    break;
                }
            }

            // Dropping outbound_tx tells the broadcaster thread to flush and exit
            drop(outbound_tx);

            if let Err(err) = broadcaster_handle.join() {
                tracing::error!("broadcaster thread panicked: {err:?}");
            }
        })
        .expect("failed to spawn engine thread")
}
