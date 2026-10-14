//! The dedicated engine thread: the **only** place an `OrderBook` is mutated.
//!
//! # Why a dedicated thread (spec §5, §24)
//!
//! The book is owned by exactly one OS thread and reached through a bounded
//! `crossbeam-channel`. That gives three properties a shared book cannot:
//!
//! * **No locks on the hot path** — the borrow checker, not a `Mutex`,
//!   guarantees single ownership.
//! * **Determinism** — one total order of commands ⇒ one canonical event
//!   order. Snapshots and deltas therefore line up by construction.
//! * **Isolation** — the thread knows nothing about Tokio, WebSockets, SQL
//!   or HTTP. It receives `Command`s and emits serialized messages.
//!
//! # Snapshot consistency
//!
//! Snapshot requests travel on the *same* channel as feed commands, so they
//! are ordered with respect to every mutation. A snapshot taken between two
//! commands carries `seq = book.seq()`, and the very next broadcast message
//! has `seq + 1`. A client only has to drop buffered messages with
//! `seq <= snapshot.seq` (see crate `feed` client rules).

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender};
use engine::{Event, OrderBook, Side};
use feed::Command;
use tokio::sync::oneshot;
use tracing::warn;

use crate::metrics::Metrics;

/// A fill destined for the `trades` table. Deliberately id-free to match
/// the schema (spec §34): persistence stores prices, not order identities.
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
    /// Stop the thread (graceful shutdown).
    Shutdown,
}

/// Current wall-clock time in epoch milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Spawns the engine thread and returns its join handle.
pub fn spawn(
    mut book: OrderBook,
    cmd_rx: Receiver<EngineCommand>,
    broadcast: tokio::sync::broadcast::Sender<Arc<ServerEvent>>,
    db_tx: Option<Sender<TradeRow>>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("engine".to_string())
        .spawn(move || {
            while let Ok(command) = cmd_rx.recv() {
                match command {
                    EngineCommand::Feed(command) => {
                        metrics.add_orders_in(1);
                        for event in command.apply(&mut book) {
                            let ts = now_ms();
                            let message = feed::Message::from_event(&event).with_timestamp(ts);
                            let seq = message.seq();
                            let json: Arc<str> = Arc::from(message.encode());

                            let trade = match &event {
                                Event::Fill {
                                    fill, taker_side, ..
                                } => Some(TradeRow {
                                    ts_ms: ts as i64,
                                    seq: seq as i64,
                                    price: fill.price.0 as i64,
                                    qty: fill.qty as i64,
                                    taker_side: match taker_side {
                                        Side::Bid => 0,
                                        Side::Ask => 1,
                                    },
                                }),
                                _ => None,
                            };

                            if let Some(row) = &trade {
                                metrics.inc_trades();
                                if let Some(tx) = &db_tx {
                                    // Bounded, non-blocking: if the writer is
                                    // behind, the trade is dropped and counted
                                    // (spec §36). Matching never waits on the DB.
                                    if tx.try_send(row.clone()).is_err() {
                                        metrics.add_persistence_drops(1);
                                    }
                                }
                            }

                            metrics.inc_events_out();
                            metrics.set_last_seq(seq);
                            // `broadcast::Sender::send` never blocks: it
                            // overwrites the oldest buffered message for any
                            // lagging receiver (which we then disconnect).
                            let _ = broadcast.send(Arc::new(ServerEvent { seq, json }));
                        }
                    }
                    EngineCommand::Snapshot { depth, respond } => {
                        let snapshot = book.snapshot(depth);
                        let json: Arc<str> =
                            Arc::from(feed::Message::from_snapshot(&snapshot).encode());
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
                    EngineCommand::Shutdown => break,
                }
            }
            // Dropping `db_tx` here closes the persistence channel, which
            // tells the writer task to flush and exit.
        })
        .expect("failed to spawn engine thread")
}
