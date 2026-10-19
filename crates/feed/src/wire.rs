//! WebSocket wire protocol — the JSON messages a browser client sees.
//!
//! One `Message` per engine [`Event`], plus the `snapshot` bootstrap.
//! The engine never serializes anything itself; this is the one place
//! where internal events become JSON.
//!
//! `ack`, `cancel`, and `reject` don't mutate the client book but they
//! still carry a `seq` so clients can detect gaps without special-casing them.
//!
//! ```json
//! {"type":"snapshot","seq":1042,"bids":[[10000,50],[9999,120]],"asks":[[10001,30],[10002,75]]}
//! {"type":"delta","seq":1043,"side":"bid","price":10000,"qty":35}
//! {"type":"trade","seq":1044,"price":10001,"qty":10,"taker_side":"bid","ts":1730000000000}
//! {"type":"ack","seq":1045,"id":9}
//! {"type":"cancel","seq":1046,"id":7}
//! {"type":"reject","seq":1047,"id":3,"reason":"duplicate_order_id"}
//! ```

use engine::{Event, OrderId, Price, RejectReason, Side, Snapshot};
use serde::{Deserialize, Serialize};

/// A single message on the `/ws` stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Message {
    /// Full book image. Replaces all client state; resets the client's
    /// expected sequence to [`Snapshot::seq`].
    Snapshot(Snapshot),

    /// New total quantity at one price level on one side. `qty == 0`
    /// deletes the level.
    Delta {
        seq: u64,
        side: Side,
        price: Price,
        qty: u64,
    },

    /// One execution. `price` is always the maker's resting price.
    /// `ts` is the server-side epoch-millisecond stamp (`0` when produced
    /// directly from an engine event).
    Trade {
        seq: u64,
        price: Price,
        qty: u64,
        taker_side: Side,
        ts: u64,
    },

    /// An order rested in the book (possibly after partial fills).
    Ack { seq: u64, id: OrderId },

    /// A resting order was cancelled.
    Cancel { seq: u64, id: OrderId },

    /// A request was refused.
    Reject {
        seq: u64,
        id: OrderId,
        reason: RejectReason,
    },
}

impl Message {
    /// Maps one engine event to its wire message.
    /// Trades are produced with `ts = 0`; stamp real time with
    /// [`Message::with_timestamp`] before broadcasting.
    pub fn from_event(event: &Event) -> Self {
        match event {
            Event::Ack { seq, id } => Message::Ack { seq: *seq, id: *id },
            Event::Cancel { seq, id } => Message::Cancel { seq: *seq, id: *id },
            Event::Reject { seq, id, reason } => Message::Reject {
                seq: *seq,
                id: *id,
                reason: *reason,
            },
            Event::Delta {
                seq,
                side,
                price,
                new_total_qty,
            } => Message::Delta {
                seq: *seq,
                side: *side,
                price: *price,
                qty: *new_total_qty,
            },
            Event::Fill {
                seq,
                fill,
                taker_side,
            } => Message::Trade {
                seq: *seq,
                price: fill.price,
                qty: fill.qty,
                taker_side: *taker_side,
                ts: 0,
            },
        }
    }

    /// Wraps a book snapshot as the bootstrap `snapshot` wire message.
    pub fn from_snapshot(snapshot: &Snapshot) -> Self {
        Message::Snapshot(snapshot.clone())
    }

    /// Sets `ts` on a trade message; no-op for all other variants.
    pub fn with_timestamp(self, ts_ms: u64) -> Self {
        match self {
            Message::Trade {
                seq,
                price,
                qty,
                taker_side,
                ..
            } => Message::Trade {
                seq,
                price,
                qty,
                taker_side,
                ts: ts_ms,
            },
            other => other,
        }
    }

    /// The sequence number carried by this message (snapshots carry the
    /// sequence they were taken at).
    pub fn seq(&self) -> u64 {
        match self {
            Message::Snapshot(snap) => snap.seq,
            Message::Delta { seq, .. }
            | Message::Trade { seq, .. }
            | Message::Ack { seq, .. }
            | Message::Cancel { seq, .. }
            | Message::Reject { seq, .. } => *seq,
        }
    }

    pub fn is_trade(&self) -> bool {
        matches!(self, Message::Trade { .. })
    }

    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("Message is always serializable")
    }

    pub fn decode(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Maps a batch of engine events to wire messages, preserving order.
pub fn messages_from_events(events: &[Event]) -> Vec<Message> {
    events.iter().map(Message::from_event).collect()
}
