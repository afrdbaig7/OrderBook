//! Core value types for the matching engine.
//!
//! All financial values are integers. Floating point is never used for
//! matching, so there is no rounding error in price or quantity comparisons.

use serde::{Deserialize, Serialize};

/// Price in integer ticks.
///
/// A price of `0` is invalid and rejected by the engine (validation lives in
/// `OrderBook`, Phase B).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Price(pub u64);

/// Monotonic order identifier assigned by the feed/producer.
///
/// Identifiers must be unique among orders submitted to a book; duplicates are
/// rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OrderId(pub u64);

/// Side of an order.
///
/// Serializes as `"bid"` / `"ask"` so the wire protocol needs no conversion.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    #[default]
    Bid,
    Ask,
}

impl Side {
    /// The opposite side, used to find the opposing book for matching.
    #[inline]
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

/// A limit order resting in (or crossing) the book.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub price: Price,
    pub qty: u64,
}

/// A single executed quantity transfer between a resting (maker) order and an
/// incoming (taker) order.
///
/// **`price` is always the maker's resting price**, never the taker's limit
/// price.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub maker_id: OrderId,
    pub taker_id: OrderId,
    pub price: Price,
    pub qty: u64,
}

/// Why a request was rejected.
///
/// Serializes lowercase to match the JSON wire protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    #[error("zero quantity")]
    ZeroQuantity,
    #[error("zero price")]
    ZeroPrice,
    #[error("duplicate order id")]
    DuplicateOrderId,
    #[error("unknown cancel")]
    UnknownCancel,
    /// A market order's remainder was dropped because liquidity ran out.
    /// The fills that *did* execute are emitted as `Fill` events before this.
    #[error("market order unfilled")]
    MarketUnfilled,
}

/// Externally visible engine events, in canonical order.
///
/// Every event carries a monotonically increasing `seq` assigned by the
/// `OrderBook` (Phase B). Clients use `seq` to detect dropped messages and to
/// order snapshot/delta reconstruction.
///
/// No serde derives here: the server layer explicitly maps these internal
/// events to wire messages (`/ws` protocol), keeping the wire format out of
/// the engine crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Ack {
        seq: u64,
        id: OrderId,
    },
    Fill {
        seq: u64,
        fill: Fill,
        taker_side: Side,
    },
    Cancel {
        seq: u64,
        id: OrderId,
    },
    Reject {
        seq: u64,
        id: OrderId,
        reason: RejectReason,
    },
    Delta {
        seq: u64,
        side: Side,
        price: Price,
        new_total_qty: u64,
    },
}

impl Event {
    /// The global sequence number carried by every externally visible event.
    #[inline]
    pub fn seq(&self) -> u64 {
        match self {
            Event::Ack { seq, .. }
            | Event::Fill { seq, .. }
            | Event::Cancel { seq, .. }
            | Event::Reject { seq, .. }
            | Event::Delta { seq, .. } => *seq,
        }
    }
}
