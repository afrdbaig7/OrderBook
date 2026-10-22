//! Core types for the matching engine. All prices and quantities are integers.

use serde::{Deserialize, Serialize};

/// Price in integer ticks. A price of `0` is rejected by the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Price(pub u64);

/// Order identifier. Must be unique among resting orders; duplicates are rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OrderId(pub u64);

/// Bid or ask.
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
    #[inline]
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

/// A limit order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub price: Price,
    pub qty: u64,
}

/// One fill between a maker and taker. Price is always the maker's resting price.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub maker_id: OrderId,
    pub taker_id: OrderId,
    pub price: Price,
    pub qty: u64,
}

/// Largest safe integer for JavaScript `Number` (2^53 - 1).
/// Also fits in PostgreSQL `BIGINT`.
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Largest accepted price in ticks.
pub const MAX_PRICE: u64 = MAX_SAFE_INTEGER;

/// Largest accepted quantity per order.
pub const MAX_ORDER_QTY: u64 = MAX_SAFE_INTEGER;

/// Largest aggregate quantity per price level.
pub const MAX_LEVEL_QTY: u64 = MAX_SAFE_INTEGER;

/// Largest order identifier that clients can represent exactly.
pub const MAX_ORDER_ID: u64 = MAX_SAFE_INTEGER;

/// Maximum distinct price levels allowed per side of the book.
/// Enforces bounded memory for complete L2 snapshot reconstructions.
pub const MAX_BOOK_LEVELS_PER_SIDE: usize = 10_000;

/// Why a request was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    #[error("zero quantity")]
    ZeroQuantity,
    #[error("zero price")]
    ZeroPrice,
    #[error("quantity exceeds maximum safe limit")]
    QuantityOutOfRange,
    #[error("price exceeds maximum safe limit")]
    PriceOutOfRange,
    #[error("order id exceeds maximum safe limit")]
    OrderIdOutOfRange,
    #[error("maximum price levels per side exceeded")]
    BookDepthExceeded,
    #[error("level quantity exceeds maximum safe limit")]
    LevelQuantityOverflow,
    #[error("duplicate order id")]
    DuplicateOrderId,
    #[error("unknown cancel")]
    UnknownCancel,
    /// Market order's remainder dropped after liquidity ran out.
    #[error("market order unfilled")]
    MarketUnfilled,
}

/// Engine events. Every event carries a monotonically increasing `seq`.
/// Clients use it to detect dropped messages. The server translates these
/// to wire messages — no serde on internal events keeps the wire format
/// out of this crate.
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
