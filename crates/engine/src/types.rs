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

/// Maximum safe integer for exact representation across JavaScript (`Number.MAX_SAFE_INTEGER`),
/// database storage (`BIGINT`), and arithmetic operations: 2^53 - 1 (9,007,199,254,740,991).
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Maximum allowed price tick: capped at [`MAX_SAFE_INTEGER`].
pub const MAX_PRICE: u64 = MAX_SAFE_INTEGER;

/// Maximum allowed quantity for a single order: capped at [`MAX_SAFE_INTEGER`].
pub const MAX_ORDER_QTY: u64 = MAX_SAFE_INTEGER;

/// Maximum aggregate quantity allowed at a single price level: capped at [`MAX_SAFE_INTEGER`].
pub const MAX_LEVEL_QTY: u64 = MAX_SAFE_INTEGER;

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
