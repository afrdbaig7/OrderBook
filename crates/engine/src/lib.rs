//! Pure limit order book and matching engine.
//!
//! No I/O, no async, no locks — the book is owned by exactly one thread.
//! Fills always execute at the maker's price. FIFO within each level.
#![forbid(unsafe_code)]

pub mod book;
pub mod price_level;
pub mod types;

pub use book::{OrderBook, Snapshot};
pub use price_level::{MatchResult, PriceLevel};
pub use types::{
    Event, Fill, Order, OrderId, Price, RejectReason, Side, MAX_LEVEL_QTY, MAX_ORDER_QTY,
    MAX_PRICE, MAX_SAFE_INTEGER,
};
