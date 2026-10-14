//! # engine — pure limit order book core
//!
#![forbid(unsafe_code)]
//! A deterministic, single-writer matching engine library with **no I/O**:
//!
//! * no async runtime, no Tokio
//! * no sockets, no HTTP, no WebSockets
//! * no SQL, no file system
//! * no `Mutex` / `RwLock` — the book is owned by exactly one thread
//!
//! This crate is independently unit-tested, property-tested and benchmarked.
//! Networking, persistence and the synthetic feed live in sibling crates and
//! communicate with the engine through message channels.
//!
//! ## Modules
//!
//! * [`types`] — integer price/quantity types, `Order`, `Fill`, `Event`
//! * [`price_level::PriceLevel`] — FIFO queue of resting orders at one price
//! * [`book::OrderBook`] — price priority, index, sequencing, deltas
//!
//! ## Core rules
//!
//! 1. Integer prices and quantities only — no floating point matching.
//! 2. Fills execute at the **resting (maker) order's price**.
//! 3. Within a price level, orders trade strictly FIFO (time priority).
//! 4. The `OrderBook` assigns every sequence number; nothing else may.

pub mod book;
pub mod price_level;
pub mod types;

pub use book::{OrderBook, Snapshot};
pub use price_level::{MatchResult, PriceLevel};
pub use types::{Event, Fill, Order, OrderId, Price, RejectReason, Side};
