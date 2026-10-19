//! Wire protocol types, synthetic order flow, and reference client-side
//! book reconstruction. No sockets, no async — just message definitions
//! and the generator logic.
#![forbid(unsafe_code)]

pub mod client;
pub mod feed;
pub mod wire;

pub use client::ClientBook;
pub use feed::{apply_to_wire, Command, FeedConfig, SyntheticFeed};
pub use wire::{messages_from_events, Message};
