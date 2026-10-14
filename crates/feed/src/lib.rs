//! # feed — wire protocol and synthetic order flow
//!
#![forbid(unsafe_code)]
//! The translation layer between the pure `engine` crate and the outside
//! world. It contains **no sockets and no async runtime**: it defines the
//! JSON messages, converts engine events to them, generates deterministic
//! demo/test order flow, and provides a reference client-side
//! reconstruction.
//!
//! ## Modules
//!
//! * [`wire`] — the `/ws` message enum, `Event` → `Message` mapping, JSON
//!   encode/decode.
//! * [`feed`] — seeded [`feed::SyntheticFeed`] producing limit / market /
//!   cancel [`feed::Command`]s.
//! * [`client`] — [`client::ClientBook`], the executable specification of
//!   how a client rebuilds the book from `snapshot` + `delta`s.
//!
//! ## The one rule that ties everything together
//!
//! Every engine event consumes exactly one sequence number, and **every**
//! event — including rejects and trades — is forwarded to clients. A client
//! that only received deltas would see gaps and resync needlessly.

pub mod client;
pub mod feed;
pub mod wire;

pub use client::ClientBook;
pub use feed::{apply_to_wire, Command, FeedConfig, SyntheticFeed};
pub use wire::{messages_from_events, Message};
