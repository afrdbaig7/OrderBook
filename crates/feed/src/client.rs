//! Reference client-side book reconstruction.
//!
//! Mirrors exactly what a browser must do with the `/ws` stream: start from
//! a `snapshot`, then apply `delta`s in strict `seq` order. `trade`, `ack`,
//! `cancel` and `reject` do not change the book but advance `seq`, so they
//! matter for gap detection.
//!
//! This type is the executable specification of the client protocol and is
//! reused by the Phase D tests; the JavaScript client will implement the
//! same rules.

use std::collections::BTreeMap;

use engine::{Price, Side};

use crate::wire::Message;

/// A price-indexed book image built purely from wire messages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientBook {
    bids: BTreeMap<Price, u64>,
    asks: BTreeMap<Price, u64>,
    last_seq: u64,
    synced: bool,
    desynced: bool,
}

impl ClientBook {
    /// A fresh, unsynced client. Feed it a `snapshot` first.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one wire message according to the client protocol rules:
    ///
    /// * a `snapshot` replaces all state and resyncs the client;
    /// * before the first snapshot, everything is ignored;
    /// * `seq <= last_seq` is stale (pre-snapshot buffer) → ignored;
    /// * `seq == last_seq + 1` → applied;
    /// * `seq > last_seq + 1` → a message was dropped → desynced (the
    ///   caller must reconnect and take a fresh snapshot).
    pub fn apply(&mut self, message: &Message) {
        if let Message::Snapshot(snapshot) = message {
            self.bids = snapshot.bids.iter().copied().collect();
            self.asks = snapshot.asks.iter().copied().collect();
            self.last_seq = snapshot.seq;
            self.synced = true;
            self.desynced = false;
            return;
        }

        if !self.synced || self.desynced {
            return;
        }

        let seq = message.seq();
        if seq <= self.last_seq {
            return;
        }
        if seq != self.last_seq + 1 {
            self.desynced = true;
            return;
        }

        if let Message::Delta {
            side, price, qty, ..
        } = message
        {
            let levels = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if *qty == 0 {
                levels.remove(price);
            } else {
                levels.insert(*price, *qty);
            }
        }

        self.last_seq = seq;
    }

    /// Decodes a JSON message and applies it.
    pub fn apply_json(&mut self, json: &str) -> Result<(), serde_json::Error> {
        let message = Message::decode(json)?;
        self.apply(&message);
        Ok(())
    }

    /// Applies a batch in order.
    pub fn apply_all(&mut self, messages: &[Message]) {
        for message in messages {
            self.apply(message);
        }
    }

    /// Resting bids, price → total qty.
    pub fn bids(&self) -> &BTreeMap<Price, u64> {
        &self.bids
    }

    /// Resting asks, price → total qty.
    pub fn asks(&self) -> &BTreeMap<Price, u64> {
        &self.asks
    }

    /// Highest applied sequence number.
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// `true` once a snapshot has been applied.
    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// `true` if a sequence gap was detected and a resync is required.
    pub fn is_desynced(&self) -> bool {
        self.desynced
    }
}
