//! Reference client-side book reconstruction.
//!
//! Mirrors what a browser must do: bootstrap from a `snapshot`, then apply
//! `delta`s in strict `seq` order. Non-book messages (`trade`, `ack`, etc.)
//! still advance `seq` for gap detection purposes.

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

    pub fn bids(&self) -> &BTreeMap<Price, u64> {
        &self.bids
    }

    pub fn asks(&self) -> &BTreeMap<Price, u64> {
        &self.asks
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub fn is_synced(&self) -> bool {
        self.synced
    }

    pub fn is_desynced(&self) -> bool {
        self.desynced
    }
}
