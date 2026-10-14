//! Protocol correctness tests — snapshot + delta reconstruction.
//!
//! Simulates the full client pipeline described in the spec:
//!
//! ```text
//! engine state → snapshot + delta events → client-side reconstructed book
//! ```
//!
//! After every N operations the reconstructed client book is compared
//! against the engine's real book. They must be identical at all times.
//! Reconnect/resync behaviour is exercised as well.

use std::collections::BTreeMap;

use engine::{Event, Order, OrderBook, OrderId, Price, RejectReason, Side, Snapshot};

/// Client-side book reconstruction: `side → price → total_qty`.
///
/// Mirrors exactly what the WebSocket protocol lets a browser rebuild —
/// only snapshots and deltas; fills/acks/cancels/rejects only advance `seq`.
#[derive(Debug, Default)]
struct ClientBook {
    bids: BTreeMap<Price, u64>,
    asks: BTreeMap<Price, u64>,
    last_seq: u64,
    synced: bool,
    desynced: bool,
}

impl ClientBook {
    /// Rule: replace local state with the snapshot; buffered messages with
    /// `seq <= snapshot.seq` are ignored afterwards.
    fn apply_snapshot(&mut self, snap: &Snapshot) {
        self.bids = snap.bids.iter().copied().collect();
        self.asks = snap.asks.iter().copied().collect();
        self.last_seq = snap.seq;
        self.synced = true;
        self.desynced = false;
    }

    /// Rule: while unsynced, buffer/ignore everything.
    /// Rule: `seq <= last_seq` is stale (pre-snapshot) → ignore.
    /// Rule: `seq == last_seq + 1` → apply.
    /// Rule: `seq > last_seq + 1` → gap → state corrupt, stop applying.
    fn feed(&mut self, event: &Event) {
        if !self.synced || self.desynced {
            return;
        }
        let seq = event.seq();
        if seq <= self.last_seq {
            return; // stale / buffered pre-snapshot message
        }
        if seq != self.last_seq + 1 {
            self.desynced = true; // dropped message — resync required
            return;
        }
        if let Event::Delta {
            side,
            price,
            new_total_qty,
            ..
        } = event
        {
            let levels = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if *new_total_qty == 0 {
                levels.remove(price); // qty 0 = delete level
            } else {
                levels.insert(*price, *new_total_qty);
            }
        }
        self.last_seq = seq;
    }

    fn feed_all(&mut self, events: &[Event]) {
        for ev in events {
            self.feed(ev);
        }
    }
}

/// The core protocol property: reconstructed state == engine state.
fn assert_matches_engine(client: &ClientBook, engine: &OrderBook, ctx: &str) {
    assert!(!client.desynced, "{ctx}: client desynced");

    let expect_side = |engine_levels: &std::collections::BTreeMap<Price, engine::PriceLevel>,
                       client_levels: &BTreeMap<Price, u64>,
                       name: &str| {
        assert_eq!(
            engine_levels.len(),
            client_levels.len(),
            "{ctx}: {name} level count mismatch"
        );
        for (price, level) in engine_levels {
            assert_eq!(
                client_levels.get(price),
                Some(&level.total_qty()),
                "{ctx}: {name} qty mismatch at {price:?}"
            );
        }
    };

    expect_side(engine.bid_levels(), &client.bids, "bids");
    expect_side(engine.ask_levels(), &client.asks, "asks");
    assert_eq!(client.last_seq, engine.seq(), "{ctx}: seq mismatch");
}

/// Deterministic PRNG (xorshift64) so reconstruction tests are reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn random_command(rng: &mut Rng, engine: &mut OrderBook) -> Vec<Event> {
    let roll = rng.next() % 10;
    let id = rng.next() % 64;
    let side = if rng.next().is_multiple_of(2) {
        Side::Bid
    } else {
        Side::Ask
    };
    let price = 96 + rng.next() % 9; // 96..=104
    let qty = 1 + rng.next() % 20;

    if roll < 6 {
        engine.submit_limit(Order {
            id: OrderId(id),
            side,
            price: Price(price),
            qty,
        })
    } else if roll < 9 {
        engine.cancel(OrderId(id))
    } else {
        engine.submit_market(OrderId(id), side, qty)
    }
}

/// Property: snapshot (seq 0) + every subsequent delta reconstructs the
/// engine's book exactly, checked after every single operation.
#[test]
fn snapshot_plus_live_deltas_reconstruct_engine() {
    let mut engine = OrderBook::new();
    let mut client = ClientBook::default();
    client.apply_snapshot(&engine.snapshot(1_000));

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for i in 0..2_000 {
        let events = random_command(&mut rng, &mut engine);
        client.feed_all(&events);
        assert_matches_engine(&client, &engine, &format!("op {i}"));
    }
}

/// Property: a client that disconnects and reconnects gets a fresh
/// snapshot; replayed (buffered) messages from before that snapshot are
/// ignored, later messages apply normally, and the state is correct.
#[test]
fn reconnect_resync_restores_correct_state() {
    let mut engine = OrderBook::new();
    let mut client = ClientBook::default();
    client.apply_snapshot(&engine.snapshot(1_000));

    let mut rng = Rng(0xDEAD_BEEF_CAFE_1234);
    let mut history: Vec<Vec<Event>> = Vec::new();

    // Live phase 1.
    for _ in 0..500 {
        let events = random_command(&mut rng, &mut engine);
        client.feed_all(&events);
        history.push(events);
    }
    assert_matches_engine(&client, &engine, "phase 1");

    // "Reconnect": request a fresh snapshot, then receive a stale replay
    // of an old delta (seq <= snapshot.seq) before live traffic resumes.
    let snapshot = engine.snapshot(1_000);
    client.apply_snapshot(&snapshot);

    if let Some(old) = history.first() {
        for ev in old {
            client.feed(ev); // must be ignored: seq <= snapshot.seq
        }
    }
    assert_matches_engine(&client, &engine, "after stale replay");

    // Live phase 2 continues from the new snapshot.
    for i in 0..500 {
        let events = random_command(&mut rng, &mut engine);
        client.feed_all(&events);
        assert_matches_engine(&client, &engine, &format!("phase 2 op {i}"));
    }
}

/// Property: skipping one message trips gap detection; resyncing from a
/// fresh snapshot recovers full correctness.
#[test]
fn sequence_gap_detected_then_resync_recovers() {
    let mut engine = OrderBook::new();
    let mut client = ClientBook::default();
    client.apply_snapshot(&engine.snapshot(1_000));

    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let mut skipped_gap = false;

    for _ in 0..1_000 {
        let events = random_command(&mut rng, &mut engine);

        if !skipped_gap && events.len() >= 2 {
            // Deliberately drop the first event to simulate packet loss.
            client.feed_all(&events[1..]);
            if client.desynced {
                skipped_gap = true;
                // Resync exactly like the spec: reconnect, fresh snapshot.
                let snap = engine.snapshot(1_000);
                client.apply_snapshot(&snap);
            }
        } else {
            client.feed_all(&events);
        }
    }

    assert!(skipped_gap, "test never produced a detectable gap");
    assert_matches_engine(&client, &engine, "after recovery");
}

/// Property: a rejected market's dropped-remainder reason never mutates
/// book state, so the client rebuild stays correct even on reject-heavy
/// workloads (unknown cancels, duplicate ids, zero quantities).
#[test]
fn reject_heavy_workload_reconstructs_correctly() {
    fn step(engine: &OrderBook, client: &mut ClientBook, events: Vec<Event>, i: usize) {
        client.feed_all(&events);
        assert_matches_engine(client, engine, &format!("reject-heavy op {i}"));
    }

    let mut engine = OrderBook::new();
    let mut client = ClientBook::default();
    client.apply_snapshot(&engine.snapshot(1_000));
    let mut rejects: Vec<RejectReason> = Vec::new();
    let track = |events: Vec<Event>, rejects: &mut Vec<RejectReason>| {
        rejects.extend(events.iter().filter_map(|e| match e {
            Event::Reject { reason, .. } => Some(*reason),
            _ => None,
        }));
        events
    };

    // Hand-picked sequence mixing accepts and rejects around one level.
    let events = track(
        engine.submit_limit(Order {
            id: OrderId(1),
            side: Side::Ask,
            price: Price(100),
            qty: 10,
        }),
        &mut rejects,
    );
    step(&engine, &mut client, events, 0);

    let events = track(
        engine.submit_limit(Order {
            id: OrderId(1),
            side: Side::Bid,
            price: Price(101),
            qty: 5,
        }), // duplicate id
        &mut rejects,
    );
    step(&engine, &mut client, events, 1);

    let events = track(engine.cancel(OrderId(77)), &mut rejects); // unknown
    step(&engine, &mut client, events, 2);

    let events = track(engine.submit_market(OrderId(2), Side::Bid, 4), &mut rejects);
    step(&engine, &mut client, events, 3);

    let events = track(
        engine.submit_market(OrderId(3), Side::Bid, 100),
        &mut rejects,
    ); // partial -> drop
    step(&engine, &mut client, events, 4);

    let events = track(
        engine.submit_limit(Order {
            id: OrderId(4),
            side: Side::Bid,
            price: Price(0), // zero price
            qty: 5,
        }),
        &mut rejects,
    );
    step(&engine, &mut client, events, 5);

    let events = track(
        engine.submit_limit(Order {
            id: OrderId(5),
            side: Side::Bid,
            price: Price(99),
            qty: 0, // zero qty
        }),
        &mut rejects,
    );
    step(&engine, &mut client, events, 6);

    let events = track(engine.cancel(OrderId(1)), &mut rejects); // front cancel of survivor
    step(&engine, &mut client, events, 7);

    // Sanity: rejects were actually produced and reconstruction held.
    assert!(rejects.len() >= 4, "expected rejects, got {rejects:?}");
    assert!(rejects.contains(&RejectReason::DuplicateOrderId));
    assert!(rejects.contains(&RejectReason::UnknownCancel));
    assert!(rejects.contains(&RejectReason::MarketUnfilled));
}
