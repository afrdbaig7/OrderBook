//! End-to-end Phase D test: a synthetic feed drives the engine, every event
//! is serialized to JSON, and a client reconstructs the book **from the
//! serialized bytes**. The reconstruction must equal the engine state after
//! every single operation.

use std::collections::BTreeMap;

use engine::{OrderBook, Price, PriceLevel, Side};
use feed::{messages_from_events, ClientBook, Command, Message, SyntheticFeed};

/// Asserts the client's reconstructed book equals the engine's book.
fn assert_synced(client: &ClientBook, engine: &OrderBook, ctx: &str) {
    assert!(client.is_synced(), "{ctx}: client not synced");
    assert!(!client.is_desynced(), "{ctx}: client desynced");
    assert_eq!(client.last_seq(), engine.seq(), "{ctx}: seq mismatch");
    assert_side(engine.bid_levels(), client.bids(), "bids", ctx);
    assert_side(engine.ask_levels(), client.asks(), "asks", ctx);
}

fn assert_side(
    engine: &BTreeMap<Price, PriceLevel>,
    client: &BTreeMap<Price, u64>,
    name: &str,
    ctx: &str,
) {
    assert_eq!(
        engine.len(),
        client.len(),
        "{ctx}: {name} level count mismatch"
    );
    for (price, level) in engine {
        assert_eq!(
            client.get(price),
            Some(&level.total_qty()),
            "{ctx}: {name} qty mismatch at {price:?}"
        );
    }
}

/// Snapshot at seq 0, then replay the entire feed through JSON strings.
#[test]
fn json_pipeline_reconstructs_engine_after_every_op() {
    let mut engine = OrderBook::new();
    let mut client = ClientBook::new();

    let bootstrap = Message::from_snapshot(&engine.snapshot(1_000));
    client
        .apply_json(&bootstrap.encode())
        .expect("snapshot decodes");

    let mut feed = SyntheticFeed::seeded(0x00C0_FFEE);
    for i in 0..3_000 {
        let command = feed.next_command();
        let events = command.apply(&mut engine);
        for message in messages_from_events(&events) {
            let json = message.encode();
            client.apply_json(&json).expect("delta decodes");
        }
        assert_synced(&client, &engine, &format!("op {i}"));
    }

    assert!(engine.seq() > 0, "feed produced no events");
    assert!(client.is_synced());
}

/// A dropped JSON message creates a detectable gap; a fresh snapshot resyncs.
#[test]
fn dropped_message_desyncs_then_snapshot_resyncs() {
    let mut engine = OrderBook::new();
    let mut client = ClientBook::new();
    client
        .apply_json(&Message::from_snapshot(&engine.snapshot(1_000)).encode())
        .unwrap();

    let mut feed = SyntheticFeed::seeded(0xDEAD_BEEF);
    let mut desynced = false;

    for _ in 0..2_000 {
        let command = feed.next_command();
        let events = command.apply(&mut engine);
        let messages = messages_from_events(&events);

        if !desynced && messages.len() >= 2 {
            // Simulate packet loss: skip one message's JSON.
            for message in &messages[1..] {
                client.apply_json(&message.encode()).unwrap();
            }
            assert!(
                client.is_desynced(),
                "dropping a message must be detected as a gap"
            );
            desynced = true;
        } else {
            for message in &messages {
                client.apply_json(&message.encode()).unwrap();
            }
        }
    }

    assert!(desynced, "test never produced a multi-event op to drop");

    // Resync: reconnect and take a fresh snapshot — always recovers.
    client
        .apply_json(&Message::from_snapshot(&engine.snapshot(1_000)).encode())
        .unwrap();
    assert_synced(&client, &engine, "after resync");
}

/// Unsynced clients ignore traffic; stale (pre-snapshot) messages are ignored
/// even after syncing.
#[test]
fn lifecycle_ignores_uninitialized_and_stale_messages() {
    let mut client = ClientBook::new();
    let delta = Message::Delta {
        seq: 1,
        side: Side::Bid,
        price: Price(100),
        qty: 5,
    };

    client.apply(&delta);
    assert!(!client.is_synced(), "no snapshot yet, must stay unsynced");
    assert_eq!(client.last_seq(), 0);

    let mut engine = OrderBook::new();
    Command::Limit(engine_order(1, Side::Ask, 100, 10)).apply(&mut engine);
    let snapshot = Message::from_snapshot(&engine.snapshot(1_000));
    let snapshot_seq = snapshot.seq();
    client.apply(&snapshot);
    assert!(client.is_synced());

    // A buffered message from before the snapshot must be ignored.
    if snapshot_seq >= 1 {
        client.apply(&delta);
        assert_eq!(client.last_seq(), snapshot_seq, "stale delta was applied");
    }
}

/// Same seed ⇒ identical command stream; the feed is reproducible.
#[test]
fn synthetic_feed_is_deterministic() {
    let mut a = SyntheticFeed::seeded(42);
    let mut b = SyntheticFeed::seeded(42);
    for i in 0..1_000 {
        assert_eq!(a.next_command(), b.next_command(), "diverged at {i}");
    }

    let mut c = SyntheticFeed::seeded(43);
    let mut d = SyntheticFeed::seeded(42);
    let differs = (0..1_000).any(|_| c.next_command() != d.next_command());
    assert!(differs, "different seeds produced identical streams");
}

fn engine_order(id: u64, side: Side, price: u64, qty: u64) -> engine::Order {
    engine::Order {
        id: engine::OrderId(id),
        side,
        price: Price(price),
        qty,
    }
}
