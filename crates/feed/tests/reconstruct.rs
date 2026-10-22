//! End-to-end reconstruction test: a synthetic feed drives the engine, every event
//! is serialized to JSON, and a client reconstructs the book **from the
//! serialized bytes**. The reconstruction must equal the engine state after
//! every single operation.

use std::collections::BTreeMap;

use engine::{Event, Order, OrderBook, Price, PriceLevel, Side};
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

/// Regression test: starting from a populated book deeper than a display or
/// truncation limit.
///
/// Demonstrates that:
/// 1. A depth-limited snapshot (e.g. depth 20 on a 30-level book) leads to an
///    incomplete client ladder when the top levels trade out, despite `synced == true`.
/// 2. A complete L2 snapshot (default bootstrap) preserves all resting depth,
///    allowing the client ladder to seamlessly reveal newly exposed resting levels.
#[test]
fn regression_deep_book_snapshot_reconstruction() {
    let mut engine = OrderBook::new();

    // Populate book with 30 distinct bid levels (130 down to 101)
    // and 30 distinct ask levels (140 up to 169).
    for i in 0..30 {
        Command::Limit(engine_order(100 + i, Side::Bid, 130 - i, 10)).apply(&mut engine);
        Command::Limit(engine_order(200 + i, Side::Ask, 140 + i, 10)).apply(&mut engine);
    }
    assert_eq!(engine.bid_levels().len(), 30);
    assert_eq!(engine.ask_levels().len(), 30);

    // Case 1: An artificially truncated snapshot (depth = 20)
    let truncated_snap = Message::from_snapshot(&engine.snapshot(20));
    let mut truncated_client = ClientBook::new();
    truncated_client.apply(&truncated_snap);
    assert_eq!(truncated_client.bids().len(), 20);

    // Case 2: Complete L2 bootstrap snapshot (depth = 0 or 1,000)
    let full_snap = Message::from_snapshot(&engine.snapshot(0));
    let mut complete_client = ClientBook::new();
    complete_client.apply(&full_snap);
    assert_eq!(complete_client.bids().len(), 30);

    // Now sweep out the top 10 bid levels via market asks:
    // Taker asks match against best bids (prices 130 down to 121).
    let sweep = Command::Market {
        id: engine::OrderId(999),
        side: Side::Ask,
        qty: 100, // 10 levels * 10 qty each = 100
    };
    let events = sweep.apply(&mut engine);
    for msg in feed::messages_from_events(&events) {
        truncated_client.apply(&msg);
        complete_client.apply(&msg);
    }

    // Engine now has 20 bid levels remaining (prices 120 down to 101).
    assert_eq!(engine.bid_levels().len(), 20);

    // Truncated client reports synced=true and contiguous seq, BUT its ladder is incomplete!
    // It only has 10 levels left (prices 120 down to 111) because prices 110..101 were never sent!
    assert!(truncated_client.is_synced());
    assert_eq!(truncated_client.last_seq(), engine.seq());
    assert_eq!(
        truncated_client.bids().len(),
        10,
        "truncated client missed levels 110..101"
    );
    let (trunc_ladder_bids, _) = truncated_client.ladder(20);
    assert_eq!(trunc_ladder_bids.len(), 10, "ladder shows incomplete depth");

    // A complete bootstrap retains levels that become visible after cancellation.
    assert!(complete_client.is_synced());
    assert_eq!(complete_client.last_seq(), engine.seq());
    assert_eq!(complete_client.bids().len(), 20);
    let (comp_ladder_bids, _) = complete_client.ladder(20);
    assert_eq!(
        comp_ladder_bids.len(),
        20,
        "ladder displays all 20 visible levels"
    );
    assert_synced(
        &complete_client,
        &engine,
        "complete bootstrap preserves deep levels",
    );
}

/// Regression test: asserts client depth equals engine depth after boundary changes.
///
/// Reproduces the exact failure mode:
/// Bids: 100, 99, 98.
/// Client bootstraps via complete snapshot.
/// Cancel bid 100 -> ladder boundary shifts to 99 and 98.
/// Asserts client depth == engine depth and ladder displays [99, 98].
#[test]
fn client_depth_equals_engine_depth_after_boundary_changes() {
    let mut engine = OrderBook::new();

    // Populate initial bids: 100, 99, 98
    for (id, price) in [(1, 100), (2, 99), (3, 98)] {
        engine.submit_limit(Order {
            id: engine::OrderId(id),
            side: Side::Bid,
            price: Price(price),
            qty: 10,
        });
    }

    assert_eq!(engine.bid_levels().len(), 3);

    // Bootstrap client via complete snapshot (depth 0)
    let snapshot_msg = Message::from_snapshot(&engine.snapshot(0));
    let mut client = ClientBook::new();
    client.apply(&snapshot_msg);

    assert_eq!(client.bids().len(), engine.bid_levels().len());
    let (ladder_bids, _) = client.ladder(2);
    assert_eq!(ladder_bids.len(), 2);
    assert_eq!(ladder_bids[0].0, Price(100));
    assert_eq!(ladder_bids[1].0, Price(99));

    // Cancel top bid at 100
    let cancel_events = engine.cancel(engine::OrderId(1));
    for msg in feed::messages_from_events(&cancel_events) {
        client.apply(&msg);
    }

    // Engine bids: 99, 98 (depth = 2)
    assert_eq!(engine.bid_levels().len(), 2);

    // Client bids: must be 99, 98 (depth = 2)
    assert_eq!(
        client.bids().len(),
        engine.bid_levels().len(),
        "client depth must equal engine depth after top-level cancel"
    );

    // Depth-2 ladder must display 99 and 98
    let (new_ladder_bids, _) = client.ladder(2);
    assert_eq!(new_ladder_bids.len(), 2);
    assert_eq!(new_ladder_bids[0].0, Price(99));
    assert_eq!(new_ladder_bids[1].0, Price(98));

    assert_synced(
        &client,
        &engine,
        "client stays 100% in sync across boundary movements",
    );
}

/// Soak test: runs 30,000 commands and verifies candidate pool size stays strictly
/// bounded, while tracking successful cancels vs rejected cancels.
#[test]
fn feed_memory_remains_bounded_and_measures_cancel_rates() {
    let mut feed = SyntheticFeed::new(feed::FeedConfig {
        max_live_candidates: 300,
        ..Default::default()
    });
    let mut engine = OrderBook::new();

    let mut successful_cancels = 0u64;
    let mut rejected_cancels = 0u64;

    for _ in 0..30_000 {
        let cmd = feed.next_command();
        assert!(
            feed.live_hint() <= 300,
            "live candidate pool exceeded bound: {}",
            feed.live_hint()
        );
        let events = cmd.apply(&mut engine);
        if matches!(cmd, Command::Cancel(_)) {
            if events.iter().any(|e| matches!(e, Event::Cancel { .. })) {
                successful_cancels += 1;
            } else if events.iter().any(|e| matches!(e, Event::Reject { .. })) {
                rejected_cancels += 1;
            }
        }
    }

    assert!(successful_cancels > 0, "must produce successful cancels");
    assert!(rejected_cancels > 0, "must track stale candidates cleanly");
    assert!(feed.live_hint() <= 300);
}
