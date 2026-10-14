//! Wire-format tests: JSON shape, encode/decode round-trips, and the
//! event → message mapping contract (one message per event, `seq` kept).

use engine::{Order, OrderBook, OrderId, Price, RejectReason, Side};
use feed::{messages_from_events, Message};
use proptest::prelude::*;

#[test]
fn every_variant_round_trips_through_json() {
    let snapshot = OrderBook::new().snapshot(1_000);
    let messages = vec![
        Message::from_snapshot(&snapshot),
        Message::Delta {
            seq: 7,
            side: Side::Bid,
            price: Price(101),
            qty: 30,
        },
        Message::Trade {
            seq: 8,
            price: Price(101),
            qty: 5,
            taker_side: Side::Ask,
            ts: 1_730_000_000_000,
        },
        Message::Ack {
            seq: 9,
            id: OrderId(2),
        },
        Message::Cancel {
            seq: 10,
            id: OrderId(1),
        },
        Message::Reject {
            seq: 11,
            id: OrderId(3),
            reason: RejectReason::DuplicateOrderId,
        },
    ];

    for message in &messages {
        let json = message.encode();
        let decoded = Message::decode(&json).expect("decodes");
        assert_eq!(&decoded, message, "round-trip changed message: {json}");
        // Re-encoding must be byte-identical (canonical form).
        assert_eq!(decoded.encode(), json);
    }
}

#[test]
fn json_is_flat_and_tagged_by_type() {
    let cases = [
        (
            Message::Delta {
                seq: 1,
                side: Side::Ask,
                price: Price(9),
                qty: 4,
            },
            "\"type\":\"delta\"",
        ),
        (
            Message::Trade {
                seq: 2,
                price: Price(9),
                qty: 4,
                taker_side: Side::Bid,
                ts: 42,
            },
            "\"type\":\"trade\"",
        ),
        (
            Message::Ack {
                seq: 3,
                id: OrderId(5),
            },
            "\"type\":\"ack\"",
        ),
        (
            Message::Cancel {
                seq: 4,
                id: OrderId(5),
            },
            "\"type\":\"cancel\"",
        ),
        (
            Message::Reject {
                seq: 5,
                id: OrderId(5),
                reason: RejectReason::ZeroPrice,
            },
            "\"type\":\"reject\"",
        ),
    ];
    for (message, tag) in cases {
        let json = message.encode();
        assert!(json.contains(tag), "missing {tag} in {json}");
        // Flat object: exactly one level of braces after trimming.
        assert!(json.starts_with('{') && json.ends_with('}'));
        assert_eq!(json.matches('{').count(), 1, "not flat: {json}");
    }
}

#[test]
fn snapshot_json_shape_matches_docs() {
    let mut book = OrderBook::new();
    book.submit_limit(Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(100),
        qty: 15,
    });
    book.submit_limit(Order {
        id: OrderId(2),
        side: Side::Ask,
        price: Price(102),
        qty: 10,
    });

    let message = Message::from_snapshot(&book.snapshot(1_000));
    let json = message.encode();

    assert!(json.contains("\"type\":\"snapshot\""), "{json}");
    assert!(json.contains("\"bids\":[[100,15]]"), "{json}");
    assert!(json.contains("\"asks\":[[102,10]]"), "{json}");

    let decoded = Message::decode(&json).expect("decodes");
    assert_eq!(decoded, message);
    match decoded {
        Message::Snapshot(snap) => {
            assert_eq!(snap.bids, vec![(Price(100), 15)]);
            assert_eq!(snap.asks, vec![(Price(102), 10)]);
        }
        other => panic!("expected snapshot, got {other:?}"),
    }
}

#[test]
fn snapshot_json_roundtrips_serialized_engine_snapshot() {
    let mut book = OrderBook::new();
    for i in 0..20u64 {
        let side = if i % 2 == 0 { Side::Bid } else { Side::Ask };
        book.submit_limit(Order {
            id: OrderId(i + 1),
            side,
            price: Price(if side == Side::Bid { 100 - i } else { 100 + i }),
            qty: i + 1,
        });
    }
    let snapshot = book.snapshot(1_000);
    let message = Message::from_snapshot(&snapshot);
    let json = message.encode();
    let decoded = Message::decode(&json).expect("decodes");
    assert_eq!(decoded, Message::Snapshot(snapshot));
}

#[test]
fn trade_uses_maker_price_and_server_stamps_timestamp() {
    let mut book = OrderBook::new();
    book.submit_limit(Order {
        id: OrderId(1),
        side: Side::Ask,
        price: Price(100),
        qty: 10,
    });
    let events = book.submit_limit(Order {
        id: OrderId(2),
        side: Side::Bid,
        price: Price(105), // aggressive taker price
        qty: 4,
    });
    let fill = events
        .iter()
        .find(|e| matches!(e, engine::Event::Fill { .. }))
        .expect("expected a fill");

    let message = Message::from_event(fill);
    match &message {
        Message::Trade {
            ts,
            price,
            qty,
            taker_side,
            ..
        } => {
            assert_eq!(*ts, 0, "engine mapping carries no clock");
            assert_eq!(*price, Price(100), "fills execute at maker price");
            assert_eq!(*qty, 4);
            assert_eq!(*taker_side, Side::Bid);
        }
        other => panic!("expected trade, got {other:?}"),
    }

    let stamped = message.clone().with_timestamp(1_730_000_000_000);
    let json = stamped.encode();
    assert!(json.contains("\"ts\":1730000000000"), "{json}");
    assert_eq!(Message::decode(&json).unwrap(), stamped);

    // Stamping is a no-op for non-trade messages.
    let delta = Message::Delta {
        seq: 1,
        side: Side::Bid,
        price: Price(100),
        qty: 1,
    };
    assert_eq!(delta.clone().with_timestamp(99), delta);
}

#[test]
fn each_event_becomes_one_message_with_the_same_seq() {
    let mut book = OrderBook::new();
    let script = [
        Order {
            id: OrderId(1),
            side: Side::Ask,
            price: Price(100),
            qty: 10,
        },
        Order {
            id: OrderId(2),
            side: Side::Bid,
            price: Price(101),
            qty: 4,
        },
        Order {
            id: OrderId(1),
            side: Side::Bid,
            price: Price(99),
            qty: 5,
        }, // duplicate
    ];

    let mut seen = 0u64;
    let mut events = Vec::new();
    for order in script {
        events.extend(book.submit_limit(order));
    }
    events.extend(book.cancel(OrderId(1)));
    events.extend(book.cancel(OrderId(404))); // unknown

    let messages = messages_from_events(&events);
    assert_eq!(messages.len(), events.len());
    for (event, message) in events.iter().zip(&messages) {
        assert_eq!(event.seq(), message.seq(), "seq diverged");
    }

    // Contiguity: the wire stream is 1, 2, 3, ... with no gaps.
    let mut prev = 0;
    for message in &messages {
        assert_eq!(message.seq(), prev + 1, "gap in wire sequence");
        prev = message.seq();
        seen += 1;
    }
    assert_eq!(seen, events.len() as u64);
}

proptest! {
    /// Any successful or rejected limit submission maps to messages that
    /// survive JSON round-trips unchanged.
    #[test]
    fn event_messages_survive_json_roundtrip(
        id in 0u64..1_000,
        side in prop::sample::select(vec![Side::Bid, Side::Ask]),
        price in 1u64..1_000,
        qty in 1u64..500,
    ) {
        let mut book = OrderBook::new();
        let events = book.submit_limit(Order {
            id: OrderId(id),
            side,
            price: Price(price),
            qty,
        });
        for event in &events {
            let message = Message::from_event(event);
            let json = message.encode();
            let back = Message::decode(&json).expect("decodes");
            prop_assert_eq!(&back, &message);
            prop_assert_eq!(back.encode(), json);
        }
    }
}
