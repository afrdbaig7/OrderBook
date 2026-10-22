//! Numeric limits for order input, price levels, and client interoperability.

use engine::{
    Event, Order, OrderBook, OrderId, Price, RejectReason, Side, MAX_LEVEL_QTY, MAX_ORDER_QTY,
    MAX_SAFE_INTEGER,
};

#[test]
fn reject_quantity_exceeding_max_safe_integer() {
    let mut book = OrderBook::new();
    let events = book.submit_limit(Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(100),
        qty: MAX_SAFE_INTEGER + 1,
    });

    assert_eq!(
        events,
        vec![Event::Reject {
            seq: 1,
            id: OrderId(1),
            reason: RejectReason::QuantityOutOfRange,
        }]
    );
    assert_eq!(book.total_resting_qty(), 0);
}

#[test]
fn reject_u64_max_quantity() {
    let mut book = OrderBook::new();
    let events = book.submit_limit(Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(100),
        qty: u64::MAX,
    });

    assert_eq!(
        events,
        vec![Event::Reject {
            seq: 1,
            id: OrderId(1),
            reason: RejectReason::QuantityOutOfRange,
        }]
    );
}

#[test]
fn reject_price_exceeding_max_safe_integer() {
    let mut book = OrderBook::new();
    let events = book.submit_limit(Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(MAX_SAFE_INTEGER + 1),
        qty: 10,
    });

    assert_eq!(
        events,
        vec![Event::Reject {
            seq: 1,
            id: OrderId(1),
            reason: RejectReason::PriceOutOfRange,
        }]
    );
}

#[test]
fn reject_market_order_exceeding_max_safe_integer() {
    let mut book = OrderBook::new();
    let events = book.submit_market(OrderId(1), Side::Bid, MAX_SAFE_INTEGER + 1);

    assert_eq!(
        events,
        vec![Event::Reject {
            seq: 1,
            id: OrderId(1),
            reason: RejectReason::QuantityOutOfRange,
        }]
    );
}

#[test]
fn accept_exact_max_safe_integer_order() {
    let mut book = OrderBook::new();
    let events = book.submit_limit(Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(100),
        qty: MAX_ORDER_QTY,
    });

    assert_eq!(events.len(), 2); // Ack + Delta
    assert!(matches!(events[0], Event::Ack { .. }));
    assert!(matches!(
        events[1],
        Event::Delta {
            new_total_qty,
            ..
        } if new_total_qty == MAX_ORDER_QTY
    ));
    assert_eq!(book.total_resting_qty(), MAX_ORDER_QTY);
}

#[test]
fn reject_aggregate_level_quantity_overflow() {
    let mut book = OrderBook::new();
    // First order takes half of MAX_LEVEL_QTY
    let half = MAX_LEVEL_QTY / 2 + 10;
    let events1 = book.submit_limit(Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(500),
        qty: half,
    });
    assert!(matches!(events1[0], Event::Ack { .. }));

    // Second order at the same price would overflow MAX_LEVEL_QTY
    let events2 = book.submit_limit(Order {
        id: OrderId(2),
        side: Side::Bid,
        price: Price(500),
        qty: half,
    });

    assert_eq!(
        events2,
        vec![
            Event::Ack {
                seq: 3,
                id: OrderId(2),
            },
            Event::Reject {
                seq: 4,
                id: OrderId(2),
                reason: RejectReason::LevelQuantityOverflow,
            }
        ]
    );

    // Book state remains clean and at the first order's quantity
    assert_eq!(book.total_resting_qty(), half);
    assert_eq!(book.index_len(), 1);
}

#[test]
fn reject_order_id_exceeding_max_safe_integer() {
    let mut book = OrderBook::new();
    let events = book.submit_limit(Order {
        id: OrderId(MAX_SAFE_INTEGER + 1),
        side: Side::Bid,
        price: Price(100),
        qty: 10,
    });

    assert_eq!(
        events,
        vec![Event::Reject {
            seq: 1,
            id: OrderId(MAX_SAFE_INTEGER + 1),
            reason: RejectReason::OrderIdOutOfRange,
        }]
    );

    let market_events = book.submit_market(OrderId(MAX_SAFE_INTEGER + 1), Side::Bid, 10);
    assert_eq!(
        market_events,
        vec![Event::Reject {
            seq: 2,
            id: OrderId(MAX_SAFE_INTEGER + 1),
            reason: RejectReason::OrderIdOutOfRange,
        }]
    );
}

#[test]
fn reject_book_depth_exceeding_max_levels_per_side() {
    let mut book = OrderBook::new();
    // Insert up to MAX_BOOK_LEVELS_PER_SIDE distinct price levels
    for i in 1..=engine::MAX_BOOK_LEVELS_PER_SIDE {
        let events = book.submit_limit(Order {
            id: OrderId(i as u64),
            side: Side::Bid,
            price: Price(i as u64),
            qty: 1,
        });
        assert!(matches!(events[0], Event::Ack { .. }));
    }

    assert_eq!(book.bid_levels().len(), engine::MAX_BOOK_LEVELS_PER_SIDE);

    // One more distinct price level on the same side must be rejected
    let overflow_id = OrderId((engine::MAX_BOOK_LEVELS_PER_SIDE + 1) as u64);
    let events = book.submit_limit(Order {
        id: overflow_id,
        side: Side::Bid,
        price: Price((engine::MAX_BOOK_LEVELS_PER_SIDE + 1) as u64),
        qty: 1,
    });

    assert_eq!(
        events,
        vec![
            Event::Ack {
                seq: (engine::MAX_BOOK_LEVELS_PER_SIDE * 2 + 1) as u64,
                id: overflow_id,
            },
            Event::Reject {
                seq: (engine::MAX_BOOK_LEVELS_PER_SIDE * 2 + 2) as u64,
                id: overflow_id,
                reason: RejectReason::BookDepthExceeded,
            }
        ]
    );

    // Existing price level order should still succeed
    let existing_price_order = book.submit_limit(Order {
        id: OrderId(overflow_id.0 + 1),
        side: Side::Bid,
        price: Price(1), // already exists
        qty: 5,
    });
    assert!(matches!(existing_price_order[0], Event::Ack { .. }));
    assert!(matches!(existing_price_order[1], Event::Delta { .. }));
}
