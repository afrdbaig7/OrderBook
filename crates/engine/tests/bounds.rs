//! Arithmetic boundary and numeric overflow tests.
//!
//! Verifies Requirement 4:
//! Enforces numeric bounds (MAX_SAFE_INTEGER = 2^53 - 1 = 9,007,199,254,740,991)
//! ensuring exact representation across JavaScript Number, PostgreSQL BIGINT,
//! and checked arithmetic in the engine.

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
