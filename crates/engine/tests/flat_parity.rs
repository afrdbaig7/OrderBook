//! Differential parity tests: FlatBook vs OrderBook.
//!
//! Asserts identical event streams (sequence numbers, fill prices, quantities,
//! order IDs, reject reasons) and identical point-in-time snapshots across
//! non-crossing inserts, FIFO cancellations, maker matches, and multi-level sweeps.

use engine::{Event, FlatBook, FlatBookConfig, FlatError, Order, OrderBook, OrderId, Price, Side};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const BASE_PRICE: u64 = 1_000_000_000;
const TICK_COUNT: usize = 512;
const MAX_ORDERS: usize = 2048;

fn make_books() -> (OrderBook, FlatBook, Vec<Event>) {
    let order_book = OrderBook::new();
    let config = FlatBookConfig {
        base_price: BASE_PRICE,
        tick_count: TICK_COUNT,
        max_orders: MAX_ORDERS,
    };
    let flat_book = FlatBook::new(config).expect("valid config");
    let buf = Vec::with_capacity(flat_book.worst_case_event_capacity());
    (order_book, flat_book, buf)
}

fn assert_parity(
    order_book: &OrderBook,
    flat_book: &FlatBook,
    ob_events: &[Event],
    fb_events: &[Event],
    context: &str,
) {
    assert_eq!(
        ob_events, fb_events,
        "event mismatch in {context}\nOB: {ob_events:#?}\nFB: {fb_events:#?}"
    );
    assert_eq!(
        order_book.seq(),
        flat_book.seq(),
        "sequence mismatch in {context}"
    );

    let ob_snap = order_book.snapshot(1000);
    let fb_snap = flat_book.snapshot();

    assert_eq!(
        ob_snap.bids, fb_snap.bids,
        "bid snapshot mismatch in {context}"
    );
    assert_eq!(
        ob_snap.asks, fb_snap.asks,
        "ask snapshot mismatch in {context}"
    );
    assert_eq!(
        order_book.best_bid(),
        flat_book.best_bid(),
        "best bid mismatch in {context}"
    );
    assert_eq!(
        order_book.best_ask(),
        flat_book.best_ask(),
        "best ask mismatch in {context}"
    );
}

#[test]
fn test_parity_non_crossing_limits() {
    let (mut ob, mut fb, mut buf) = make_books();

    for i in 0..50 {
        let order = Order {
            id: OrderId(i + 1),
            side: Side::Bid,
            price: Price(BASE_PRICE + 100 - (i % 20)),
            qty: 10 + i,
        };
        let ob_ev = ob.submit_limit(order.clone());
        fb.submit_limit_into(order, &mut buf).expect("flat insert");
        assert_parity(&ob, &fb, &ob_ev, &buf, &format!("insert bid {i}"));
    }

    for i in 0..50 {
        let order = Order {
            id: OrderId(100 + i + 1),
            side: Side::Ask,
            price: Price(BASE_PRICE + 200 + (i % 20)),
            qty: 15 + i,
        };
        let ob_ev = ob.submit_limit(order.clone());
        fb.submit_limit_into(order, &mut buf).expect("flat insert");
        assert_parity(&ob, &fb, &ob_ev, &buf, &format!("insert ask {i}"));
    }
}

#[test]
fn test_parity_cancellations() {
    let (mut ob, mut fb, mut buf) = make_books();

    // Populate 3 orders at same level to test front, middle, back cancellations
    for i in 1..=3 {
        let order = Order {
            id: OrderId(i),
            side: Side::Bid,
            price: Price(BASE_PRICE + 50),
            qty: 10 * i,
        };
        let ob_ev = ob.submit_limit(order.clone());
        fb.submit_limit_into(order, &mut buf).expect("flat insert");
        assert_parity(&ob, &fb, &ob_ev, &buf, "setup order");
    }

    // Cancel middle
    let ob_ev = ob.cancel(OrderId(2));
    fb.cancel_into(OrderId(2), &mut buf).expect("cancel middle");
    assert_parity(&ob, &fb, &ob_ev, &buf, "cancel middle");

    // Cancel front
    let ob_ev = ob.cancel(OrderId(1));
    fb.cancel_into(OrderId(1), &mut buf).expect("cancel front");
    assert_parity(&ob, &fb, &ob_ev, &buf, "cancel front");

    // Cancel back
    let ob_ev = ob.cancel(OrderId(3));
    fb.cancel_into(OrderId(3), &mut buf).expect("cancel back");
    assert_parity(&ob, &fb, &ob_ev, &buf, "cancel back");

    // Cancel unknown order
    let ob_ev = ob.cancel(OrderId(999));
    fb.cancel_into(OrderId(999), &mut buf)
        .expect("cancel unknown");
    assert_parity(&ob, &fb, &ob_ev, &buf, "cancel unknown");
}

#[test]
fn test_parity_single_and_multi_maker_matching() {
    let (mut ob, mut fb, mut buf) = make_books();

    // Resting asks: 3 orders at price 100
    for i in 1..=3 {
        let order = Order {
            id: OrderId(i),
            side: Side::Ask,
            price: Price(BASE_PRICE + 100),
            qty: 10,
        };
        let ob_ev = ob.submit_limit(order.clone());
        fb.submit_limit_into(order, &mut buf).expect("ask setup");
        assert_parity(&ob, &fb, &ob_ev, &buf, "ask setup");
    }

    // Partial fill first maker (taker qty 5)
    let taker1 = Order {
        id: OrderId(10),
        side: Side::Bid,
        price: Price(BASE_PRICE + 100),
        qty: 5,
    };
    let ob_ev = ob.submit_limit(taker1.clone());
    fb.submit_limit_into(taker1, &mut buf)
        .expect("partial match");
    assert_parity(&ob, &fb, &ob_ev, &buf, "partial match");

    // Multi-maker fill: fill remainder of first maker (5) + entire second maker (10) + partial of third (2) = 17
    let taker2 = Order {
        id: OrderId(11),
        side: Side::Bid,
        price: Price(BASE_PRICE + 100),
        qty: 17,
    };
    let ob_ev = ob.submit_limit(taker2.clone());
    fb.submit_limit_into(taker2, &mut buf)
        .expect("multi-maker match");
    assert_parity(&ob, &fb, &ob_ev, &buf, "multi-maker match");
}

#[test]
fn test_parity_multi_level_sweep() {
    let (mut ob, mut fb, mut buf) = make_books();

    // Resting asks across 5 consecutive levels
    for level in 0..5 {
        for order_in_level in 0..2 {
            let id = level * 10 + order_in_level + 1;
            let order = Order {
                id: OrderId(id),
                side: Side::Ask,
                price: Price(BASE_PRICE + 100 + level),
                qty: 10,
            };
            let ob_ev = ob.submit_limit(order.clone());
            fb.submit_limit_into(order, &mut buf).expect("ask setup");
            assert_parity(&ob, &fb, &ob_ev, &buf, "ask sweep setup");
        }
    }

    // Taker bid sweeps across 3 levels and rests remainder:
    // Levels 100, 101, 102 have 20 qty each = 60 total liquidity.
    // Taker requests 75 at price 102.
    // Sweeps 60 from asks, and rests 15 as bid at 102!
    let sweep_taker = Order {
        id: OrderId(99),
        side: Side::Bid,
        price: Price(BASE_PRICE + 102),
        qty: 75,
    };
    let ob_ev = ob.submit_limit(sweep_taker.clone());
    fb.submit_limit_into(sweep_taker, &mut buf)
        .expect("sweep match");
    assert_parity(&ob, &fb, &ob_ev, &buf, "sweep match");
}

#[test]
fn test_parity_market_order_and_unfilled_remainder() {
    let (mut ob, mut fb, mut buf) = make_books();

    // 2 asks with 10 qty each = 20 total
    for i in 1..=2 {
        let order = Order {
            id: OrderId(i),
            side: Side::Ask,
            price: Price(BASE_PRICE + 50),
            qty: 10,
        };
        let ob_ev = ob.submit_limit(order.clone());
        fb.submit_limit_into(order, &mut buf).expect("setup");
        assert_parity(&ob, &fb, &ob_ev, &buf, "setup");
    }

    // Market buy for 30 (fills 20, drops 10 with MarketUnfilled)
    let ob_ev = ob.submit_market(OrderId(100), Side::Bid, 30);
    fb.submit_market_into(OrderId(100), Side::Bid, 30, &mut buf)
        .expect("market");
    assert_parity(&ob, &fb, &ob_ev, &buf, "market partial fill remainder drop");
}

#[test]
fn test_randomized_differential_stress() {
    let (mut ob, mut fb, mut buf) = make_books();
    let mut rng = ChaCha8Rng::seed_from_u64(1337);

    let mut resting_ids = Vec::new();
    let mut next_id = 1u64;

    for step in 0..5_000 {
        let op_type = rng.random_range(0..100);

        if op_type < 60 {
            // Limit order
            let side = if rng.random_bool(0.5) {
                Side::Bid
            } else {
                Side::Ask
            };
            // Confine to 512-tick active window around BASE_PRICE
            let tick = rng.random_range(50..450);
            let price = Price(BASE_PRICE + tick);
            let qty = rng.random_range(1..50);
            let id = OrderId(next_id);
            next_id += 1;

            let order = Order {
                id,
                side,
                price,
                qty,
            };

            let ob_ev = ob.submit_limit(order.clone());
            fb.submit_limit_into(order, &mut buf).expect("flat limit");

            assert_parity(&ob, &fb, &ob_ev, &buf, &format!("rand step {step} limit"));
            resting_ids.push(id);
        } else if op_type < 85 && !resting_ids.is_empty() {
            // Cancel order
            let pick = rng.random_range(0..resting_ids.len());
            let id = resting_ids.swap_remove(pick);

            let ob_ev = ob.cancel(id);
            fb.cancel_into(id, &mut buf).expect("flat cancel");

            assert_parity(&ob, &fb, &ob_ev, &buf, &format!("rand step {step} cancel"));
        } else {
            // Market order
            let side = if rng.random_bool(0.5) {
                Side::Bid
            } else {
                Side::Ask
            };
            let qty = rng.random_range(1..100);
            let id = OrderId(next_id);
            next_id += 1;

            let ob_ev = ob.submit_market(id, side, qty);
            fb.submit_market_into(id, side, qty, &mut buf)
                .expect("flat market");

            assert_parity(&ob, &fb, &ob_ev, &buf, &format!("rand step {step} market"));
        }
    }
}

#[test]
fn test_flatbook_specific_capacity_and_errors() {
    let config = FlatBookConfig {
        base_price: BASE_PRICE,
        tick_count: 512,
        max_orders: 4,
    };
    let mut fb = FlatBook::new(config).expect("init");
    let mut small_buf = Vec::with_capacity(1);
    let mut ok_buf = Vec::with_capacity(fb.worst_case_event_capacity());

    // 1. Output buffer too small
    let order = Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(BASE_PRICE + 10),
        qty: 5,
    };
    assert!(matches!(
        fb.submit_limit_into(order.clone(), &mut small_buf),
        Err(FlatError::OutputBufferTooSmall { .. })
    ));

    // 2. Price out of window returns explicit PriceOutOfWindow error (not BookDepthExceeded)
    let out_of_window = Order {
        id: OrderId(2),
        side: Side::Bid,
        price: Price(BASE_PRICE + 1000), // > 512 ticks
        qty: 5,
    };
    assert!(matches!(
        fb.submit_limit_into(out_of_window, &mut ok_buf),
        Err(FlatError::PriceOutOfWindow { .. })
    ));

    // 3. Arena capacity exhaustion returns explicit ArenaFull error
    for i in 1..=4 {
        let o = Order {
            id: OrderId(i),
            side: Side::Bid,
            price: Price(BASE_PRICE + 10),
            qty: 5,
        };
        fb.submit_limit_into(o, &mut ok_buf).expect("insert");
    }
    assert_eq!(fb.resting_order_count(), 4);

    let overflow_order = Order {
        id: OrderId(5),
        side: Side::Bid,
        price: Price(BASE_PRICE + 10),
        qty: 5,
    };
    assert!(matches!(
        fb.submit_limit_into(overflow_order, &mut ok_buf),
        Err(FlatError::ArenaFull(4))
    ));

    // 4. Constructor validation
    assert!(matches!(
        FlatBook::new(FlatBookConfig {
            tick_count: 0,
            ..config
        }),
        Err(FlatError::InvalidConfig(_))
    ));
    assert!(matches!(
        FlatBook::new(FlatBookConfig {
            tick_count: 5000,
            ..config
        }),
        Err(FlatError::InvalidConfig(_))
    ));
    assert!(matches!(
        FlatBook::new(FlatBookConfig {
            max_orders: 0,
            ..config
        }),
        Err(FlatError::InvalidConfig(_))
    ));
    assert!(matches!(
        FlatBook::new(FlatBookConfig {
            base_price: 0,
            ..config
        }),
        Err(FlatError::InvalidConfig(_))
    ));
    assert!(matches!(
        FlatBook::new(FlatBookConfig {
            base_price: u64::MAX,
            ..config
        }),
        Err(FlatError::InvalidConfig(_))
    ));
}

#[test]
fn test_numeric_rejection_precedence_parity() {
    let (mut ob, mut fb, mut buf) = make_books();

    // First resting order to create a duplicate ID candidate
    let base_order = Order {
        id: OrderId(42),
        side: Side::Bid,
        price: Price(BASE_PRICE + 50),
        qty: 10,
    };
    ob.submit_limit(base_order.clone());
    fb.submit_limit_into(base_order, &mut buf)
        .expect("base insert");

    // Precedence 1: OrderIdOutOfRange over ZeroQuantity
    let o1 = Order {
        id: OrderId(engine::MAX_ORDER_ID + 1),
        side: Side::Bid,
        price: Price(BASE_PRICE + 50),
        qty: 0,
    };
    let ev_ob1 = ob.submit_limit(o1.clone());
    fb.submit_limit_into(o1, &mut buf).expect("handled");
    assert_parity(&ob, &fb, &ev_ob1, &buf, "OrderIdOutOfRange > ZeroQuantity");

    // Precedence 2: ZeroQuantity over ZeroPrice
    let o2 = Order {
        id: OrderId(101),
        side: Side::Bid,
        price: Price(0),
        qty: 0,
    };
    let ev_ob2 = ob.submit_limit(o2.clone());
    fb.submit_limit_into(o2, &mut buf).expect("handled");
    assert_parity(&ob, &fb, &ev_ob2, &buf, "ZeroQuantity > ZeroPrice");

    // Precedence 3: QuantityOutOfRange over ZeroPrice
    let o3 = Order {
        id: OrderId(102),
        side: Side::Bid,
        price: Price(0),
        qty: engine::MAX_ORDER_QTY + 1,
    };
    let ev_ob3 = ob.submit_limit(o3.clone());
    fb.submit_limit_into(o3, &mut buf).expect("handled");
    assert_parity(&ob, &fb, &ev_ob3, &buf, "QuantityOutOfRange > ZeroPrice");

    // Precedence 4: ZeroPrice over DuplicateOrderId
    let o4 = Order {
        id: OrderId(42), // duplicate resting ID
        side: Side::Bid,
        price: Price(0),
        qty: 10,
    };
    let ev_ob4 = ob.submit_limit(o4.clone());
    fb.submit_limit_into(o4, &mut buf).expect("handled");
    assert_parity(&ob, &fb, &ev_ob4, &buf, "ZeroPrice > DuplicateOrderId");

    // Precedence 5: PriceOutOfRange over DuplicateOrderId
    let o5 = Order {
        id: OrderId(42), // duplicate resting ID
        side: Side::Bid,
        price: Price(engine::MAX_PRICE + 1),
        qty: 10,
    };
    let ev_ob5 = ob.submit_limit(o5.clone());
    fb.submit_limit_into(o5, &mut buf).expect("handled");
    assert_parity(
        &ob,
        &fb,
        &ev_ob5,
        &buf,
        "PriceOutOfRange > DuplicateOrderId",
    );

    // Precedence 6: DuplicateOrderId when price and qty are valid
    let o6 = Order {
        id: OrderId(42), // duplicate resting ID
        side: Side::Bid,
        price: Price(BASE_PRICE + 50),
        qty: 10,
    };
    let ev_ob6 = ob.submit_limit(o6.clone());
    fb.submit_limit_into(o6, &mut buf).expect("handled");
    assert_parity(&ob, &fb, &ev_ob6, &buf, "DuplicateOrderId");
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TestCommand {
    Limit {
        id: OrderId,
        side: Side,
        price: Price,
        qty: u64,
    },
    Market {
        id: OrderId,
        side: Side,
        qty: u64,
    },
    Cancel {
        id: OrderId,
    },
}

#[test]
fn test_replay_scenario_basic_commands() {
    let commands_str = include_str!("../../../docs/recordings/basic.commands.jsonl");

    let mut ob = OrderBook::new();
    let config = FlatBookConfig {
        base_price: 90,
        tick_count: 512,
        max_orders: 1024,
    };
    let mut fb = FlatBook::new(config).expect("valid config");
    let mut buf = Vec::with_capacity(fb.worst_case_event_capacity());

    for (line_idx, line) in commands_str.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let cmd: TestCommand = serde_json::from_str(trimmed)
            .unwrap_or_else(|e| panic!("invalid json on line {line_idx}: {e}"));

        match cmd {
            TestCommand::Limit {
                id,
                side,
                price,
                qty,
            } => {
                let order = Order {
                    id,
                    side,
                    price,
                    qty,
                };
                let ob_ev = ob.submit_limit(order.clone());
                fb.submit_limit_into(order, &mut buf)
                    .expect("limit command");
                assert_parity(
                    &ob,
                    &fb,
                    &ob_ev,
                    &buf,
                    &format!("replay line {line_idx} limit"),
                );
            }
            TestCommand::Market { id, side, qty } => {
                let ob_ev = ob.submit_market(id, side, qty);
                fb.submit_market_into(id, side, qty, &mut buf)
                    .expect("market command");
                assert_parity(
                    &ob,
                    &fb,
                    &ob_ev,
                    &buf,
                    &format!("replay line {line_idx} market"),
                );
            }
            TestCommand::Cancel { id } => {
                let ob_ev = ob.cancel(id);
                fb.cancel_into(id, &mut buf).expect("cancel command");
                assert_parity(
                    &ob,
                    &fb,
                    &ob_ev,
                    &buf,
                    &format!("replay line {line_idx} cancel"),
                );
            }
        }
    }

    // Compare final snapshots
    let ob_snap = ob.snapshot(100);
    let fb_snap = fb.snapshot();
    assert_eq!(ob_snap.bids, fb_snap.bids);
    assert_eq!(ob_snap.asks, fb_snap.asks);
    assert_eq!(ob.seq(), fb.seq());
}

#[test]
fn test_full_table_deletion_and_repeated_reuse() {
    let cap = 64;
    let config = FlatBookConfig {
        base_price: BASE_PRICE,
        tick_count: 128,
        max_orders: cap,
    };
    let mut fb = FlatBook::new(config).expect("init");
    let mut buf = Vec::with_capacity(fb.worst_case_event_capacity());

    for cycle in 0..5 {
        // Fill table to 100%
        for i in 1..=cap {
            let id = OrderId((cycle * 1000 + i) as u64);
            let order = Order {
                id,
                side: Side::Bid,
                price: Price(BASE_PRICE + (i as u64 % 50)),
                qty: 10,
            };
            fb.submit_limit_into(order, &mut buf)
                .unwrap_or_else(|e| panic!("insert failed cycle {cycle} order {i}: {e:?}"));
        }
        assert_eq!(fb.resting_order_count(), cap);

        // Cannot insert 1 more
        let overflow = Order {
            id: OrderId(999_999),
            side: Side::Bid,
            price: Price(BASE_PRICE + 10),
            qty: 10,
        };
        assert!(matches!(
            fb.submit_limit_into(overflow, &mut buf),
            Err(FlatError::ArenaFull(_))
        ));

        // Delete all orders in arbitrary order (odd then even)
        for i in (1..=cap).step_by(2) {
            let id = OrderId((cycle * 1000 + i) as u64);
            fb.cancel_into(id, &mut buf).expect("cancel odd");
        }
        for i in (2..=cap).step_by(2) {
            let id = OrderId((cycle * 1000 + i) as u64);
            fb.cancel_into(id, &mut buf).expect("cancel even");
        }
        assert_eq!(fb.resting_order_count(), 0);
        assert_eq!(fb.best_bid(), None);
        assert_eq!(fb.best_ask(), None);
    }
}

#[test]
fn test_maximum_supported_output_sweep() {
    let (mut ob, mut fb, mut buf) = make_books();

    // Populate 200 makers across 50 price levels
    let mut id_counter = 1u64;
    for tick in 100..150 {
        for _ in 0..4 {
            let order = Order {
                id: OrderId(id_counter),
                side: Side::Ask,
                price: Price(BASE_PRICE + tick),
                qty: 5,
            };
            id_counter += 1;
            let ob_ev = ob.submit_limit(order.clone());
            fb.submit_limit_into(order, &mut buf).expect("ask setup");
            assert_parity(&ob, &fb, &ob_ev, &buf, "setup ask");
        }
    }

    // Now submit a gigantic taker buy that sweeps all 200 makers across all 50 levels
    // Total liquidity = 200 * 5 = 1000.
    // Taker requests 1050 at price BASE_PRICE + 150.
    // Sweeps all 1000 from asks, then rests 50 at BASE_PRICE + 150!
    let taker = Order {
        id: OrderId(id_counter),
        side: Side::Bid,
        price: Price(BASE_PRICE + 150),
        qty: 1050,
    };
    let ob_ev = ob.submit_limit(taker.clone());
    fb.submit_limit_into(taker, &mut buf).expect("sweep all");

    assert_eq!(buf.len(), ob_ev.len());
    // 1 Ack + 200 Fills + 50 Ask Deltas + 1 Resting Bid Delta = 252 events!
    assert_eq!(buf.len(), 252);
    assert_parity(&ob, &fb, &ob_ev, &buf, "massive sweep 200 makers 50 levels");
    assert_eq!(fb.best_ask(), None);
    assert_eq!(fb.best_bid(), Some(Price(BASE_PRICE + 150)));
}
