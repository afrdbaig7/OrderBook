//! Unit tests for `OrderBook`.
//!
//! Each test documents the correctness property it protects.

use engine::{Event, Fill, Order, OrderBook, OrderId, Price, RejectReason, Side};

fn bid(id: u64, price: u64, qty: u64) -> Order {
    Order {
        id: OrderId(id),
        side: Side::Bid,
        price: Price(price),
        qty,
    }
}

fn ask(id: u64, price: u64, qty: u64) -> Order {
    Order {
        id: OrderId(id),
        side: Side::Ask,
        price: Price(price),
        qty,
    }
}

fn fills(events: &[Event]) -> Vec<Fill> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Fill { fill, .. } => Some(fill.clone()),
            _ => None,
        })
        .collect()
}

fn deltas(events: &[Event]) -> Vec<(Side, Price, u64)> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Delta {
                side,
                price,
                new_total_qty,
                ..
            } => Some((*side, *price, *new_total_qty)),
            _ => None,
        })
        .collect()
}

fn reject_reasons(events: &[Event]) -> Vec<RejectReason> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Reject { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect()
}

/// Property: every command's events are numbered contiguously starting at
/// `book_before_seq + 1` — the foundation of client gap detection.
fn assert_contiguous(events: &[Event], book_before_seq: u64) {
    for (i, ev) in events.iter().enumerate() {
        assert_eq!(
            ev.seq(),
            book_before_seq + 1 + i as u64,
            "event {i}: {ev:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Price-time priority
// ---------------------------------------------------------------------------

/// Property: a better-priced opposing order always executes before a
/// worse-priced one, regardless of arrival time (price priority).
#[test]
fn better_priced_order_fills_first() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 101, 10)); // arrives first, worse price
    book.submit_limit(ask(2, 100, 10)); // arrives second, better price

    let evs = book.submit_limit(bid(3, 101, 15));
    let prices: Vec<u64> = fills(&evs).iter().map(|f| f.price.0).collect();

    assert_eq!(prices, vec![100, 101]); // drains 100, then 5 of 101
    assert_eq!(book.order(OrderId(2)), None); // better-priced consumed fully
    assert_eq!(
        book.order(OrderId(1)).map(|o| o.qty),
        Some(5), // worse-priced partially filled, still resting
    );
}

/// Property: at one price, arrival order decides execution order (time
/// priority / FIFO), and fully filled makers are purged from the index.
#[test]
fn same_price_orders_fill_fifo() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));
    book.submit_limit(ask(2, 100, 10));

    let evs = book.submit_limit(bid(3, 100, 15));
    let makers: Vec<u64> = fills(&evs).iter().map(|f| f.maker_id.0).collect();

    assert_eq!(makers, vec![1, 2]); // oldest first, never reordered
    assert!(!book.is_resting(OrderId(1))); // fully filled: purged
    assert_eq!(book.order(OrderId(2)).map(|o| o.qty), Some(5));
}

/// Property: a partially filled maker stays at the front of its level with
/// its reduced quantity.
#[test]
fn partial_fill_leaves_remainder_resting() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));

    let evs = book.submit_limit(bid(2, 100, 4));
    assert_eq!(fills(&evs)[0].qty, 4);

    let resting = book.order(OrderId(1)).expect("maker still rests");
    assert_eq!(resting.qty, 6);
    assert_eq!(book.ask_levels()[&Price(100)].total_qty(), 6);
    assert!(book.is_resting(OrderId(1)));
}

/// Property: an exact fill removes the maker and, when the level empties,
/// removes the level entirely (delta qty 0 = wire-level delete signal).
#[test]
fn exact_fill_removes_maker_and_level() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));

    let evs = book.submit_limit(bid(2, 100, 10));
    assert_eq!(deltas(&evs), vec![(Side::Ask, Price(100), 0)]);
    assert!(book.ask_levels().is_empty());
    assert!(!book.is_resting(OrderId(1)));
}

/// Property: a single aggressive order sweeps levels from best price
/// outward, draining each before touching the next.
#[test]
fn sweep_across_three_levels() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 5));
    book.submit_limit(ask(2, 101, 5));
    book.submit_limit(ask(3, 102, 5));

    let evs = book.submit_limit(bid(4, 102, 15));
    let seen: Vec<(u64, u64)> = fills(&evs).iter().map(|f| (f.price.0, f.qty)).collect();

    assert_eq!(seen, vec![(100, 5), (101, 5), (102, 5)]);
    assert!(book.ask_levels().is_empty());
    assert!(!book.is_resting(OrderId(4))); // fully filled: never indexed
}

/// Property: an order that only partly crosses rests its remainder on its
/// own side, and the book is left uncrossed.
#[test]
fn limit_partly_crosses_then_rests_remainder() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 5));
    book.submit_limit(ask(2, 101, 5));
    book.submit_limit(ask(3, 105, 5)); // stays after the sweep

    let evs = book.submit_limit(bid(4, 101, 12));
    assert_eq!(
        fills(&evs).iter().map(|f| f.price.0).collect::<Vec<_>>(),
        vec![100, 101]
    );

    // Remainder 12 - 10 = 2 rests as a bid at 101.
    assert_eq!(book.best_bid(), Some(Price(101)));
    assert_eq!(book.order(OrderId(4)).map(|o| o.qty), Some(2));
    assert_eq!(book.bid_levels()[&Price(101)].total_qty(), 2);

    // Crossed makers are gone; the untouched level remains.
    assert_eq!(book.order(OrderId(1)), None);
    assert_eq!(book.order(OrderId(2)), None);
    assert_eq!(book.order(OrderId(3)).map(|o| o.qty), Some(5));

    // Never crossed.
    assert!(book.best_bid().unwrap() < book.best_ask().unwrap());
}

/// Property: ask-side crossing works symmetrically, and the fill executes
/// at the resting **bid's** price — not the incoming ask's limit price.
#[test]
fn ask_limit_crosses_at_maker_price() {
    let mut book = OrderBook::new();
    book.submit_limit(bid(1, 100, 10));

    let evs = book.submit_limit(ask(2, 90, 10)); // would accept >= 90
    let fs = fills(&evs);

    assert_eq!(fs[0].price, Price(100)); // maker price (100 > 90)
    let taker_side = evs
        .iter()
        .find_map(|e| match e {
            Event::Fill { taker_side, .. } => Some(*taker_side),
            _ => None,
        })
        .unwrap();
    assert_eq!(taker_side, Side::Ask);
    assert!(book.bid_levels().is_empty());
}

/// Property: fill price is always the maker's resting price even when the
/// taker's limit is far more aggressive.
#[test]
fn fill_uses_maker_price_not_taker_price() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));

    let evs = book.submit_limit(bid(2, 500, 10)); // would pay up to 500
    assert_eq!(fills(&evs)[0].price, Price(100));
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

/// Property: cancel emits exactly `Cancel` then `Delta`, removes only that
/// order, and the delta reports the level's new aggregate quantity.
#[test]
fn cancel_front_order() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));
    book.submit_limit(ask(2, 100, 20));

    let evs = book.cancel(OrderId(1));
    assert_eq!(evs.len(), 2);
    assert!(matches!(evs[0], Event::Cancel { .. }));
    assert!(matches!(evs[1], Event::Delta { .. }));
    assert_eq!(deltas(&evs), vec![(Side::Ask, Price(100), 20)]);

    assert_eq!(book.order(OrderId(1)), None);
    assert!(!book.is_resting(OrderId(1)));
    assert_eq!(book.ask_levels()[&Price(100)].total_qty(), 20);
}

/// Property: cancelling a middle order leaves neighbours in their original
/// relative order (FIFO integrity preserved).
#[test]
fn cancel_middle_order_preserves_fifo_order() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));
    book.submit_limit(ask(2, 100, 10));
    book.submit_limit(ask(3, 100, 10));

    book.cancel(OrderId(2));
    let remaining: Vec<u64> = book.ask_levels()[&Price(100)]
        .iter()
        .map(|o| o.id.0)
        .collect();
    assert_eq!(remaining, vec![1, 3]);
    assert_eq!(book.ask_levels()[&Price(100)].total_qty(), 20);
}

/// Property: cancelling the last order at a level deletes the level and
/// emits a delta with qty 0.
#[test]
fn cancel_last_order_deletes_level_with_zero_delta() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));
    book.submit_limit(ask(2, 200, 10));

    let evs = book.cancel(OrderId(2));
    assert_eq!(deltas(&evs), vec![(Side::Ask, Price(200), 0)]);
    assert!(book.ask_levels().get(&Price(200)).is_none());
    assert_eq!(book.index_len(), 1);
}

/// Property: cancelling an unknown id is a rejected no-op that consumes
/// exactly one sequence number.
#[test]
fn unknown_cancel_is_rejected_noop() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));
    let before = book.seq();

    let evs = book.cancel(OrderId(999));
    assert_contiguous(&evs, before);
    assert_eq!(reject_reasons(&evs), vec![RejectReason::UnknownCancel]);
    assert_eq!(book.index_len(), 1);
    assert_eq!(book.total_resting_qty(), 10);
}

/// Property: double cancellation — the second attempt sees an unknown id
/// and is rejected without disturbing state.
#[test]
fn double_cancel_rejected_second_time() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));

    let first = book.cancel(OrderId(1));
    assert!(matches!(first[0], Event::Cancel { .. }));

    let second = book.cancel(OrderId(1));
    assert_eq!(reject_reasons(&second), vec![RejectReason::UnknownCancel]);
    assert!(book.ask_levels().is_empty());
    assert_eq!(book.index_len(), 0);
}

// ---------------------------------------------------------------------------
// Market orders
// ---------------------------------------------------------------------------

/// Property: a market order against an empty book executes nothing, never
/// rests, and reports the full quantity as dropped.
#[test]
fn market_order_against_empty_book() {
    let mut book = OrderBook::new();
    let evs = book.submit_market(OrderId(1), Side::Bid, 10);

    assert!(matches!(evs[0], Event::Ack { .. }));
    assert_eq!(reject_reasons(&evs), vec![RejectReason::MarketUnfilled]);
    assert!(fills(&evs).is_empty());
    assert_eq!(book.index_len(), 0);
    assert!(book.bid_levels().is_empty() && book.ask_levels().is_empty());
}

/// Property: a market order sweeps every level it can, never rests, and
/// drops only the genuinely unfilled remainder.
#[test]
fn market_sweeps_then_drops_remainder() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 5));
    book.submit_limit(ask(2, 101, 5));

    let evs = book.submit_market(OrderId(99), Side::Bid, 20);
    assert_eq!(
        fills(&evs).iter().map(|f| f.price.0).collect::<Vec<_>>(),
        vec![100, 101]
    );
    // 10 executed, 10 dropped.
    assert_eq!(reject_reasons(&evs), vec![RejectReason::MarketUnfilled]);
    assert!(book.ask_levels().is_empty());
    assert_eq!(book.index_len(), 0); // market never rests
}

/// Property: a market order that is fully filled emits no rejection.
#[test]
fn market_fully_filled_emits_no_reject() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));

    let evs = book.submit_market(OrderId(2), Side::Bid, 10);
    assert_eq!(reject_reasons(&evs), vec![]);
    assert_eq!(fills(&evs).len(), 1);
    assert!(matches!(evs.last().unwrap(), Event::Delta { .. }));
}

/// Property: market orders are validated like limits (qty 0, duplicates).
#[test]
fn market_order_validation() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));

    let zero = book.submit_market(OrderId(2), Side::Bid, 0);
    assert_eq!(reject_reasons(&zero), vec![RejectReason::ZeroQuantity]);

    let dup = book.submit_market(OrderId(1), Side::Bid, 5); // id 1 rests
    assert_eq!(reject_reasons(&dup), vec![RejectReason::DuplicateOrderId]);
    assert_eq!(book.index_len(), 1);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Property: a duplicate resting OrderId is rejected and changes nothing.
#[test]
fn duplicate_order_id_rejected() {
    let mut book = OrderBook::new();
    book.submit_limit(bid(1, 100, 10));

    let evs = book.submit_limit(bid(1, 105, 99));
    assert_eq!(reject_reasons(&evs), vec![RejectReason::DuplicateOrderId]);
    assert_eq!(book.index_len(), 1);
    assert_eq!(book.order(OrderId(1)).map(|o| o.qty), Some(10));
}

/// Property: price 0 is rejected for limit orders and still consumes a
/// sequence number (clients stay gap-free even on rejects).
#[test]
fn zero_price_rejected() {
    let mut book = OrderBook::new();
    let evs = book.submit_limit(bid(1, 0, 10));
    assert_eq!(reject_reasons(&evs), vec![RejectReason::ZeroPrice]);
    assert_eq!(book.index_len(), 0);
    assert_eq!(book.seq(), 1);
}

/// Property: quantity 0 is rejected for both limit and market orders.
#[test]
fn zero_quantity_rejected() {
    let mut book = OrderBook::new();
    assert_eq!(
        reject_reasons(&book.submit_limit(bid(1, 100, 0))),
        vec![RejectReason::ZeroQuantity]
    );
    assert_eq!(
        reject_reasons(&book.submit_market(OrderId(2), Side::Ask, 0)),
        vec![RejectReason::ZeroQuantity]
    );
    assert_eq!(book.index_len(), 0);
}

// ---------------------------------------------------------------------------
// Deltas, sequencing, index
// ---------------------------------------------------------------------------

/// Property: every delta carries the level's aggregate quantity *after* the
/// operation — the exact value a client sets its local view to.
#[test]
fn deltas_carry_correct_aggregate_quantities() {
    let mut book = OrderBook::new();

    let e1 = book.submit_limit(ask(1, 100, 10));
    let e2 = book.submit_limit(ask(2, 100, 20));
    assert_eq!(deltas(&e1), vec![(Side::Ask, Price(100), 10)]);
    assert_eq!(deltas(&e2), vec![(Side::Ask, Price(100), 30)]);

    // Fill 15: ask level drops to 15 (maker id1 fully + 5 of id2).
    let e3 = book.submit_limit(bid(3, 100, 15));
    assert_eq!(deltas(&e3), vec![(Side::Ask, Price(100), 15)]);

    // Cancel the surviving maker: level empties -> delta 0.
    let e4 = book.cancel(OrderId(2));
    assert_eq!(deltas(&e4), vec![(Side::Ask, Price(100), 0)]);

    assert!(book.ask_levels().is_empty());
}

/// Property: sequence numbers are globally contiguous across many distinct
/// commands — a client detects any dropped message via `seq != prev + 1`.
#[test]
fn seq_contiguous_across_commands() {
    let mut book = OrderBook::new();
    let mut all: Vec<u64> = Vec::new();

    let collect = |evs: Vec<Event>, all: &mut Vec<u64>| {
        all.extend(evs.iter().map(|e| e.seq()));
    };

    collect(book.submit_limit(ask(1, 100, 10)), &mut all);
    collect(book.submit_limit(bid(2, 99, 10)), &mut all);
    collect(book.submit_limit(bid(3, 100, 5)), &mut all);
    collect(book.cancel(OrderId(2)), &mut all);
    collect(book.submit_market(OrderId(4), Side::Ask, 5), &mut all);
    collect(book.submit_limit(bid(5, 0, 5)), &mut all); // reject
    collect(book.cancel(OrderId(77)), &mut all); // reject
    collect(book.submit_limit(bid(1, 98, 5)), &mut all); // duplicate id

    let expected: Vec<u64> = (1..=all.len() as u64).collect();
    assert_eq!(all, expected);
}

/// Property: the index contains exactly the resting orders — populated on
/// rest, purged on full fill and on cancel, takers never indexed, and every
/// entry resolves to the order's real (side, price).
#[test]
fn index_tracks_resting_orders_exactly() {
    let mut book = OrderBook::new();

    book.submit_limit(ask(1, 100, 10));
    book.submit_limit(bid(2, 90, 10));
    assert_eq!(book.index_len(), 2);

    // Taker that fully fills: never indexed, maker purged.
    book.submit_limit(bid(3, 100, 10));
    assert!(!book.is_resting(OrderId(3)));
    assert!(!book.is_resting(OrderId(1)));
    assert!(book.is_resting(OrderId(2)));
    assert_eq!(book.index_len(), 1);

    book.cancel(OrderId(2));
    assert_eq!(book.index_len(), 0);

    book.submit_limit(ask(10, 105, 7));
    let entries: Vec<_> = book.index_entries().collect();
    assert_eq!(entries, vec![(OrderId(10), Side::Ask, Price(105))]);
    let o = book.order(OrderId(10)).unwrap();
    assert_eq!((o.side, o.price), (Side::Ask, Price(105)));
}

/// Property: snapshots list best prices first on each side and honour the
/// depth limit.
#[test]
fn snapshot_best_first_and_depth_limited() {
    let mut book = OrderBook::new();
    for (i, p) in [(1u64, 100u64), (2, 99), (3, 98)] {
        book.submit_limit(bid(i, p, 10));
    }
    for (i, p) in [(4u64, 110u64), (5, 111), (6, 112)] {
        book.submit_limit(ask(i, p, 10));
    }

    let snap = book.snapshot(2);
    assert_eq!(snap.seq, book.seq());
    assert_eq!(snap.bids, vec![(Price(100), 10), (Price(99), 10)]);
    assert_eq!(snap.asks, vec![(Price(110), 10), (Price(111), 10)]);

    let full = book.snapshot(100);
    assert_eq!(full.bids.len(), 3);
    assert_eq!(full.asks.len(), 3);
}

/// Property: after any accepted command the book is never crossed.
#[test]
fn book_never_crossed_after_aggressive_orders() {
    let mut book = OrderBook::new();
    book.submit_limit(ask(1, 100, 10));
    book.submit_limit(bid(2, 95, 10));

    book.submit_limit(bid(3, 100, 3)); // crosses, partial
    if let (Some(b), Some(a)) = (book.best_bid(), book.best_ask()) {
        assert!(b < a);
    }

    book.submit_market(OrderId(4), Side::Ask, 4); // sweeps bid side
    if let (Some(b), Some(a)) = (book.best_bid(), book.best_ask()) {
        assert!(b < a);
    }
}
