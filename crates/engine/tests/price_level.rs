//! Phase A tests for `PriceLevel`.
//!
//! Each test documents the correctness property it protects.

use engine::{Fill, Order, OrderId, Price, PriceLevel, Side};

/// Helper: a bid at price 100 (price is irrelevant *within* one level, but
/// fills must echo it back, which the maker-price tests assert).
fn order(id: u64, qty: u64) -> Order {
    Order {
        id: OrderId(id),
        side: Side::Bid,
        price: Price(100),
        qty,
    }
}

fn fill_qty(fills: &[Fill]) -> u64 {
    fills.iter().map(|f| f.qty).sum()
}

/// Property: matching consumes the queue strictly front-to-back, a partially
/// filled maker *stays* at the front, and the remainder keeps its queue
/// position.
///
/// A(10), B(10), match 15 ⇒ Fill A 10, Fill B 5; B remains front with qty 5.
#[test]
fn partial_fifo_fill_keeps_remainder_at_front() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 10)); // A
    level.insert(order(2, 10)); // B

    let fills = level.match_order(OrderId(99), 15);

    assert_eq!(
        fills,
        vec![
            Fill {
                maker_id: OrderId(1),
                taker_id: OrderId(99),
                price: Price(100),
                qty: 10,
            },
            Fill {
                maker_id: OrderId(2),
                taker_id: OrderId(99),
                price: Price(100),
                qty: 5,
            },
        ]
    );

    // B survived with 5 and is now the front (A was fully consumed).
    let front = level.front().expect("B must still rest");
    assert_eq!(front.id, OrderId(2));
    assert_eq!(front.qty, 5);
    assert_eq!(level.len(), 1);
    assert_eq!(level.total_qty(), 5);
}

/// Property: a taker whose quantity exactly drains the level empties it —
/// no phantom orders, `total_qty` returns to zero.
#[test]
fn exact_fill_empties_level() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 10));
    level.insert(order(2, 10));

    let fills = level.match_order(OrderId(7), 20);

    assert_eq!(fill_qty(&fills), 20);
    assert_eq!(fills.len(), 2);
    assert!(level.is_empty());
    assert_eq!(level.total_qty(), 0);
    assert_eq!(level.len(), 0);
    assert!(level.front().is_none());
}

/// Property: a taker larger than the level drains the level completely and
/// the returned fills sum to exactly the available liquidity — the caller
/// sees the unfilled remainder by subtraction.
#[test]
fn match_quantity_larger_than_entire_level() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 4));
    level.insert(order(2, 6));

    let fills = level.match_order(OrderId(7), 1_000);

    assert_eq!(fill_qty(&fills), 10); // only available qty
    assert!(level.is_empty());
    assert_eq!(level.total_qty(), 0);
}

/// Property: matching against an empty level is a no-op (no panic, no
/// phantom fills) — required for market orders sweeping an empty book.
#[test]
fn match_against_empty_level_returns_no_fills() {
    let mut level = PriceLevel::new();
    assert!(level.match_order(OrderId(1), 10).is_empty());

    level.insert(order(1, 10));
    level.match_order(OrderId(2), 10);
    assert!(level.is_empty());

    let fills = level.match_order(OrderId(3), 5);
    assert!(fills.is_empty());
}

/// Property: `total_qty` is the sum of resting quantities after *every*
/// mutation — insert, partial match, full match.
#[test]
fn total_qty_is_correct_after_each_mutation() {
    let mut level = PriceLevel::new();
    assert_eq!(level.total_qty(), 0);

    level.insert(order(1, 10));
    assert_eq!(level.total_qty(), 10);

    level.insert(order(2, 20));
    assert_eq!(level.total_qty(), 30);

    level.match_order(OrderId(9), 12); // 10 from #1, 2 from #2
    assert_eq!(level.total_qty(), 18);

    level.match_order(OrderId(9), 18);
    assert_eq!(level.total_qty(), 0);
    assert!(level.is_empty());
}

/// Property: arrival order is preserved; a single large taker executes
/// against makers in insertion order (time priority), never reordered.
#[test]
fn insertion_order_is_preserved_and_matched_fifo() {
    let mut level = PriceLevel::new();
    for id in 1..=5 {
        level.insert(order(id, 10));
    }

    // Consumption order must be 1,2,3 — never 4,5 first, never reordered.
    let fills = level.match_order(OrderId(99), 25);
    let maker_order: Vec<u64> = fills.iter().map(|f| f.maker_id.0).collect();
    assert_eq!(maker_order, vec![1, 2, 3]);
    assert_eq!(fill_qty(&fills), 25);

    // Remaining queue: 3 partially filled at the front, then 4, then 5.
    let rest: Vec<(u64, u64)> = level.iter().map(|o| (o.id.0, o.qty)).collect();
    assert_eq!(rest, vec![(3, 5), (4, 10), (5, 10)]);
}

/// Property: a level holds a single price, and every fill executes at that
/// resting price — the maker price rule.
#[test]
fn fills_use_maker_price() {
    let maker_price = Price(9_999);
    let mut level = PriceLevel::new();
    level.insert(Order {
        id: OrderId(1),
        side: Side::Ask,
        price: maker_price,
        qty: 10,
    });

    let fills = level.match_order(OrderId(2), 10);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price, maker_price);
    assert_eq!(fills[0].maker_id, OrderId(1));
    assert_eq!(fills[0].taker_id, OrderId(2));
}

/// Property: a zero-quantity match request produces no fills and does not
/// mutate the level (defensive; the engine also rejects qty == 0 upstream).
#[test]
fn zero_quantity_match_is_a_noop() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 10));

    assert!(level.match_order(OrderId(2), 0).is_empty());
    assert_eq!(level.total_qty(), 10);
    assert_eq!(level.len(), 1);
}

/// Property: removing the front order keeps the rest in order and updates
/// `total_qty`.
#[test]
fn remove_front_order() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 10));
    level.insert(order(2, 20));

    let removed = level.remove(OrderId(1)).expect("front order exists");
    assert_eq!(removed.id, OrderId(1));
    assert_eq!(level.total_qty(), 20);
    assert_eq!(level.front().map(|o| o.id), Some(OrderId(2)));
}

/// Property: removing a middle order leaves its neighbours untouched and in
/// their original relative order.
#[test]
fn remove_middle_order_preserves_neighbours() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 10));
    level.insert(order(2, 20));
    level.insert(order(3, 30));

    assert!(level.remove(OrderId(2)).is_some());

    let rest: Vec<u64> = level.iter().map(|o| o.id.0).collect();
    assert_eq!(rest, vec![1, 3]);
    assert_eq!(level.total_qty(), 40);
    assert_eq!(level.len(), 2);
}

/// Property: removing the last order leaves a truly empty level with zero
/// quantity (callers must be able to drop empty levels safely).
#[test]
fn remove_last_order_empties_level() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 10));

    assert!(level.remove(OrderId(1)).is_some());
    assert!(level.is_empty());
    assert_eq!(level.total_qty(), 0);
    assert!(level.front().is_none());
    assert!(level.remove(OrderId(1)).is_none()); // double remove is harmless
}

/// Property: removing an unknown id changes nothing (the OrderBook layer
/// turns this into a `Reject` event in Phase B).
#[test]
fn remove_unknown_order_is_none_and_does_not_mutate() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 10));
    level.insert(order(2, 10));

    assert!(level.remove(OrderId(42)).is_none());
    assert_eq!(level.total_qty(), 20);
    assert_eq!(level.len(), 2);
}

/// Property: `total_qty` always equals the sum of the individual order
/// quantities — checked after a randomized-looking mix of operations.
#[test]
fn total_qty_matches_sum_of_orders_through_mixed_ops() {
    let mut level = PriceLevel::new();
    level.insert(order(1, 5));
    level.insert(order(2, 7));
    level.insert(order(3, 11));
    level.insert(order(4, 13));

    let assert_consistent = |level: &PriceLevel| {
        let summed: u64 = level.iter().map(|o| o.qty).sum();
        assert_eq!(level.total_qty(), summed);
        assert_eq!(level.len(), level.iter().count());
    };

    assert_consistent(&level);

    level.match_order(OrderId(50), 6); // partial across #1 and #2
    assert_consistent(&level);

    level.remove(OrderId(3));
    assert_consistent(&level);

    level.match_order(OrderId(51), 100); // drain everything left
    assert_consistent(&level);
    assert!(level.is_empty());
}
