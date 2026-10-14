//! Property tests for `OrderBook` — 10,000 generated command sequences.
//!
//! Invariants verified on every sequence:
//!
//! 1. **Book never crossed** — `best_bid < best_ask` when both sides exist.
//! 2. **Quantity conservation** — submitted = filled + cancelled + dropped + resting.
//! 3. **Index correctness** — index holds exactly the resting orders, each
//!    resolving to its true (side, price); no empty levels exist.
//! 4. **Determinism** — two independent books fed the same commands produce
//!    identical event sequences and identical final state.
//!
//! Plus: per-command sequence contiguity (foundation of client gap detection).

use proptest::prelude::*;

use engine::{Event, Order, OrderBook, OrderId, Price, RejectReason, Side};

#[derive(Clone, Debug)]
enum Cmd {
    Limit {
        id: u64,
        side: Side,
        price: u64,
        qty: u64,
    },
    Market {
        id: u64,
        side: Side,
        qty: u64,
    },
    Cancel {
        id: u64,
    },
}

/// Small id pool (0..48) deliberately causes duplicate-id collisions with
/// resting orders — the rejection path must hold invariants too.
/// Prices cluster around 100 so books actually cross.
fn cmd_strategy() -> impl Strategy<Value = Cmd> {
    let id = 0u64..48;
    let side = prop_oneof![Just(Side::Bid), Just(Side::Ask)];
    prop_oneof![
        6 => (id.clone(), side.clone(), 95u64..=105, 1u64..=25)
                .prop_map(|(id, side, price, qty)| Cmd::Limit { id, side, price, qty }),
        3 => (id.clone(), side.clone(), 1u64..=25)
                .prop_map(|(id, side, qty)| Cmd::Market { id, side, qty }),
        1 => id.prop_map(|id| Cmd::Cancel { id }),
    ]
}

/// Accounting ledger — an independent bookkeeping of where every unit of
/// submitted quantity went. Deliberately *not* computed from the engine's
/// resting state (except the final `resting` term) so a systematic engine
/// bug cannot cancel itself out.
///
/// Conservation: a fill of `q` removes `q` from a maker order *and* `q`
/// from the taker order, so every submitted unit ends up in exactly one of
/// `2·fills` (maker side + taker side), `cancelled`, `dropped`, `resting`.
#[derive(Default, Debug)]
struct Ledger {
    submitted: u64,
    /// Sum of fill quantities (each unit hits maker *and* taker books).
    fills: u64,
    cancelled: u64,
    dropped: u64,
}

fn fill_sum(events: &[Event]) -> u64 {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Fill { fill, .. } => Some(fill.qty),
            _ => None,
        })
        .sum()
}

fn was_ack(events: &[Event]) -> bool {
    matches!(events.first(), Some(Event::Ack { .. }))
}

fn assert_not_crossed(book: &OrderBook) {
    if let (Some(b), Some(a)) = (book.best_bid(), book.best_ask()) {
        assert!(
            b < a,
            "book crossed: best_bid={b:?} >= best_ask={a:?}; snapshot={:?}",
            book.snapshot(10)
        );
    }
}

fn assert_index_consistent(book: &OrderBook) {
    let mut in_levels = 0usize;
    for (price, level) in book.bid_levels().iter().chain(book.ask_levels()) {
        assert!(!level.is_empty(), "empty level stored at {price:?}");
        assert!(price.0 > 0, "zero price level stored");
        assert_eq!(level.total_qty(), level.iter().map(|o| o.qty).sum::<u64>());
        in_levels += level.len();
    }
    assert_eq!(
        book.index_len(),
        in_levels,
        "index size != orders in levels"
    );
    for (id, side, price) in book.index_entries() {
        let order = book
            .order(id)
            .unwrap_or_else(|| panic!("index entry {id:?} does not resolve"));
        assert_eq!((order.side, order.price), (side, price));
    }
}

fn run_sequence(cmds: &[Cmd]) {
    let mut book = OrderBook::new();
    let mut twin = OrderBook::new(); // invariant 4: determinism
    let mut ledger = Ledger::default();

    for (i, cmd) in cmds.iter().enumerate() {
        let before = book.seq();
        // Quantity resting *before* the command — needed to credit cancels.
        let pre_cancel_qty = match cmd {
            Cmd::Cancel { id } => book.order(OrderId(*id)).map(|o| o.qty),
            _ => None,
        };
        let events = match cmd {
            Cmd::Limit {
                id,
                side,
                price,
                qty,
            } => book.submit_limit(Order {
                id: OrderId(*id),
                side: *side,
                price: Price(*price),
                qty: *qty,
            }),
            Cmd::Market { id, side, qty } => book.submit_market(OrderId(*id), *side, *qty),
            Cmd::Cancel { id } => book.cancel(OrderId(*id)),
        };

        // Sequence contiguity for this command.
        for (j, ev) in events.iter().enumerate() {
            assert_eq!(
                ev.seq(),
                before + 1 + j as u64,
                "cmd {i} ({cmd:?}) event {j} out of order: {ev:?}"
            );
        }

        // Ledger accounting.
        match cmd {
            Cmd::Limit { qty, .. } => {
                if was_ack(&events) {
                    ledger.submitted += *qty;
                    ledger.fills += fill_sum(&events);
                } else {
                    assert!(events.iter().any(|e| matches!(e, Event::Reject { .. })));
                }
            }
            Cmd::Market { qty, .. } => {
                if was_ack(&events) {
                    ledger.submitted += *qty;
                    let executed = fill_sum(&events);
                    ledger.fills += executed;
                    ledger.dropped += qty - executed;
                    // Any dropped remainder must be explicitly reported.
                    let reported = events.iter().any(|e| {
                        matches!(
                            e,
                            Event::Reject {
                                reason: RejectReason::MarketUnfilled,
                                ..
                            }
                        )
                    });
                    assert_eq!(
                        reported,
                        executed < *qty,
                        "dropped remainder must be reported"
                    );
                }
            }
            Cmd::Cancel { .. } => {
                if matches!(events.first(), Some(Event::Cancel { .. })) {
                    ledger.cancelled += pre_cancel_qty.expect("cancelling a resting order");
                }
            }
        }

        // Determinism: same command, same response.
        let twin_events = match cmd {
            Cmd::Limit {
                id,
                side,
                price,
                qty,
            } => twin.submit_limit(Order {
                id: OrderId(*id),
                side: *side,
                price: Price(*price),
                qty: *qty,
            }),
            Cmd::Market { id, side, qty } => twin.submit_market(OrderId(*id), *side, *qty),
            Cmd::Cancel { id } => twin.cancel(OrderId(*id)),
        };
        assert_eq!(
            events, twin_events,
            "cmd {i} ({cmd:?}) diverged between twins"
        );

        // Invariants 1 and 3 after every command.
        assert_not_crossed(&book);
        assert_index_consistent(&book);
    }

    // Invariant 4: identical final state.
    assert_eq!(book.snapshot(10_000), twin.snapshot(10_000));
    let mut left = book.index_entries().collect::<Vec<_>>();
    let mut right = twin.index_entries().collect::<Vec<_>>();
    // HashMap iteration order differs per instance — order-independent compare.
    left.sort_unstable();
    right.sort_unstable();
    assert_eq!(left, right);
    assert_eq!(book.seq(), twin.seq());

    // Invariant 2: quantity conservation.
    // A fill of q removes q from a maker order *and* q from the taker order,
    // so every unit submitted ends up in exactly one of:
    //   maker-side fill, taker-side fill, cancelled, dropped, resting.
    let resting = book.total_resting_qty();
    assert_eq!(
        ledger.submitted,
        2 * ledger.fills + ledger.cancelled + ledger.dropped + resting,
        "conservation violated: {ledger:?}, resting={resting}, book={:?}",
        book.snapshot(20)
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 10_000, ..Default::default() })]

    /// All four invariants across 10,000 randomized command sequences.
    #[test]
    fn orderbook_invariants(cmds in prop::collection::vec(cmd_strategy(), 1..100)) {
        run_sequence(&cmds);
    }
}
