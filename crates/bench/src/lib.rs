//! Deterministic benchmark workloads shared by the Criterion benches and
//! the latency-histogram binary.
//!
//! Every generator is seeded (`ChaCha8Rng`) so runs are reproducible across
//! processes and machines. Prices are integer ticks around [`BASE_PRICE`].

pub mod calibrate;
pub mod harness;
pub mod profile_costs;
pub mod timer;
pub mod workload;

use engine::{Order, OrderBook, OrderId, Price, Side};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Anchor price; templates spread levels around it. Large enough that
/// ±100_000 levels of depth stay well inside `u64` and never hit price 0.
pub const BASE_PRICE: u64 = 1_000_000_000;

/// Default seed for every benchmark in this crate.
pub const DEFAULT_SEED: u64 = 42;

/// One operation of the mixed workload.
#[derive(Clone, Debug)]
pub enum Op {
    Limit(Order),
    Market { id: OrderId, side: Side, qty: u64 },
    Cancel { id: OrderId },
}

impl Op {
    pub fn apply(&self, book: &mut OrderBook) {
        match self {
            Op::Limit(o) => {
                book.submit_limit(o.clone());
            }
            Op::Market { id, side, qty } => {
                book.submit_market(*id, *side, *qty);
            }
            Op::Cancel { id } => {
                book.cancel(*id);
            }
        }
    }
}

/// Ask-side book: `depth` levels, `per_level` orders at each, ids `1..`.
/// Prices ascend from [`BASE_PRICE`] (all resting, nothing crosses).
pub fn ask_depth(depth: u64, per_level: u64) -> OrderBook {
    side_depth(Side::Ask, depth, per_level)
}

/// Bid-side book: `depth` levels, `per_level` orders at each, ids `1..`.
pub fn bid_depth(depth: u64, per_level: u64) -> OrderBook {
    side_depth(Side::Bid, depth, per_level)
}

fn side_depth(side: Side, depth: u64, per_level: u64) -> OrderBook {
    assert!(
        depth <= engine::MAX_BOOK_LEVELS_PER_SIDE as u64,
        "depth ({depth}) exceeds MAX_BOOK_LEVELS_PER_SIDE ({})",
        engine::MAX_BOOK_LEVELS_PER_SIDE
    );
    let mut book = OrderBook::new();
    let mut id = 1u64;
    for i in 0..depth {
        let price = match side {
            Side::Ask => BASE_PRICE + i,
            Side::Bid => BASE_PRICE.saturating_sub(1 + i),
        };
        for _ in 0..per_level {
            let events = book.submit_limit(Order {
                id: OrderId(id),
                side,
                price: Price(price),
                qty: 10,
            });
            assert!(
                matches!(
                    events.as_slice(),
                    [engine::Event::Ack { .. }, engine::Event::Delta { .. }]
                ),
                "expected Ack + Delta during template setup, got {events:?}"
            );
            id += 1;
        }
    }
    match side {
        Side::Ask => assert_eq!(book.ask_levels().len(), depth as usize),
        Side::Bid => assert_eq!(book.bid_levels().len(), depth as usize),
    }
    assert_eq!(book.index_len(), (depth * per_level) as usize);
    book
}

/// Two-sided book for mixed workloads: `depth` ask levels above the base and
/// `depth` bid levels below it, `per_level` orders each.
/// Bid ids run after ask ids so ids stay unique.
pub fn two_sided(depth: u64, per_level: u64) -> OrderBook {
    assert!(
        depth <= engine::MAX_BOOK_LEVELS_PER_SIDE as u64,
        "depth ({depth}) exceeds MAX_BOOK_LEVELS_PER_SIDE ({})",
        engine::MAX_BOOK_LEVELS_PER_SIDE
    );
    let mut book = OrderBook::new();
    let mut id = 1u64;
    for i in 0..depth {
        for _ in 0..per_level {
            let events = book.submit_limit(Order {
                id: OrderId(id),
                side: Side::Ask,
                price: Price(BASE_PRICE + i),
                qty: 10,
            });
            assert!(
                matches!(
                    events.as_slice(),
                    [engine::Event::Ack { .. }, engine::Event::Delta { .. }]
                ),
                "expected Ack + Delta during template setup, got {events:?}"
            );
            id += 1;
        }
    }
    for i in 0..depth {
        for _ in 0..per_level {
            let events = book.submit_limit(Order {
                id: OrderId(id),
                side: Side::Bid,
                price: Price(BASE_PRICE.saturating_sub(1 + i)),
                qty: 10,
            });
            assert!(
                matches!(
                    events.as_slice(),
                    [engine::Event::Ack { .. }, engine::Event::Delta { .. }]
                ),
                "expected Ack + Delta during template setup, got {events:?}"
            );
            id += 1;
        }
    }
    assert_eq!(book.ask_levels().len(), depth as usize);
    assert_eq!(book.bid_levels().len(), depth as usize);
    assert_eq!(book.index_len(), (depth * per_level * 2) as usize);
    book
}

/// Total resting quantity on the ask side of `book` (liquidity to sweep).
pub fn ask_qty(book: &OrderBook) -> u64 {
    book.ask_levels().values().map(|l| l.total_qty()).sum()
}

/// Generates `len` mixed operations (default mix ~60% limit / 30% cancel /
/// 10% market) from a fixed seed.
///
/// * limit orders are **non-crossing** w.r.t. the two-sided template at
///   [`BASE_PRICE`] depth: bids below, asks above the template range.
/// * cancels target ids that exist in the template (some will already be
///   consumed/cancelled by earlier script ops — the engine rejects those,
///   which is part of the workload).
/// * market orders are small and may execute against the template.
pub fn mixed_script(len: usize, depth: u64, per_level: u64, seed: u64) -> Vec<Op> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let template_ids = depth * per_level * 2; // ask + bid ids
    let mut next_id = template_ids + 1;
    let mut ops = Vec::with_capacity(len);

    for _ in 0..len {
        let roll = rng.random_range(0u64..100);
        if roll < 60 {
            let side = if rng.random_bool(0.5) {
                Side::Bid
            } else {
                Side::Ask
            };
            let price = match side {
                Side::Ask => BASE_PRICE + depth + rng.random_range(0..100),
                Side::Bid => BASE_PRICE.saturating_sub(1 + depth + rng.random_range(0..100)),
            };
            ops.push(Op::Limit(Order {
                id: OrderId(next_id),
                side,
                price: Price(price),
                qty: rng.random_range(1..20),
            }));
            next_id += 1;
        } else if roll < 90 {
            let id = rng.random_range(1..=template_ids);
            ops.push(Op::Cancel { id: OrderId(id) });
        } else {
            let id = next_id;
            next_id += 1;
            let side = if rng.random_bool(0.5) {
                Side::Bid
            } else {
                Side::Ask
            };
            ops.push(Op::Market {
                id: OrderId(id),
                side,
                qty: rng.random_range(1..50),
            });
        }
    }
    ops
}
