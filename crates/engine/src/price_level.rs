//! `PriceLevel`: the FIFO queue of resting orders at a single price.
//!
//! # Why a `VecDeque`
//!
//! A price level has two hot operations:
//!
//! * **append** a new resting order (back of the queue),
//! * **consume** resting orders from the front (time priority).
//!
//! `VecDeque` does both in O(1). Within one price, priority is purely time:
//! the oldest order trades first. That is the "time" half of price-time
//! priority; the `BTreeMap` of levels in `OrderBook` supplies the "price"
//! half.
//!
//! # What this type deliberately does *not* do (v1)
//!
//! Cancellation is not specialized here: removing an arbitrary order from a
//! level is a linear scan (O(n) in the level's order count). That is
//! acceptable for v1 and will be measured before any redesign (slab +
//! intrusive list) is attempted.
//!
//! # Invariants
//!
//! 1. Orders are stored in arrival order (front = oldest).
//! 2. `total_qty` equals the sum of `qty` over all stored orders.
//! 3. No stored order has `qty == 0` (the engine rejects zero-quantity
//!    orders before insertion).

use std::collections::VecDeque;

use crate::types::{Fill, Order, OrderId};

/// Outcome of matching a taker against one level.
///
/// * `fills` — executed transfers, in execution order.
/// * `consumed` — maker ids that were **fully** filled and removed from the
///   level; the `OrderBook` uses this to purge them from its order index.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MatchResult {
    pub fills: Vec<Fill>,
    pub consumed: Vec<OrderId>,
}

impl MatchResult {
    /// Total quantity executed against this level.
    pub fn filled_qty(&self) -> u64 {
        self.fills.iter().map(|f| f.qty).sum()
    }
}

/// FIFO queue of orders resting at one price.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PriceLevel {
    orders: VecDeque<Order>,
    total_qty: u64,
}

impl PriceLevel {
    /// Creates an empty level.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an order to the back of the queue (time priority preserved).
    ///
    /// Complexity: O(1) amortized.
    ///
    /// The engine validates `qty > 0` before calling this; `debug_assert`
    /// catches violations in tests/debug builds without taxing release.
    pub fn insert(&mut self, order: Order) {
        debug_assert!(order.qty > 0, "zero-qty order must be rejected upstream");
        self.total_qty += order.qty;
        self.orders.push_back(order);
    }

    /// Matches `qty` against this level, consuming orders front-to-back.
    ///
    /// Each fill executes at the **maker's (resting) price** — the taker's
    /// limit price is not a parameter of this function, so matching at the
    /// wrong price is unrepresentable here.
    ///
    /// Also reports which makers were fully consumed so the caller can keep
    /// its order index in sync.
    ///
    /// Complexity: O(k) where k is the number of makers consumed.
    pub fn execute(&mut self, taker_id: OrderId, qty: u64) -> MatchResult {
        let mut result = MatchResult::default();
        let mut remaining = qty;

        while remaining > 0 {
            let Some(maker) = self.orders.front_mut() else {
                break; // liquidity exhausted
            };

            let fill_qty = remaining.min(maker.qty);
            if fill_qty == 0 {
                // Defensive: invariant 3 says this is unreachable. Drop the
                // degenerate order rather than spin forever.
                self.orders.pop_front();
                continue;
            }

            result.fills.push(Fill {
                maker_id: maker.id,
                taker_id,
                price: maker.price,
                qty: fill_qty,
            });

            maker.qty -= fill_qty;
            let exhausted = maker.qty == 0;
            if exhausted {
                result.consumed.push(maker.id);
            }

            self.total_qty -= fill_qty;
            remaining -= fill_qty;

            if exhausted {
                self.orders.pop_front();
            }
        }

        result
    }

    /// Matches `qty` and returns only the fills (Phase A API).
    ///
    /// Complexity: same as [`PriceLevel::execute`].
    pub fn match_order(&mut self, taker_id: OrderId, qty: u64) -> Vec<Fill> {
        self.execute(taker_id, qty).fills
    }

    /// Removes the first order with `id`, preserving the order of the rest.
    ///
    /// Returns the removed order, or `None` if this level does not contain
    /// it.
    ///
    /// Complexity: O(n) in the number of orders at this level (v1 design;
    /// see module docs).
    pub fn remove(&mut self, id: OrderId) -> Option<Order> {
        let pos = self.orders.iter().position(|o| o.id == id)?;
        let removed = self.orders.remove(pos)?;
        self.total_qty -= removed.qty;
        Some(removed)
    }

    /// Aggregate quantity resting at this level.
    ///
    /// Complexity: O(1) — maintained incrementally.
    #[inline]
    pub fn total_qty(&self) -> u64 {
        self.total_qty
    }

    /// Number of orders resting at this level.
    #[inline]
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// `true` when no orders rest at this level.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// Oldest resting order (the next maker to trade at this price).
    #[inline]
    pub fn front(&self) -> Option<&Order> {
        self.orders.front()
    }

    /// Iterates resting orders from oldest (front) to newest (back).
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Order> {
        self.orders.iter()
    }
}
