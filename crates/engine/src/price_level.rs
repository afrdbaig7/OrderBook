//! FIFO queue of resting orders at a single price.
//!
//! Appends go to the back (new arrivals), matches consume from the front
//! (oldest first). Cancellation is an O(n) scan — acceptable at typical
//! level depths and simple to reason about.

use std::collections::VecDeque;

use crate::types::{Fill, Order, OrderId};

/// Result of matching a taker against one level.
/// `consumed` lists maker ids that were fully filled so the book can
/// remove them from its index.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MatchResult {
    pub fills: Vec<Fill>,
    pub consumed: Vec<OrderId>,
}

impl MatchResult {
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an order to the back of the queue, checking for arithmetic overflow and safe limits.
    pub fn try_insert(&mut self, order: Order) -> Result<(), &'static str> {
        debug_assert!(order.qty > 0, "zero-qty order must be rejected upstream");
        let new_total = self
            .total_qty
            .checked_add(order.qty)
            .ok_or("arithmetic overflow in price level quantity")?;
        if new_total > crate::types::MAX_LEVEL_QTY {
            return Err("aggregate price level quantity exceeds maximum safe limit");
        }
        self.total_qty = new_total;
        self.orders.push_back(order);
        Ok(())
    }

    /// Appends an order to the back of the queue. Panics on arithmetic overflow.
    pub fn insert(&mut self, order: Order) {
        self.try_insert(order)
            .expect("price level insert exceeded numeric capacity");
    }

    /// Matches `qty` against this level front-to-back.
    /// Fills execute at each maker's resting price, never the taker's.
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

    pub fn match_order(&mut self, taker_id: OrderId, qty: u64) -> Vec<Fill> {
        self.execute(taker_id, qty).fills
    }

    /// Removes the order with `id` from this level, or returns `None`.
    pub fn remove(&mut self, id: OrderId) -> Option<Order> {
        let pos = self.orders.iter().position(|o| o.id == id)?;
        let removed = self.orders.remove(pos)?;
        self.total_qty -= removed.qty;
        Some(removed)
    }

    #[inline]
    pub fn total_qty(&self) -> u64 {
        self.total_qty
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    #[inline]
    pub fn front(&self) -> Option<&Order> {
        self.orders.front()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Order> {
        self.orders.iter()
    }
}
