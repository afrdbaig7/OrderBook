//! High-performance experimental order book with zero heap allocations on the hot path.
//!
//! Enforces:
//! - `#![forbid(unsafe_code)]`
//! - Fixed-depth dense tick array with $\mathcal{O}(1)$ bitmap best-price discovery
//! - Preallocated 32-bit index order arena with intrusive doubly-linked queues
//! - Preallocated order ID open-addressing index with backward-shift deletion
//! - Reusable caller-owned event buffers guaranteeing zero allocations during matching

#![forbid(unsafe_code)]

pub mod arena;
pub mod id_index;
pub mod levels;

use arena::{OrderArena, SENTINEL};
use id_index::OrderIdIndex;
use levels::PriceLadder;

use crate::book::Snapshot;
use crate::types::{
    Event, Fill, Order, OrderId, Price, RejectReason, Side, MAX_LEVEL_QTY, MAX_ORDER_ID,
    MAX_ORDER_QTY, MAX_PRICE,
};

/// Configuration for [`FlatBook`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlatBookConfig {
    /// Lowest price tick represented by index 0.
    pub base_price: u64,
    /// Total price ticks supported in the contiguous active window.
    pub tick_count: usize,
    /// Maximum number of simultaneously resting orders.
    pub max_orders: usize,
}

impl Default for FlatBookConfig {
    fn default() -> Self {
        Self {
            base_price: 1_000_000_000,
            tick_count: 512,
            max_orders: 4_096,
        }
    }
}

/// Errors returned by [`FlatBook`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FlatError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),

    #[error("output buffer too small: required {required}, available {available}")]
    OutputBufferTooSmall { required: usize, available: usize },

    #[error("price {price:?} is outside active window [{base_price}, {max_price})")]
    PriceOutOfWindow {
        price: Price,
        base_price: u64,
        max_price: u64,
    },

    #[error("arena capacity exhausted ({0} orders)")]
    ArenaFull(usize),

    #[error("id index capacity exhausted ({0} entries)")]
    IndexFull(usize),
}

/// Experimental L1-cache friendly order book.
#[derive(Clone, Debug)]
pub struct FlatBook {
    config: FlatBookConfig,
    arena: OrderArena,
    id_index: OrderIdIndex,
    ladder: PriceLadder,
    next_seq: u64,
}

impl FlatBook {
    /// Creates a new `FlatBook` with declared capacities.
    pub fn new(config: FlatBookConfig) -> Result<Self, FlatError> {
        if config.tick_count == 0 || config.tick_count > 4096 {
            return Err(FlatError::InvalidConfig("tick_count must be in 1..=4096"));
        }
        if config.max_orders == 0 {
            return Err(FlatError::InvalidConfig("max_orders must be > 0"));
        }
        if config.max_orders > (u32::MAX - 1) as usize {
            return Err(FlatError::InvalidConfig(
                "max_orders exceeds u32 index capacity",
            ));
        }
        let index_cap = match config.max_orders.checked_mul(2) {
            Some(c) => c,
            None => return Err(FlatError::InvalidConfig("max_orders capacity overflow")),
        };
        if index_cap.checked_next_power_of_two().is_none() {
            return Err(FlatError::InvalidConfig(
                "id index power-of-two capacity overflow",
            ));
        }
        if config.base_price == 0 || config.base_price > MAX_PRICE {
            return Err(FlatError::InvalidConfig("invalid base_price"));
        }
        let max_tick_price = config.base_price.checked_add(config.tick_count as u64 - 1);
        match max_tick_price {
            Some(p) if p <= MAX_PRICE => {}
            _ => return Err(FlatError::InvalidConfig("price window exceeds MAX_PRICE")),
        }

        let arena = OrderArena::new(config.max_orders);
        let id_index = OrderIdIndex::new(config.max_orders * 2);
        let ladder = PriceLadder::new(config.base_price, config.tick_count);

        Ok(Self {
            config,
            arena,
            id_index,
            ladder,
            next_seq: 1,
        })
    }

    /// Worst-case event buffer capacity required to guarantee zero allocations.
    #[inline]
    pub fn worst_case_event_capacity(&self) -> usize {
        self.config.max_orders + self.config.tick_count + 8
    }

    #[inline]
    pub fn seq(&self) -> u64 {
        self.next_seq - 1
    }

    #[inline]
    fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Highest bid price, if any.
    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.ladder
            .bids
            .best_bid_tick()
            .map(|t| self.ladder.tick_to_price(t))
    }

    /// Lowest ask price, if any.
    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.ladder
            .asks
            .best_ask_tick()
            .map(|t| self.ladder.tick_to_price(t))
    }

    #[inline]
    pub fn resting_order_count(&self) -> usize {
        self.arena.len()
    }

    /// Checks if a caller-owned event buffer has sufficient capacity.
    #[inline(always)]
    fn ensure_event_capacity(&self, events: &Vec<Event>) -> Result<(), FlatError> {
        let required = self.worst_case_event_capacity();
        if events.capacity() < required {
            return Err(FlatError::OutputBufferTooSmall {
                required,
                available: events.capacity(),
            });
        }
        Ok(())
    }

    /// Submits a limit order into caller-owned `events` buffer.
    pub fn submit_limit_into(
        &mut self,
        order: Order,
        events: &mut Vec<Event>,
    ) -> Result<(), FlatError> {
        self.ensure_event_capacity(events)?;
        events.clear();

        // 1. Validation before mutation (order matches OrderBook rejection priority)
        if order.id.0 > MAX_ORDER_ID {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id: order.id,
                reason: RejectReason::OrderIdOutOfRange,
            });
            return Ok(());
        }
        if order.qty == 0 {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id: order.id,
                reason: RejectReason::ZeroQuantity,
            });
            return Ok(());
        }
        if order.qty > MAX_ORDER_QTY {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id: order.id,
                reason: RejectReason::QuantityOutOfRange,
            });
            return Ok(());
        }
        if order.price.0 == 0 {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id: order.id,
                reason: RejectReason::ZeroPrice,
            });
            return Ok(());
        }
        if order.price.0 > MAX_PRICE {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id: order.id,
                reason: RejectReason::PriceOutOfRange,
            });
            return Ok(());
        }
        let entry = self.id_index.find_entry(order.id);
        let (vacant_bucket, vacant_version) = match entry {
            id_index::EntrySlot::Occupied => {
                events.push(Event::Reject {
                    seq: self.take_seq(),
                    id: order.id,
                    reason: RejectReason::DuplicateOrderId,
                });
                return Ok(());
            }
            id_index::EntrySlot::Full => {
                return Err(FlatError::IndexFull(self.id_index.capacity()));
            }
            id_index::EntrySlot::Vacant {
                bucket_idx,
                version,
            } => (bucket_idx, version),
        };

        // Explicit price window domain boundary check
        let tick_idx = match self.ladder.price_to_tick(order.price) {
            Some(idx) => idx,
            None => {
                return Err(FlatError::PriceOutOfWindow {
                    price: order.price,
                    base_price: self.config.base_price,
                    max_price: self.config.base_price + self.config.tick_count as u64,
                });
            }
        };

        // 2. Pre-check crossing against opposing book
        let maker_side = order.side.opposite();
        let best_opposing = self.ladder.best_tick(maker_side);
        let crosses = match (best_opposing, order.side) {
            (Some(best_tick), Side::Bid) => tick_idx >= best_tick,
            (Some(best_tick), Side::Ask) => tick_idx <= best_tick,
            _ => false,
        };

        // If the order does not cross and the arena cannot accept resting orders, return explicit error
        if !crosses && self.arena.len() >= self.config.max_orders {
            return Err(FlatError::ArenaFull(self.config.max_orders));
        }

        // Acknowledge order
        events.push(Event::Ack {
            seq: self.take_seq(),
            id: order.id,
        });

        // 3. Matching against opposing side
        let mut remaining = order.qty;

        while remaining > 0 {
            let best_tick = self.ladder.best_tick(maker_side);
            let Some(maker_tick) = best_tick else {
                break; // No opposing liquidity
            };

            let maker_price = self.ladder.tick_to_price(maker_tick);
            let crosses_level = match order.side {
                Side::Bid => order.price >= maker_price,
                Side::Ask => order.price <= maker_price,
            };

            if !crosses_level {
                break; // Limit price does not cross
            }

            let consumed = Self::consume_level(
                &mut self.ladder,
                &mut self.arena,
                &mut self.id_index,
                &mut self.next_seq,
                maker_side,
                maker_tick,
                order.id,
                remaining,
                events,
            );
            remaining -= consumed;
        }

        // 4. Rest remainder
        if remaining > 0 {
            if self.arena.len() >= self.config.max_orders {
                return Err(FlatError::ArenaFull(self.config.max_orders));
            }

            let current_total = self
                .ladder
                .side(order.side)
                .get_level(tick_idx)
                .map_or(0, |l| l.total_qty);

            if current_total
                .checked_add(remaining)
                .is_none_or(|q| q > MAX_LEVEL_QTY)
            {
                events.push(Event::Reject {
                    seq: self.take_seq(),
                    id: order.id,
                    reason: RejectReason::LevelQuantityOverflow,
                });
                return Ok(());
            }

            let slot_idx = self
                .arena
                .allocate(order.id, order.side, tick_idx as u32, remaining)
                .expect("arena not full");

            let new_total = self.ladder.side_mut(order.side).enqueue_order(
                tick_idx,
                slot_idx,
                remaining,
                &mut self.arena,
            );

            if !crosses {
                self.id_index
                    .insert_at_vacant(vacant_bucket, vacant_version, order.id, slot_idx)
                    .expect("id index capacity");
            } else {
                self.id_index
                    .insert(order.id, slot_idx)
                    .expect("id index capacity");
            }

            events.push(Event::Delta {
                seq: self.take_seq(),
                side: order.side,
                price: order.price,
                new_total_qty: new_total,
            });
        }

        Ok(())
    }

    /// Submits a market order into caller-owned `events` buffer.
    pub fn submit_market_into(
        &mut self,
        id: OrderId,
        side: Side,
        qty: u64,
        events: &mut Vec<Event>,
    ) -> Result<(), FlatError> {
        self.ensure_event_capacity(events)?;
        events.clear();

        if id.0 > MAX_ORDER_ID {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::OrderIdOutOfRange,
            });
            return Ok(());
        }
        if qty == 0 {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::ZeroQuantity,
            });
            return Ok(());
        }
        if qty > MAX_ORDER_QTY {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::QuantityOutOfRange,
            });
            return Ok(());
        }
        if self.id_index.contains(id) {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::DuplicateOrderId,
            });
            return Ok(());
        }

        events.push(Event::Ack {
            seq: self.take_seq(),
            id,
        });

        let maker_side = side.opposite();
        let mut remaining = qty;

        while remaining > 0 {
            let best_tick = match maker_side {
                Side::Bid => self.ladder.bids.best_bid_tick(),
                Side::Ask => self.ladder.asks.best_ask_tick(),
            };

            let Some(maker_tick) = best_tick else {
                break;
            };

            let consumed = Self::consume_level(
                &mut self.ladder,
                &mut self.arena,
                &mut self.id_index,
                &mut self.next_seq,
                maker_side,
                maker_tick,
                id,
                remaining,
                events,
            );
            remaining -= consumed;
        }

        if remaining > 0 {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::MarketUnfilled,
            });
        }

        Ok(())
    }

    /// Cancels a resting order into caller-owned `events` buffer.
    pub fn cancel_into(&mut self, id: OrderId, events: &mut Vec<Event>) -> Result<(), FlatError> {
        self.ensure_event_capacity(events)?;
        events.clear();

        if id.0 > MAX_ORDER_ID {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::OrderIdOutOfRange,
            });
            return Ok(());
        }

        let slot_idx = match self.id_index.remove(id) {
            Some(idx) => idx,
            None => {
                events.push(Event::Reject {
                    seq: self.take_seq(),
                    id,
                    reason: RejectReason::UnknownCancel,
                });
                return Ok(());
            }
        };

        let slot = *self.arena.get(slot_idx).expect("valid slot");
        let tick_idx = slot.price_tick as usize;
        let side = slot.side;
        let price = self.ladder.tick_to_price(tick_idx);

        let new_total =
            self.ladder
                .side_mut(side)
                .remove_order(tick_idx, slot_idx, &mut self.arena);

        self.arena.free(slot_idx);

        events.push(Event::Cancel {
            seq: self.take_seq(),
            id,
        });
        events.push(Event::Delta {
            seq: self.take_seq(),
            side,
            price,
            new_total_qty: new_total,
        });

        Ok(())
    }

    /// Returns the probe distance (number of buckets probed) for looking up `id` in the index.
    #[inline]
    pub fn index_probe_distance(&self, id: OrderId) -> usize {
        self.id_index.probe_distance(id)
    }

    /// Diagnostic cancellation returning the number of buckets shifted during backward-shift deletion.
    pub fn cancel_into_with_shift_count(
        &mut self,
        id: OrderId,
        events: &mut Vec<Event>,
    ) -> Result<usize, FlatError> {
        self.ensure_event_capacity(events)?;
        events.clear();

        if id.0 > MAX_ORDER_ID {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::OrderIdOutOfRange,
            });
            return Ok(0);
        }

        let (slot_idx, buckets_moved) = match self.id_index.remove_with_shift_count(id) {
            (Some(idx), moved) => (idx, moved),
            (None, _) => {
                events.push(Event::Reject {
                    seq: self.take_seq(),
                    id,
                    reason: RejectReason::UnknownCancel,
                });
                return Ok(0);
            }
        };

        let slot = *self.arena.get(slot_idx).expect("valid slot");
        let tick_idx = slot.price_tick as usize;
        let side = slot.side;
        let price = self.ladder.tick_to_price(tick_idx);

        let new_total =
            self.ladder
                .side_mut(side)
                .remove_order(tick_idx, slot_idx, &mut self.arena);

        self.arena.free(slot_idx);

        events.push(Event::Cancel {
            seq: self.take_seq(),
            id,
        });
        events.push(Event::Delta {
            seq: self.take_seq(),
            side,
            price,
            new_total_qty: new_total,
        });

        Ok(buckets_moved)
    }

    /// Helper to consume resting liquidity from a price level.
    #[allow(clippy::too_many_arguments)]
    fn consume_level(
        ladder: &mut PriceLadder,
        arena: &mut OrderArena,
        id_index: &mut OrderIdIndex,
        next_seq: &mut u64,
        maker_side: Side,
        tick_idx: usize,
        taker_id: OrderId,
        mut taker_qty: u64,
        events: &mut Vec<Event>,
    ) -> u64 {
        let price = ladder.tick_to_price(tick_idx);
        let mut total_filled = 0;

        let level = ladder
            .side_mut(maker_side)
            .get_level_mut(tick_idx)
            .expect("level");

        let mut curr_idx = level.head;
        let mut filled_orders = 0u32;

        while curr_idx != SENTINEL && taker_qty > 0 {
            let slot = *arena.get(curr_idx).expect("slot");
            let fill_qty = slot.qty.min(taker_qty);

            let seq = *next_seq;
            *next_seq += 1;

            events.push(Event::Fill {
                seq,
                fill: Fill {
                    maker_id: slot.id,
                    taker_id,
                    price,
                    qty: fill_qty,
                },
                taker_side: maker_side.opposite(),
            });

            total_filled += fill_qty;
            taker_qty -= fill_qty;

            if fill_qty == slot.qty {
                let next_idx = slot.next;
                id_index.remove(slot.id);
                arena.free(curr_idx);
                curr_idx = next_idx;
                filled_orders += 1;
            } else {
                arena.get_mut(curr_idx).expect("slot").qty -= fill_qty;
                break;
            }
        }

        level.head = curr_idx;
        level.order_count -= filled_orders;
        level.total_qty -= total_filled;
        let new_total = level.total_qty;

        if curr_idx == SENTINEL {
            level.tail = SENTINEL;
            ladder.side_mut(maker_side).clear_bit(tick_idx);
        } else {
            arena.get_mut(curr_idx).expect("slot").prev = SENTINEL;
        }

        let seq = *next_seq;
        *next_seq += 1;

        events.push(Event::Delta {
            seq,
            side: maker_side,
            price,
            new_total_qty: new_total,
        });

        total_filled
    }

    /// Generates a point-in-time snapshot of all resting levels.
    pub fn snapshot(&self) -> Snapshot {
        let mut bids = Vec::new();
        // High to low
        for tick in (0..self.config.tick_count).rev() {
            let level = self.ladder.bids.get_level(tick).unwrap();
            if level.order_count > 0 {
                bids.push((self.ladder.tick_to_price(tick), level.total_qty));
            }
        }

        let mut asks = Vec::new();
        // Low to high
        for tick in 0..self.config.tick_count {
            let level = self.ladder.asks.get_level(tick).unwrap();
            if level.order_count > 0 {
                asks.push((self.ladder.tick_to_price(tick), level.total_qty));
            }
        }

        Snapshot {
            seq: self.seq(),
            bids,
            asks,
        }
    }
}
