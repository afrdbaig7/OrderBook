//! Limit order book matching engine (price-time priority).
//!
//! Single-threaded: the engine thread owns the book exclusively.
//! Bids and asks are stored in `BTreeMap`s for logarithmic best-price queries
//! and ordered traversal. An order index (`HashMap<OrderId, (Side, Price)>`)
//! enables direct O(1) level lookup on cancellation.
//!
//! All state transitions emit strictly sequenced events (`seq` increments by 1
//! per event, including rejects and cancels).

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::price_level::PriceLevel;
use crate::types::{
    Event, Order, OrderId, Price, RejectReason, Side, MAX_BOOK_LEVELS_PER_SIDE, MAX_ORDER_ID,
    MAX_ORDER_QTY, MAX_PRICE,
};

/// Point-in-time view of the book, computed only by the engine.
///
/// Serialized directly onto the wire as the `snapshot` message:
/// `{"type":"snapshot","seq":N,"bids":[[p,q],...],"asks":[[p,q],...]}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Last assigned sequence number at snapshot time.
    pub seq: u64,
    /// Best (highest) bid first.
    pub bids: Vec<(Price, u64)>,
    /// Best (lowest) ask first.
    pub asks: Vec<(Price, u64)>,
}

/// The limit order book for a single symbol.
///
/// Invariants (property-tested in `tests/properties.rs`):
///
/// 1. Never crossed: `best_bid < best_ask` whenever both sides are non-empty.
/// 2. Quantity conservation: submitted = filled + cancelled + dropped + resting.
/// 3. `index` contains exactly the resting orders, mapped to their real level.
/// 4. Determinism: same commands ⇒ identical events and identical state.
/// 5. No empty levels are ever stored.
/// 6. `seq` increases by exactly 1 per emitted event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<Price, PriceLevel>,
    asks: BTreeMap<Price, PriceLevel>,
    index: HashMap<OrderId, (Side, Price)>,
    /// Next sequence number to assign (starts at 1).
    next_seq: u64,
}

impl Default for OrderBook {
    fn default() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: HashMap::new(),
            next_seq: 1,
        }
    }
}

impl OrderBook {
    /// Creates an empty book.
    pub fn new() -> Self {
        Self::default()
    }

    /// Last assigned sequence number (0 for a fresh book).
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

    //
    // Accessors
    //
    /// Highest bid price, if any.
    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    /// Lowest ask price, if any.
    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    /// Resting bid levels, ascending by price (best = `next_back`).
    pub fn bid_levels(&self) -> &BTreeMap<Price, PriceLevel> {
        &self.bids
    }

    /// Resting ask levels, ascending by price (best = `next`).
    pub fn ask_levels(&self) -> &BTreeMap<Price, PriceLevel> {
        &self.asks
    }

    /// Lookup a resting order (O(level depth)).
    pub fn order(&self, id: OrderId) -> Option<&Order> {
        let &(side, price) = self.index.get(&id)?;
        let levels = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        levels.get(&price)?.iter().find(|o| o.id == id)
    }

    /// `true` if `id` currently rests in the book.
    #[inline]
    pub fn is_resting(&self, id: OrderId) -> bool {
        self.index.contains_key(&id)
    }

    /// Number of resting orders (index size).
    #[inline]
    pub fn index_len(&self) -> usize {
        self.index.len()
    }

    /// Iterates index entries: `(order_id, side, price)`.
    pub fn index_entries(&self) -> impl Iterator<Item = (OrderId, Side, Price)> + '_ {
        self.index
            .iter()
            .map(|(&id, &(side, price))| (id, side, price))
    }

    /// Sum of all resting quantity across both sides.
    pub fn total_resting_qty(&self) -> u64 {
        self.bids
            .values()
            .chain(self.asks.values())
            .map(|l| l.total_qty())
            .sum()
    }

    /// Snapshot view of the book; best prices first on each side.
    /// If `depth == 0`, captures the complete book without truncation.
    /// If `depth > 0`, takes at most `depth` levels per side.
    pub fn snapshot(&self, depth: usize) -> Snapshot {
        let (bid_limit, ask_limit) = if depth == 0 {
            (self.bids.len(), self.asks.len())
        } else {
            (depth.min(self.bids.len()), depth.min(self.asks.len()))
        };
        Snapshot {
            seq: self.seq(),
            bids: self
                .bids
                .iter()
                .rev()
                .take(bid_limit)
                .map(|(&p, l)| (p, l.total_qty()))
                .collect(),
            asks: self
                .asks
                .iter()
                .take(ask_limit)
                .map(|(&p, l)| (p, l.total_qty()))
                .collect(),
        }
    }

    /// Best price resting on `side`.
    fn best_price(&self, side: Side) -> Option<Price> {
        match side {
            Side::Bid => self.best_bid(),
            Side::Ask => self.best_ask(),
        }
    }

    // -------------------------------------------------------------------
    // Commands
    // -------------------------------------------------------------------

    /// Submits a limit order.
    ///
    /// Rejects (`qty == 0`, `price == 0`, duplicate resting `OrderId`) emit
    /// a single `Reject` event and change no state.
    ///
    /// Otherwise: `Ack`, then per crossed opposing level the executed
    /// `Fill`s followed by one `Delta` for that level, and finally — if any
    /// quantity remains — a resting `Delta` for the order's own level.
    pub fn submit_limit(&mut self, order: Order) -> Vec<Event> {
        let mut events = Vec::new();

        if let Some(reason) = self.reject_new_order(order.id, order.price, order.qty) {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id: order.id,
                reason,
            });
            return events;
        }

        events.push(Event::Ack {
            seq: self.take_seq(),
            id: order.id,
        });

        let maker_side = order.side.opposite();
        let mut remaining = order.qty;

        while remaining > 0 {
            let Some(best) = self.best_price(maker_side) else {
                break; // no opposing liquidity
            };
            let crosses = match order.side {
                Side::Bid => order.price >= best,
                Side::Ask => order.price <= best,
            };
            if !crosses {
                break; // limit no longer crosses — rest below
            }

            let Some(fills) =
                self.consume_level(maker_side, best, order.id, remaining, &mut events)
            else {
                break; // defensive: nothing executed
            };
            remaining -= fills;
        }

        if remaining > 0 {
            let levels = match order.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if !levels.contains_key(&order.price) && levels.len() >= MAX_BOOK_LEVELS_PER_SIDE {
                events.push(Event::Reject {
                    seq: self.take_seq(),
                    id: order.id,
                    reason: RejectReason::BookDepthExceeded,
                });
                return events;
            }
            let level = levels.entry(order.price).or_default();
            if level
                .try_insert(Order {
                    qty: remaining,
                    ..order
                })
                .is_err()
            {
                if level.is_empty() {
                    levels.remove(&order.price);
                }
                events.push(Event::Reject {
                    seq: self.take_seq(),
                    id: order.id,
                    reason: RejectReason::LevelQuantityOverflow,
                });
                return events;
            }
            let new_total = level.total_qty();
            self.index.insert(order.id, (order.side, order.price));
            events.push(Event::Delta {
                seq: self.take_seq(),
                side: order.side,
                price: order.price,
                new_total_qty: new_total,
            });
        }

        events
    }

    /// Submits a market order.
    ///
    /// Consumes best opposing levels until filled or liquidity is gone.
    /// **Never rests.** Any remainder is dropped and reported as
    /// `Reject(MarketUnfilled)` after the fills it did execute.
    pub fn submit_market(&mut self, id: OrderId, side: Side, qty: u64) -> Vec<Event> {
        let mut events = Vec::new();

        if id.0 > MAX_ORDER_ID {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::OrderIdOutOfRange,
            });
            return events;
        }
        if qty == 0 {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::ZeroQuantity,
            });
            return events;
        }
        if qty > MAX_ORDER_QTY {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::QuantityOutOfRange,
            });
            return events;
        }
        if self.index.contains_key(&id) {
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::DuplicateOrderId,
            });
            return events;
        }

        events.push(Event::Ack {
            seq: self.take_seq(),
            id,
        });

        let maker_side = side.opposite();
        let mut remaining = qty;

        while remaining > 0 {
            let Some(best) = self.best_price(maker_side) else {
                break; // liquidity gone
            };
            let Some(fills) = self.consume_level(maker_side, best, id, remaining, &mut events)
            else {
                break;
            };
            remaining -= fills;
        }

        if remaining > 0 {
            // Dropped remainder: fills above are authoritative, this records
            // the part that never executed.
            events.push(Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::MarketUnfilled,
            });
        }

        events
    }

    /// Cancels a resting order.
    ///
    /// Emits `Cancel` then a `Delta` (`qty = 0` when the level empties).
    /// Unknown or already-cancelled ids emit `Reject(UnknownCancel)`.
    pub fn cancel(&mut self, id: OrderId) -> Vec<Event> {
        let Some(&(side, price)) = self.index.get(&id) else {
            return vec![Event::Reject {
                seq: self.take_seq(),
                id,
                reason: RejectReason::UnknownCancel,
            }];
        };

        let (new_total, level_empty) = {
            let levels = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let level = levels
                .get_mut(&price)
                .expect("indexed order must have a level");
            level
                .remove(id)
                .expect("indexed order must exist in its level");
            (level.total_qty(), level.is_empty())
        };

        if level_empty {
            match side {
                Side::Bid => self.bids.remove(&price),
                Side::Ask => self.asks.remove(&price),
            };
        }
        self.index.remove(&id);

        vec![
            Event::Cancel {
                seq: self.take_seq(),
                id,
            },
            Event::Delta {
                seq: self.take_seq(),
                side,
                price,
                new_total_qty: new_total,
            },
        ]
    }

    // -------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------

    /// Validation for newly submitted orders (cancel has its own path).
    fn reject_new_order(&self, id: OrderId, price: Price, qty: u64) -> Option<RejectReason> {
        if id.0 > MAX_ORDER_ID {
            Some(RejectReason::OrderIdOutOfRange)
        } else if qty == 0 {
            Some(RejectReason::ZeroQuantity)
        } else if qty > MAX_ORDER_QTY {
            Some(RejectReason::QuantityOutOfRange)
        } else if price.0 == 0 {
            Some(RejectReason::ZeroPrice)
        } else if price.0 > MAX_PRICE {
            Some(RejectReason::PriceOutOfRange)
        } else if self.index.contains_key(&id) {
            Some(RejectReason::DuplicateOrderId)
        } else {
            None
        }
    }

    /// Executes `taker_qty` against the level at `price`, appending the
    /// resulting `Fill`/`Delta` events to `events`, purging consumed makers
    /// from the index and dropping the level if it empties.
    ///
    /// Returns the quantity executed, or `None` if nothing executed.
    ///
    /// Borrow note: the level borrow never spans a `take_seq()` call, which
    /// needs `&mut self`.
    fn consume_level(
        &mut self,
        maker_side: Side,
        price: Price,
        taker_id: OrderId,
        taker_qty: u64,
        events: &mut Vec<Event>,
    ) -> Option<u64> {
        let result = {
            let levels = match maker_side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            levels.get_mut(&price)?.execute(taker_id, taker_qty)
        };

        if result.fills.is_empty() {
            return None;
        }
        let executed = result.filled_qty();

        // One Delta per changed level: the level's total after *all* fills
        // in this call. Clients set their view to exactly this number.
        let new_total = {
            let levels = match maker_side {
                Side::Bid => &self.bids,
                Side::Ask => &self.asks,
            };
            levels.get(&price).map_or(0, |l| l.total_qty())
        };

        for fill in result.fills {
            events.push(Event::Fill {
                seq: self.take_seq(),
                fill,
                taker_side: maker_side.opposite(),
            });
        }
        events.push(Event::Delta {
            seq: self.take_seq(),
            side: maker_side,
            price,
            new_total_qty: new_total,
        });

        for consumed in result.consumed {
            self.index.remove(&consumed);
        }

        let empty = {
            let levels = match maker_side {
                Side::Bid => &self.bids,
                Side::Ask => &self.asks,
            };
            levels.get(&price).is_some_and(|l| l.is_empty())
        };
        if empty {
            match maker_side {
                Side::Bid => self.bids.remove(&price),
                Side::Ask => self.asks.remove(&price),
            };
        }

        Some(executed)
    }
}
