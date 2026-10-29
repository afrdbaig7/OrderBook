//! Fixed price ladder with hierarchical bitmap price discovery.
//!
//! Uses 64-bit integer words and summary masks with portable CPU intrinsics
//! (`trailing_zeros` and `leading_zeros`) for single-cycle best bid and ask lookup.

#![forbid(unsafe_code)]

use super::arena::SENTINEL;
use crate::types::{Price, Side};

/// Aggregate state for a single price tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    pub total_qty: u64,
    pub head: u32,
    pub tail: u32,
    pub order_count: u32,
}

impl Default for Level {
    #[inline]
    fn default() -> Self {
        Self {
            total_qty: 0,
            head: SENTINEL,
            tail: SENTINEL,
            order_count: 0,
        }
    }
}

/// Hierarchical bitmap for instant best-price resolution over 512 ticks.
#[derive(Clone, Debug)]
pub struct LevelSide {
    pub levels: Box<[Level]>,
    words: Box<[u64]>,
    summary: u64,
    tick_count: usize,
}

impl LevelSide {
    pub fn new(tick_count: usize) -> Self {
        assert!(tick_count > 0, "tick count must be positive");
        let num_words = tick_count.div_ceil(64);
        assert!(
            num_words <= 64,
            "summary mask currently supports up to 64 words (4096 ticks)"
        );

        Self {
            levels: vec![Level::default(); tick_count].into_boxed_slice(),
            words: vec![0u64; num_words].into_boxed_slice(),
            summary: 0,
            tick_count,
        }
    }

    #[inline(always)]
    pub fn set_bit(&mut self, tick_idx: usize) {
        let word_idx = tick_idx / 64;
        let bit_idx = tick_idx % 64;
        self.words[word_idx] |= 1u64 << bit_idx;
        self.summary |= 1u64 << word_idx;
    }

    #[inline(always)]
    pub fn clear_bit(&mut self, tick_idx: usize) {
        let word_idx = tick_idx / 64;
        let bit_idx = tick_idx % 64;
        self.words[word_idx] &= !(1u64 << bit_idx);
        if self.words[word_idx] == 0 {
            self.summary &= !(1u64 << word_idx);
        }
    }

    /// Finds lowest occupied tick index (Best Ask) in ~1 cycle via `trailing_zeros`.
    #[inline(always)]
    pub fn best_ask_tick(&self) -> Option<usize> {
        if self.summary == 0 {
            return None;
        }
        let word_idx = self.summary.trailing_zeros() as usize;
        let bit_idx = self.words[word_idx].trailing_zeros() as usize;
        let tick = word_idx * 64 + bit_idx;
        if tick < self.tick_count {
            Some(tick)
        } else {
            None
        }
    }

    /// Finds highest occupied tick index (Best Bid) in ~1 cycle via `leading_zeros`.
    #[inline(always)]
    pub fn best_bid_tick(&self) -> Option<usize> {
        if self.summary == 0 {
            return None;
        }
        // Highest word with bits
        let word_idx = 63 - (self.summary.leading_zeros() as usize);
        if word_idx >= self.words.len() {
            return None;
        }
        let bit_idx = 63 - (self.words[word_idx].leading_zeros() as usize);
        let tick = word_idx * 64 + bit_idx;
        if tick < self.tick_count {
            Some(tick)
        } else {
            None
        }
    }

    #[inline]
    pub fn get_level(&self, tick_idx: usize) -> Option<&Level> {
        self.levels.get(tick_idx)
    }

    #[inline]
    pub fn get_level_mut(&mut self, tick_idx: usize) -> Option<&mut Level> {
        self.levels.get_mut(tick_idx)
    }

    #[inline(always)]
    pub fn enqueue_order(
        &mut self,
        tick_idx: usize,
        slot_idx: u32,
        qty: u64,
        arena: &mut super::arena::OrderArena,
    ) -> u64 {
        let was_empty = self.levels[tick_idx].head == SENTINEL;
        let new_total = {
            let level = &mut self.levels[tick_idx];
            if was_empty {
                level.head = slot_idx;
                level.tail = slot_idx;
            } else {
                let old_tail = level.tail;
                arena.get_mut(old_tail).expect("slot").next = slot_idx;
                arena.get_mut(slot_idx).expect("slot").prev = old_tail;
                level.tail = slot_idx;
            }
            level.total_qty += qty;
            level.order_count += 1;
            level.total_qty
        };

        if was_empty {
            self.set_bit(tick_idx);
        }

        new_total
    }

    #[inline(always)]
    pub fn remove_order(
        &mut self,
        tick_idx: usize,
        slot_idx: u32,
        arena: &mut super::arena::OrderArena,
    ) -> u64 {
        let slot = *arena.get(slot_idx).expect("valid slot");
        let (now_empty, new_total) = {
            let level = &mut self.levels[tick_idx];

            if slot.prev == SENTINEL {
                level.head = slot.next;
            } else {
                arena.get_mut(slot.prev).expect("prev").next = slot.next;
            }

            if slot.next == SENTINEL {
                level.tail = slot.prev;
            } else {
                arena.get_mut(slot.next).expect("next").prev = slot.prev;
            }

            level.total_qty -= slot.qty;
            level.order_count -= 1;
            (level.head == SENTINEL, level.total_qty)
        };

        if now_empty {
            self.clear_bit(tick_idx);
        }

        new_total
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.summary == 0
    }
}

/// Price ladder containing both bid and ask sides.
#[derive(Clone, Debug)]
pub struct PriceLadder {
    pub base_price: u64,
    pub tick_count: usize,
    pub bids: LevelSide,
    pub asks: LevelSide,
}

impl PriceLadder {
    pub fn new(base_price: u64, tick_count: usize) -> Self {
        Self {
            base_price,
            tick_count,
            bids: LevelSide::new(tick_count),
            asks: LevelSide::new(tick_count),
        }
    }

    /// Maps external Price into tick index `[0, tick_count)`.
    #[inline(always)]
    pub fn price_to_tick(&self, price: Price) -> Option<usize> {
        if price.0 < self.base_price {
            return None;
        }
        let offset = (price.0 - self.base_price) as usize;
        if offset < self.tick_count {
            Some(offset)
        } else {
            None
        }
    }

    /// Maps tick index to external Price.
    #[inline(always)]
    pub fn tick_to_price(&self, tick_idx: usize) -> Price {
        Price(self.base_price + tick_idx as u64)
    }

    #[inline]
    pub fn side(&self, side: Side) -> &LevelSide {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    #[inline]
    pub fn side_mut(&mut self, side: Side) -> &mut LevelSide {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    #[inline(always)]
    pub fn best_tick(&self, side: Side) -> Option<usize> {
        match side {
            Side::Bid => self.bids.best_bid_tick(),
            Side::Ask => self.asks.best_ask_tick(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_parity_with_linear_scan() {
        let mut side = LevelSide::new(512);
        assert_eq!(side.best_ask_tick(), None);
        assert_eq!(side.best_bid_tick(), None);

        // Test boundary ticks: 0, 63, 64, 127, 128, 511
        let test_ticks = [0, 1, 63, 64, 65, 127, 128, 255, 256, 510, 511];

        for &t in &test_ticks {
            side.set_bit(t);
        }

        assert_eq!(side.best_ask_tick(), Some(0));
        assert_eq!(side.best_bid_tick(), Some(511));

        // Clear tick 0
        side.clear_bit(0);
        assert_eq!(side.best_ask_tick(), Some(1));

        // Clear tick 511
        side.clear_bit(511);
        assert_eq!(side.best_bid_tick(), Some(510));

        // Verify that best_ask_tick and best_bid_tick match brute-force scans
        for (i, w) in side.words.iter().enumerate() {
            for b in 0..64 {
                let tick = i * 64 + b;
                if tick >= 512 {
                    continue;
                }
                let bit_set = (w & (1 << b)) != 0;
                let expected_set = test_ticks.contains(&tick) && tick != 0 && tick != 511;
                assert_eq!(bit_set, expected_set, "mismatch at tick {tick}");
            }
        }
    }
}
