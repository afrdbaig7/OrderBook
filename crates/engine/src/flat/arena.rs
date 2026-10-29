//! Preallocated order arena with safe index-linked intrusive queues.
//!
//! Slots are stored in a preallocated contiguous `Box<[Slot]>`. A singly-linked
//! free list manages available slots using `u32` indices. Empty links are
//! initialized to [`SENTINEL`]. Reused slots are completely reset.

#![forbid(unsafe_code)]

use crate::types::{OrderId, Side};

pub const SENTINEL: u32 = u32::MAX;

/// A single order storage slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    pub id: OrderId,
    pub side: Side,
    pub price_tick: u32,
    pub qty: u64,
    pub prev: u32,
    pub next: u32,
}

impl Default for Slot {
    #[inline]
    fn default() -> Self {
        Self {
            id: OrderId(0),
            side: Side::Bid,
            price_tick: 0,
            qty: 0,
            prev: SENTINEL,
            next: SENTINEL,
        }
    }
}

/// Contiguous order storage with constant-time allocation and reuse.
#[derive(Clone, Debug)]
pub struct OrderArena {
    slots: Box<[Slot]>,
    free_head: u32,
    allocated_count: usize,
    capacity: usize,
}

impl OrderArena {
    /// Preallocates storage for up to `capacity` resting orders.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "arena capacity must be positive");
        assert!(
            capacity <= (u32::MAX - 1) as usize,
            "arena capacity exceeds u32 limit"
        );

        let mut slots = vec![Slot::default(); capacity].into_boxed_slice();
        for i in 0..(capacity - 1) {
            slots[i].next = (i + 1) as u32;
        }
        slots[capacity - 1].next = SENTINEL;

        Self {
            slots,
            free_head: 0,
            allocated_count: 0,
            capacity,
        }
    }

    /// Allocates a slot index from the free list.
    /// Returns `None` if the arena is at full capacity.
    #[inline]
    pub fn allocate(&mut self, id: OrderId, side: Side, price_tick: u32, qty: u64) -> Option<u32> {
        if self.free_head == SENTINEL {
            return None;
        }

        let idx = self.free_head;
        self.free_head = self.slots[idx as usize].next;

        // Reset the slot completely on reuse
        self.slots[idx as usize] = Slot {
            id,
            side,
            price_tick,
            qty,
            prev: SENTINEL,
            next: SENTINEL,
        };

        self.allocated_count += 1;
        Some(idx)
    }

    #[inline(always)]
    pub fn free(&mut self, idx: u32) {
        assert!((idx as usize) < self.capacity, "invalid slot index");
        let slot = &mut self.slots[idx as usize];
        slot.id = OrderId(0);
        slot.prev = SENTINEL;
        slot.next = self.free_head;
        self.free_head = idx;
        self.allocated_count -= 1;
    }

    #[inline]
    pub fn get(&self, idx: u32) -> Option<&Slot> {
        self.slots.get(idx as usize)
    }

    #[inline]
    pub fn get_mut(&mut self, idx: u32) -> Option<&mut Slot> {
        self.slots.get_mut(idx as usize)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.allocated_count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.allocated_count == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_alloc_and_free_cycles() {
        let mut arena = OrderArena::new(3);
        assert_eq!(arena.capacity(), 3);
        assert_eq!(arena.len(), 0);

        let _i0 = arena.allocate(OrderId(10), Side::Bid, 5, 100).expect("i0");
        let i1 = arena.allocate(OrderId(20), Side::Ask, 6, 200).expect("i1");
        let _i2 = arena.allocate(OrderId(30), Side::Bid, 7, 300).expect("i2");
        assert_eq!(arena.len(), 3);
        assert!(arena.allocate(OrderId(40), Side::Ask, 8, 400).is_none());

        arena.free(i1);
        assert_eq!(arena.len(), 2);

        // Reused slot should have new clean values
        let i3 = arena.allocate(OrderId(50), Side::Ask, 9, 500).expect("i3");
        assert_eq!(i3, i1);
        let slot = arena.get(i3).expect("slot");
        assert_eq!(slot.id, OrderId(50));
        assert_eq!(slot.qty, 500);
        assert_eq!(slot.prev, SENTINEL);
        assert_eq!(slot.next, SENTINEL);
    }
}
