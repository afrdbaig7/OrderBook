//! Preallocated order-ID to arena slot index with collision handling.
//!
//! Uses open addressing with linear probing and backward-shift deletion.
//! Backward-shift deletion eliminates tombstones entirely, guaranteeing that
//! deleted buckets do not degrade probe chains over repeated insert/delete cycles.

#![forbid(unsafe_code)]

use crate::types::OrderId;

/// Fast integer hash mixer (SplitMix64) to prevent collisions on sequential or clustered IDs.
#[inline(always)]
fn hash_order_id(id: OrderId) -> u64 {
    let mut x = id.0;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bucket {
    pub id: OrderId,
    pub slot_idx: u32,
}

impl Default for Bucket {
    #[inline]
    fn default() -> Self {
        Self {
            id: OrderId(0),
            slot_idx: 0,
        }
    }
}

/// Result of looking up an ID slot for insertion.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EntrySlot {
    Occupied,
    Vacant { bucket_idx: usize, version: u64 },
    Full,
}

/// Open-addressing table mapping arbitrary `OrderId`s to arena slot indices.
#[derive(Clone, Debug)]
pub struct OrderIdIndex {
    buckets: Box<[Bucket]>,
    mask: usize,
    count: usize,
    capacity: usize,
    version: u64,
}

impl OrderIdIndex {
    /// Creates an index with a power-of-two capacity >= `min_capacity`.
    pub fn new(min_capacity: usize) -> Self {
        let capacity = min_capacity.next_power_of_two().max(16);
        let buckets = vec![Bucket::default(); capacity].into_boxed_slice();
        Self {
            buckets,
            mask: capacity - 1,
            count: 0,
            capacity,
            version: 0,
        }
    }

    #[inline(always)]
    fn bucket_idx(&self, id: OrderId) -> usize {
        (hash_order_id(id) as usize) & self.mask
    }

    /// Looks up the arena slot index for `id`.
    #[inline]
    pub fn get(&self, id: OrderId) -> Option<u32> {
        let mut idx = self.bucket_idx(id);
        for _ in 0..self.capacity {
            let bucket = &self.buckets[idx];
            if bucket.id.0 == 0 {
                return None;
            }
            if bucket.id == id {
                return Some(bucket.slot_idx);
            }
            idx = (idx + 1) & self.mask;
        }
        None
    }

    /// Returns `true` if `id` is present in the index.
    #[inline]
    pub fn contains(&self, id: OrderId) -> bool {
        self.get(id).is_some()
    }

    /// Single-pass probe that returns `Occupied` (duplicate), `Full`,
    /// or `Vacant { bucket_idx, version }` where the order ID can be placed safely
    /// if no intervening mutations occur.
    #[inline(always)]
    pub fn find_entry(&self, id: OrderId) -> EntrySlot {
        if self.count >= self.capacity {
            return EntrySlot::Full;
        }
        let mut idx = self.bucket_idx(id);
        for _ in 0..self.capacity {
            let bucket = &self.buckets[idx];
            if bucket.id.0 == 0 {
                return EntrySlot::Vacant {
                    bucket_idx: idx,
                    version: self.version,
                };
            }
            if bucket.id == id {
                return EntrySlot::Occupied;
            }
            idx = (idx + 1) & self.mask;
        }
        EntrySlot::Full
    }

    /// Inserts into a vacant bucket slot located by `find_entry`.
    /// Validates version and occupancy: if an intervening deletion or insertion occurred,
    /// it safely re-probes via `insert`.
    #[inline(always)]
    pub fn insert_at_vacant(
        &mut self,
        bucket_idx: usize,
        version: u64,
        id: OrderId,
        slot_idx: u32,
    ) -> Result<(), &'static str> {
        if self.version == version
            && bucket_idx < self.capacity
            && self.buckets[bucket_idx].id.0 == 0
        {
            self.buckets[bucket_idx] = Bucket { id, slot_idx };
            self.count += 1;
            self.version = self.version.wrapping_add(1);
            Ok(())
        } else {
            self.insert(id, slot_idx)
        }
    }

    /// Inserts `id -> slot_idx`. Returns `Err` if `id` already exists or index is full.
    #[inline]
    pub fn insert(&mut self, id: OrderId, slot_idx: u32) -> Result<(), &'static str> {
        if self.count >= self.capacity {
            return Err("id index full");
        }

        let mut idx = self.bucket_idx(id);
        for _ in 0..self.capacity {
            let bucket = &mut self.buckets[idx];
            if bucket.id.0 == 0 {
                *bucket = Bucket { id, slot_idx };
                self.count += 1;
                self.version = self.version.wrapping_add(1);
                return Ok(());
            }
            if bucket.id == id {
                return Err("duplicate order id");
            }
            idx = (idx + 1) & self.mask;
        }
        Err("id index full")
    }

    /// Removes `id` using backward-shift deletion to preserve probe chains without tombstones.
    /// Returns `(slot_idx, buckets_moved)`.
    #[inline]
    pub fn remove_with_shift_count(&mut self, id: OrderId) -> (Option<u32>, usize) {
        let mut idx = self.bucket_idx(id);
        let mut found_slot = None;

        // 1. Locate the item (bounded to capacity to prevent infinite loop on 100% full table)
        for _ in 0..self.capacity {
            if self.buckets[idx].id.0 == 0 {
                break;
            }
            if self.buckets[idx].id == id {
                found_slot = Some(self.buckets[idx].slot_idx);
                break;
            }
            idx = (idx + 1) & self.mask;
        }

        let Some(slot) = found_slot else {
            return (None, 0);
        };

        // 2. Backward-shift deletion
        let mut i = idx;
        let mut j = (idx + 1) & self.mask;
        let mut buckets_moved = 0usize;

        for _ in 0..self.capacity {
            if self.buckets[j].id.0 == 0 {
                break;
            }
            let natural_h = self.bucket_idx(self.buckets[j].id);
            // In circular buffer: is natural_h <= i in the range from j backwards?
            let should_move = if i <= j {
                natural_h <= i || natural_h > j
            } else {
                natural_h <= i && natural_h > j
            };

            if should_move {
                self.buckets[i] = self.buckets[j];
                i = j;
                buckets_moved += 1;
            }
            j = (j + 1) & self.mask;
        }

        // Clear the now-empty terminal bucket
        self.buckets[i] = Bucket::default();
        self.count -= 1;
        self.version = self.version.wrapping_add(1);

        (Some(slot), buckets_moved)
    }

    /// Removes `id` using backward-shift deletion to preserve probe chains without tombstones.
    /// Returns the removed slot index, if present.
    #[inline]
    pub fn remove(&mut self, id: OrderId) -> Option<u32> {
        self.remove_with_shift_count(id).0
    }

    /// Measures the probe distance (number of buckets probed) for looking up `id`.
    #[inline]
    pub fn probe_distance(&self, id: OrderId) -> usize {
        let mut idx = self.bucket_idx(id);
        for probe in 1..=self.capacity {
            let bucket = &self.buckets[idx];
            if bucket.id.0 == 0 || bucket.id == id {
                return probe;
            }
            idx = (idx + 1) & self.mask;
        }
        self.capacity
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
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
    fn insert_lookup_and_backward_shift_deletion() {
        let mut index = OrderIdIndex::new(32);
        assert_eq!(index.len(), 0);

        for i in 1..=20 {
            index.insert(OrderId(i), i as u32).expect("insert");
        }
        assert_eq!(index.len(), 20);

        for i in 1..=20 {
            assert_eq!(index.get(OrderId(i)), Some(i as u32));
        }

        // Delete odd IDs
        for i in (1..=20).step_by(2) {
            assert_eq!(index.remove(OrderId(i)), Some(i as u32));
            assert_eq!(index.get(OrderId(i)), None);
        }
        assert_eq!(index.len(), 10);

        // Even IDs must still resolve correctly despite deletions in probe chains
        for i in (2..=20).step_by(2) {
            assert_eq!(index.get(OrderId(i)), Some(i as u32));
        }
    }

    #[test]
    fn deliberate_hash_collision_probing() {
        // Test with power-of-two table and sequential IDs
        let mut index = OrderIdIndex::new(16);
        for i in 1..=8 {
            index.insert(OrderId(i * 1000), i as u32).expect("insert");
        }
        for i in 1..=8 {
            assert_eq!(index.get(OrderId(i * 1000)), Some(i as u32));
        }
        assert_eq!(index.remove(OrderId(4000)), Some(4));
        assert_eq!(index.get(OrderId(4000)), None);
        // Ensure neighbors are intact
        assert_eq!(index.get(OrderId(3000)), Some(3));
        assert_eq!(index.get(OrderId(5000)), Some(5));
    }

    #[test]
    fn full_table_lookup_terminates() {
        let mut index = OrderIdIndex::new(16);
        let cap = index.capacity();
        for i in 0..cap {
            index
                .insert(OrderId(i as u64 + 1), i as u32)
                .expect("insert");
        }
        assert_eq!(index.len(), cap);
        // Table is 100% full; missing ID lookup must return None safely and terminate immediately
        assert_eq!(index.get(OrderId(999_999)), None);
        assert!(!index.contains(OrderId(999_999)));
        assert_eq!(index.remove(OrderId(999_999)), None);
    }
}
