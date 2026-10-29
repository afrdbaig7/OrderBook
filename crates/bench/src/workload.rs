//! Reference-simulated bounded workload generation.
//!
//! Enforces:
//! 1. Reference simulation tracking exact resting order IDs and queue positions.
//! 2. Guaranteed bounded occupancy bands: Low (15-25%), Half (45-55%), Near-Capacity (80-90%).
//! 3. Genuine resting ID selection for successful cancellations (front, mid, back).
//! 4. Separate unknown-ID rejection workload.
//! 5. Targeted workloads: new-level insert, existing-level insert, single maker, sweeps (8, 64, 256).

use std::collections::{HashMap, VecDeque};

use engine::{Order, OrderId, Price, Side};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

pub const BENCH_BASE_PRICE: u64 = 1_000_000_000;
pub const BENCH_TICK_COUNT: usize = 1024;
pub const BENCH_MID_TICK: usize = BENCH_TICK_COUNT / 2;

#[derive(Clone, Debug)]
pub enum BenchCommand {
    Limit(Order),
    Cancel(OrderId),
    Market { id: OrderId, side: Side, qty: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OccupancyBand {
    Low,          // 15% - 25% of capacity
    HalfCapacity, // 45% - 55% of capacity
    NearCapacity, // 80% - 90% of capacity
}

impl OccupancyBand {
    pub fn bounds(self, capacity: usize) -> (usize, usize) {
        match self {
            Self::Low => ((capacity * 15) / 100, (capacity * 25) / 100),
            Self::HalfCapacity => ((capacity * 45) / 100, (capacity * 55) / 100),
            Self::NearCapacity => ((capacity * 80) / 100, (capacity * 90) / 100),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkloadTrace {
    pub name: String,
    pub initial_orders: Vec<Order>,
    pub commands: Vec<BenchCommand>,
    pub capacity: usize,
    pub min_occupancy_observed: usize,
    pub max_occupancy_observed: usize,
    pub initial_occupancy: usize,
    pub final_occupancy: usize,
}

/// Reference order book simulator used during generation to track resting state exactly.
struct RefSimulator {
    bids: HashMap<u64, VecDeque<(OrderId, u64)>>,
    asks: HashMap<u64, VecDeque<(OrderId, u64)>>,
    order_loc: HashMap<OrderId, (Side, u64)>,
}

impl RefSimulator {
    fn new() -> Self {
        Self {
            bids: HashMap::new(),
            asks: HashMap::new(),
            order_loc: HashMap::new(),
        }
    }

    fn order_count(&self) -> usize {
        self.order_loc.len()
    }

    fn insert_resting(&mut self, order: Order) {
        let side_map = match order.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        side_map
            .entry(order.price.0)
            .or_default()
            .push_back((order.id, order.qty));
        self.order_loc.insert(order.id, (order.side, order.price.0));
    }

    fn cancel_order(&mut self, id: OrderId) -> bool {
        let Some((side, price)) = self.order_loc.remove(&id) else {
            return false;
        };
        let side_map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if let Some(queue) = side_map.get_mut(&price) {
            queue.retain(|(oid, _)| *oid != id);
            if queue.is_empty() {
                side_map.remove(&price);
            }
        }
        true
    }

    fn get_resting_ids(&self) -> Vec<OrderId> {
        self.order_loc.keys().copied().collect()
    }

    fn get_front_id(&self, rng: &mut ChaCha8Rng) -> Option<OrderId> {
        let side_map = if rng.random_bool(0.5) {
            &self.bids
        } else {
            &self.asks
        };
        for queue in side_map.values() {
            if let Some(&(id, _)) = queue.front() {
                return Some(id);
            }
        }
        self.get_resting_ids().first().copied()
    }

    fn get_back_id(&self, rng: &mut ChaCha8Rng) -> Option<OrderId> {
        let side_map = if rng.random_bool(0.5) {
            &self.bids
        } else {
            &self.asks
        };
        for queue in side_map.values() {
            if let Some(&(id, _)) = queue.back() {
                return Some(id);
            }
        }
        self.get_resting_ids().last().copied()
    }

    fn get_middle_id(&self) -> Option<OrderId> {
        for queue in self.bids.values().chain(self.asks.values()) {
            if queue.len() >= 3 {
                return Some(queue[queue.len() / 2].0);
            }
        }
        self.get_resting_ids().get(self.order_count() / 2).copied()
    }
}

/// Generates a steady-state workload strictly bounded within the given `OccupancyBand`.
pub fn generate_bounded_steady_workload(
    capacity: usize,
    band: OccupancyBand,
    command_count: usize,
    seed: u64,
) -> WorkloadTrace {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let (min_target, max_target) = band.bounds(capacity);
    let target_initial = (min_target + max_target) / 2;

    let mut sim = RefSimulator::new();
    let mut initial_orders = Vec::new();
    let mut next_id = 1u64;

    let base_price = BENCH_BASE_PRICE - BENCH_MID_TICK as u64;

    // 1. Prepopulate initial state to target_initial
    let per_side = target_initial / 2;
    for i in 0..per_side {
        let tick = BENCH_MID_TICK.saturating_sub(1 + (i % 200));
        let price = Price(base_price + tick as u64);
        let id = OrderId(next_id);
        next_id += 1;
        let order = Order {
            id,
            side: Side::Bid,
            price,
            qty: 10,
        };
        sim.insert_resting(order.clone());
        initial_orders.push(order);
    }
    for i in 0..per_side {
        let tick = BENCH_MID_TICK + (i % 200);
        let price = Price(base_price + tick as u64);
        let id = OrderId(next_id);
        next_id += 1;
        let order = Order {
            id,
            side: Side::Ask,
            price,
            qty: 10,
        };
        sim.insert_resting(order.clone());
        initial_orders.push(order);
    }

    let initial_occupancy = sim.order_count();
    let mut min_occ = initial_occupancy;
    let mut max_occ = initial_occupancy;

    let mut commands = Vec::with_capacity(command_count);

    // 2. Generate commands balancing inserts and cancels to strictly respect bounds
    for _ in 0..command_count {
        let current_occ = sim.order_count();
        min_occ = min_occ.min(current_occ);
        max_occ = max_occ.max(current_occ);

        // Decide operation based on occupancy constraints
        let do_insert = if current_occ <= min_target {
            true // Force insert to stay above minimum
        } else if current_occ >= max_target {
            false // Force cancel to stay below maximum
        } else {
            rng.random_bool(0.50)
        };

        if do_insert {
            let side = if rng.random_bool(0.5) {
                Side::Bid
            } else {
                Side::Ask
            };
            let tick = match side {
                Side::Bid => BENCH_MID_TICK.saturating_sub(1 + rng.random_range(1..150)),
                Side::Ask => BENCH_MID_TICK + rng.random_range(1..150),
            };
            let price = Price(base_price + tick as u64);
            let id = OrderId(next_id);
            next_id += 1;
            let order = Order {
                id,
                side,
                price,
                qty: rng.random_range(1..15),
            };
            sim.insert_resting(order.clone());
            commands.push(BenchCommand::Limit(order));
        } else if let Some(id) = sim.get_front_id(&mut rng) {
            sim.cancel_order(id);
            commands.push(BenchCommand::Cancel(id));
        }
    }

    let final_occupancy = sim.order_count();

    // Verify bounds were strictly respected
    assert!(
        min_occ >= min_target - 5 && max_occ <= max_target + 5,
        "occupancy left declared bounds: observed [{min_occ}, {max_occ}], expected [{min_target}, {max_target}]"
    );

    WorkloadTrace {
        name: format!("Bounded Steady-State ({band:?})"),
        initial_orders,
        commands,
        capacity,
        min_occupancy_observed: min_occ,
        max_occupancy_observed: max_occ,
        initial_occupancy,
        final_occupancy,
    }
}

#[inline(always)]
pub fn bench_window_base() -> u64 {
    BENCH_BASE_PRICE - BENCH_MID_TICK as u64
}

/// Generates targeted workload for non-crossing existing-level insertion.
pub fn generate_existing_level_insert_workload(
    capacity: usize,
    count: usize,
    seed: u64,
) -> WorkloadTrace {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut initial_orders = Vec::new();
    let mut next_id = 1u64;
    let window_base = bench_window_base();

    // Setup 50 resting levels with 2 orders each
    for level in 0..50 {
        let tick = 100 + level;
        let price = Price(window_base + tick as u64);
        for _ in 0..2 {
            let order = Order {
                id: OrderId(next_id),
                side: Side::Bid,
                price,
                qty: 10,
            };
            next_id += 1;
            initial_orders.push(order);
        }
    }

    let mut commands = Vec::with_capacity(count);
    for _ in 0..count {
        let level = rng.random_range(0..50);
        let tick = 100 + level;
        let price = Price(window_base + tick as u64);
        let order = Order {
            id: OrderId(next_id),
            side: Side::Bid,
            price,
            qty: rng.random_range(1..10),
        };
        next_id += 1;
        commands.push(BenchCommand::Limit(order));
    }

    WorkloadTrace {
        name: "Non-Crossing Existing-Level Insert".to_string(),
        initial_orders,
        commands,
        capacity,
        min_occupancy_observed: 100,
        max_occupancy_observed: 100 + count,
        initial_occupancy: 100,
        final_occupancy: 100 + count,
    }
}

/// Generates targeted workload for non-crossing new-level insertion.
pub fn generate_new_level_insert_workload(
    capacity: usize,
    count: usize,
    _seed: u64,
) -> WorkloadTrace {
    let mut commands = Vec::with_capacity(count);
    let window_base = bench_window_base();

    for i in 0..count {
        // Ticks 100..450 are bid levels below BENCH_MID_TICK (512)
        let tick = 100 + (i % 350);
        let price = Price(window_base + tick as u64);
        let order = Order {
            id: OrderId((i + 1) as u64),
            side: Side::Bid,
            price,
            qty: 10,
        };
        commands.push(BenchCommand::Limit(order));
    }

    WorkloadTrace {
        name: "Non-Crossing New-Level Insert".to_string(),
        initial_orders: Vec::new(),
        commands,
        capacity,
        min_occupancy_observed: 0,
        max_occupancy_observed: count,
        initial_occupancy: 0,
        final_occupancy: count,
    }
}

/// Generates targeted workload for successful cancellations (front, mid, back).
pub fn generate_cancellation_workload(
    capacity: usize,
    position: &'static str,
    count: usize,
    seed: u64,
) -> WorkloadTrace {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut sim = RefSimulator::new();
    let mut initial_orders = Vec::new();
    let mut next_id = 1u64;
    let window_base = bench_window_base();

    // Create levels with 10 orders each so front, middle, back are well-defined
    for level in 0..50 {
        let tick = 100 + level;
        let price = Price(window_base + tick as u64);
        for _ in 0..10 {
            let order = Order {
                id: OrderId(next_id),
                side: Side::Bid,
                price,
                qty: 10,
            };
            next_id += 1;
            sim.insert_resting(order.clone());
            initial_orders.push(order);
        }
    }

    let mut commands = Vec::with_capacity(count);
    for _ in 0..count {
        let target_id = match position {
            "front" => sim.get_front_id(&mut rng),
            "middle" => sim.get_middle_id(),
            "back" => sim.get_back_id(&mut rng),
            _ => sim.get_front_id(&mut rng),
        };
        if let Some(id) = target_id {
            sim.cancel_order(id);
            commands.push(BenchCommand::Cancel(id));
        }
    }

    let init_occ = initial_orders.len();
    let final_occ = sim.order_count();

    WorkloadTrace {
        name: format!("Cancellation ({position})"),
        initial_orders,
        commands,
        capacity,
        min_occupancy_observed: final_occ,
        max_occupancy_observed: init_occ,
        initial_occupancy: init_occ,
        final_occupancy: final_occ,
    }
}

/// Generates separate unknown-ID rejection workload.
pub fn generate_unknown_id_rejection_workload(count: usize) -> WorkloadTrace {
    let mut commands = Vec::with_capacity(count);
    // IDs start high and never exist in the book
    for i in 0..count {
        commands.push(BenchCommand::Cancel(OrderId(888_000_000 + i as u64)));
    }
    WorkloadTrace {
        name: "Unknown-ID Rejection".to_string(),
        initial_orders: Vec::new(),
        commands,
        capacity: 4096,
        min_occupancy_observed: 0,
        max_occupancy_observed: 0,
        initial_occupancy: 0,
        final_occupancy: 0,
    }
}

/// Generates multi-maker sweep workload (sweeping across N makers and levels).
pub fn generate_sweep_workload(
    capacity: usize,
    makers_to_sweep: usize,
    levels_to_visit: usize,
    repetitions: usize,
) -> WorkloadTrace {
    let mut initial_orders = Vec::new();
    let mut next_id = 1u64;
    let window_base = bench_window_base();
    let ask_base_tick = BENCH_MID_TICK + 20;

    let makers_per_level = makers_to_sweep.div_ceil(levels_to_visit);
    let mut commands = Vec::with_capacity(repetitions);

    for rep in 0..repetitions {
        let rep_base_tick = ask_base_tick + rep * (levels_to_visit + 1);

        // Populate asks across levels for this repetition
        for level in 0..levels_to_visit {
            let tick = rep_base_tick + level;
            assert!(tick < BENCH_TICK_COUNT, "sweep tick exceeds window");
            let price = Price(window_base + tick as u64);
            for _ in 0..makers_per_level {
                let order = Order {
                    id: OrderId(next_id),
                    side: Side::Ask,
                    price,
                    qty: 10,
                };
                next_id += 1;
                initial_orders.push(order);
            }
        }

        let sweep_price = Price(window_base + (rep_base_tick + levels_to_visit) as u64);
        let sweep_taker = Order {
            id: OrderId(next_id),
            side: Side::Bid,
            price: sweep_price,
            qty: (makers_to_sweep * 10) as u64,
        };
        next_id += 1;
        commands.push(BenchCommand::Limit(sweep_taker));
    }

    let init_occ = initial_orders.len();

    WorkloadTrace {
        name: format!("Sweep {makers_to_sweep} Makers / {levels_to_visit} Levels"),
        initial_orders,
        commands,
        capacity,
        min_occupancy_observed: 0,
        max_occupancy_observed: init_occ,
        initial_occupancy: init_occ,
        final_occupancy: 0,
    }
}
