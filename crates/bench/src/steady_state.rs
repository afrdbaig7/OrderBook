//! Identical, steady-state benchmark comparison between OrderBook and FlatBook.
//!
//! Generates a single deterministic command stream with stable occupancy
//! (inserts balanced by cancels and fills) and executes the exact same stream
//! on both engines. Separately verifies and reports:
//! - Count of successful inserts, fills, cancellations, and rejections
//! - Batched amortized mean latency (free from timer overhead)
//! - Individual-operation latency percentiles (p50, p90, p99, p99.9, max, >1µs)

use std::time::Instant;

use engine::{
    Event, FlatBook, FlatBookConfig, Order, OrderBook, OrderId, Price, RejectReason, Side,
};
use hdrhistogram::Histogram;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

pub const STEADY_BASE_PRICE: u64 = 1_000_000_000;
pub const STEADY_TICKS: usize = 1024;
pub const STEADY_MID_TICK: usize = STEADY_TICKS / 2;

#[derive(Clone, Debug)]
pub enum Command {
    Limit(Order),
    Cancel(OrderId),
    Market { id: OrderId, side: Side, qty: u64 },
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct EngineCounters {
    pub total_commands: u64,
    pub limit_placed: u64,
    pub fills_count: u64,
    pub filled_qty: u64,
    pub cancels_successful: u64,
    pub cancels_rejected: u64,
    pub rejects_duplicate_id: u64,
    pub rejects_depth_exceeded: u64,
    pub rejects_other: u64,
    pub final_resting_orders: usize,
    pub final_resting_qty: u64,
    pub final_seq: u64,
}

pub struct BenchmarkResult {
    #[allow(dead_code)]
    pub engine_name: &'static str,
    pub counters: EngineCounters,
    pub batch_mean_ns: f64,
    pub batch_throughput_mops: f64,
    pub individual_p50_ns: u64,
    pub individual_p90_ns: u64,
    pub individual_p99_ns: u64,
    pub individual_p99_9_ns: u64,
    pub individual_max_ns: u64,
    pub individual_gt_1us_count: u64,
    pub individual_gt_1us_pct: f64,
}

/// Generates a steady-state stream of commands where inserts, cancels,
/// and fills balance to keep book occupancy steady.
pub fn generate_steady_workload(
    count: usize,
    seed: u64,
    depth: usize,
    per_level: usize,
) -> (Vec<Command>, Vec<Order>, u64) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut initial_orders = Vec::new();
    let mut live_ids = Vec::new();
    let mut next_id = 1u64;

    let base_price = STEADY_BASE_PRICE - STEADY_MID_TICK as u64;

    // 1. Initial balanced book template
    for i in 0..depth {
        let tick = STEADY_MID_TICK.saturating_sub(1 + i);
        let price = Price(base_price + tick as u64);
        for _ in 0..per_level {
            let order = Order {
                id: OrderId(next_id),
                side: Side::Bid,
                price,
                qty: 10,
            };
            initial_orders.push(order);
            live_ids.push(next_id);
            next_id += 1;
        }
    }
    for i in 0..depth {
        let tick = STEADY_MID_TICK + i;
        let price = Price(base_price + tick as u64);
        for _ in 0..per_level {
            let order = Order {
                id: OrderId(next_id),
                side: Side::Ask,
                price,
                qty: 10,
            };
            initial_orders.push(order);
            live_ids.push(next_id);
            next_id += 1;
        }
    }

    // 2. Steady-state stream: 50% Limit, 40% Cancel, 10% Market
    let mut commands = Vec::with_capacity(count);

    for _ in 0..count {
        let roll = rng.random_range(0u64..100);
        if roll < 50 {
            // Limit order: 50% bid, 50% ask
            let side = if rng.random_bool(0.5) {
                Side::Bid
            } else {
                Side::Ask
            };
            let tick = match side {
                Side::Bid => STEADY_MID_TICK.saturating_sub(1 + rng.random_range(0..depth)),
                Side::Ask => STEADY_MID_TICK + rng.random_range(0..depth),
            };
            let price = Price(base_price + tick as u64);
            let id = next_id;
            next_id += 1;
            live_ids.push(id);
            commands.push(Command::Limit(Order {
                id: OrderId(id),
                side,
                price,
                qty: rng.random_range(1..15),
            }));
        } else if roll < 90 && !live_ids.is_empty() {
            // Cancel an active order to maintain steady occupancy
            let idx = rng.random_range(0..live_ids.len());
            let id = live_ids.swap_remove(idx);
            commands.push(Command::Cancel(OrderId(id)));
        } else {
            // Market order against opposing liquidity
            let side = if rng.random_bool(0.5) {
                Side::Bid
            } else {
                Side::Ask
            };
            let id = next_id;
            next_id += 1;
            commands.push(Command::Market {
                id: OrderId(id),
                side,
                qty: rng.random_range(1..20),
            });
        }
    }

    (commands, initial_orders, next_id)
}

fn classify_events(events: &[Event], counters: &mut EngineCounters) {
    for ev in events {
        match ev {
            Event::Ack { .. } => {}
            Event::Fill { fill, .. } => {
                counters.fills_count += 1;
                counters.filled_qty += fill.qty;
            }
            Event::Cancel { .. } => {
                counters.cancels_successful += 1;
            }
            Event::Delta { .. } => {
                counters.limit_placed += 1;
            }
            Event::Reject { reason, .. } => match reason {
                RejectReason::UnknownCancel => counters.cancels_rejected += 1,
                RejectReason::DuplicateOrderId => counters.rejects_duplicate_id += 1,
                RejectReason::BookDepthExceeded => counters.rejects_depth_exceeded += 1,
                _ => counters.rejects_other += 1,
            },
        }
    }
}

pub fn run_order_book_benchmark(
    initial_orders: &[Order],
    commands: &[Command],
) -> BenchmarkResult {
    let mut book = OrderBook::new();
    for o in initial_orders {
        book.submit_limit(o.clone());
    }

    let mut counters = EngineCounters {
        total_commands: commands.len() as u64,
        ..Default::default()
    };

    // 1. Batched measurement for true average per-op cost
    let mut book_batch = book.clone();
    let t_batch_start = Instant::now();
    for cmd in commands {
        match cmd {
            Command::Limit(o) => {
                let evs = book_batch.submit_limit(o.clone());
                classify_events(&evs, &mut counters);
            }
            Command::Cancel(id) => {
                let evs = book_batch.cancel(*id);
                classify_events(&evs, &mut counters);
            }
            Command::Market { id, side, qty } => {
                let evs = book_batch.submit_market(*id, *side, *qty);
                classify_events(&evs, &mut counters);
            }
        }
    }
    let batch_dt = t_batch_start.elapsed();
    let batch_mean_ns = batch_dt.as_nanos() as f64 / commands.len() as f64;
    let batch_throughput_mops = commands.len() as f64 / batch_dt.as_secs_f64() / 1_000_000.0;

    counters.final_resting_orders = book_batch.index_len();
    counters.final_resting_qty = book_batch.total_resting_qty();
    counters.final_seq = book_batch.seq();

    // 2. Individual operation measurement for percentile distribution
    let mut book_indiv = book;
    let mut hist = Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).unwrap();

    for cmd in commands {
        let t0 = Instant::now();
        match cmd {
            Command::Limit(o) => {
                book_indiv.submit_limit(o.clone());
            }
            Command::Cancel(id) => {
                book_indiv.cancel(*id);
            }
            Command::Market { id, side, qty } => {
                book_indiv.submit_market(*id, *side, *qty);
            }
        }
        let dt = t0.elapsed().as_nanos() as u64;
        hist.record(dt.max(1)).unwrap();
    }

    let count_le_1us = hist.count_between(0, 1000);
    let count_gt_1us = hist.len().saturating_sub(count_le_1us);
    let pct_gt_1us = (count_gt_1us as f64 / hist.len() as f64) * 100.0;

    BenchmarkResult {
        engine_name: "OrderBook (Reference)",
        counters,
        batch_mean_ns,
        batch_throughput_mops,
        individual_p50_ns: hist.value_at_quantile(0.50),
        individual_p90_ns: hist.value_at_quantile(0.90),
        individual_p99_ns: hist.value_at_quantile(0.99),
        individual_p99_9_ns: hist.value_at_quantile(0.999),
        individual_max_ns: hist.max(),
        individual_gt_1us_count: count_gt_1us,
        individual_gt_1us_pct: pct_gt_1us,
    }
}

pub fn run_flat_book_benchmark(
    initial_orders: &[Order],
    commands: &[Command],
) -> BenchmarkResult {
    let base_price = STEADY_BASE_PRICE - STEADY_MID_TICK as u64;
    let config = FlatBookConfig {
        base_price,
        tick_count: STEADY_TICKS,
        max_orders: initial_orders.len() + commands.len() + 65_536,
    };
    let mut book = FlatBook::new(config).expect("valid config");
    let mut events_buf = Vec::with_capacity(book.worst_case_event_capacity());

    for o in initial_orders {
        book.submit_limit_into(o.clone(), &mut events_buf).expect("seed");
    }

    let mut counters = EngineCounters {
        total_commands: commands.len() as u64,
        ..Default::default()
    };

    // 1. Batched measurement for true average per-op cost
    let mut book_batch = book.clone();
    let t_batch_start = Instant::now();
    for cmd in commands {
        match cmd {
            Command::Limit(o) => {
                book_batch
                    .submit_limit_into(o.clone(), &mut events_buf)
                    .expect("limit");
                classify_events(&events_buf, &mut counters);
            }
            Command::Cancel(id) => {
                book_batch.cancel_into(*id, &mut events_buf).expect("cancel");
                classify_events(&events_buf, &mut counters);
            }
            Command::Market { id, side, qty } => {
                book_batch
                    .submit_market_into(*id, *side, *qty, &mut events_buf)
                    .expect("market");
                classify_events(&events_buf, &mut counters);
            }
        }
    }
    let batch_dt = t_batch_start.elapsed();
    let batch_mean_ns = batch_dt.as_nanos() as f64 / commands.len() as f64;
    let batch_throughput_mops = commands.len() as f64 / batch_dt.as_secs_f64() / 1_000_000.0;

    let snap = book_batch.snapshot();
    let resting_qty = snap.bids.iter().map(|(_, q)| *q).sum::<u64>()
        + snap.asks.iter().map(|(_, q)| *q).sum::<u64>();
    counters.final_resting_orders = book_batch.resting_order_count();
    counters.final_resting_qty = resting_qty;
    counters.final_seq = book_batch.seq();

    // 2. Individual operation measurement for percentile distribution
    let mut book_indiv = book;
    let mut hist = Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).unwrap();

    for cmd in commands {
        let t0 = Instant::now();
        match cmd {
            Command::Limit(o) => {
                book_indiv
                    .submit_limit_into(o.clone(), &mut events_buf)
                    .expect("limit");
            }
            Command::Cancel(id) => {
                book_indiv.cancel_into(*id, &mut events_buf).expect("cancel");
            }
            Command::Market { id, side, qty } => {
                book_indiv
                    .submit_market_into(*id, *side, *qty, &mut events_buf)
                    .expect("market");
            }
        }
        let dt = t0.elapsed().as_nanos() as u64;
        hist.record(dt.max(1)).unwrap();
    }

    let count_le_1us = hist.count_between(0, 1000);
    let count_gt_1us = hist.len().saturating_sub(count_le_1us);
    let pct_gt_1us = (count_gt_1us as f64 / hist.len() as f64) * 100.0;

    BenchmarkResult {
        engine_name: "FlatBook (Intrusive)",
        counters,
        batch_mean_ns,
        batch_throughput_mops,
        individual_p50_ns: hist.value_at_quantile(0.50),
        individual_p90_ns: hist.value_at_quantile(0.90),
        individual_p99_ns: hist.value_at_quantile(0.99),
        individual_p99_9_ns: hist.value_at_quantile(0.999),
        individual_max_ns: hist.max(),
        individual_gt_1us_count: count_gt_1us,
        individual_gt_1us_pct: pct_gt_1us,
    }
}

pub fn print_steady_comparison(ref_res: &BenchmarkResult, flat_res: &BenchmarkResult) {
    println!("=========================================================================================");
    println!("                    STEADY-STATE IDENTICAL WORKLOAD COMPARISON                           ");
    println!("=========================================================================================");
    println!("  Metric / Operation                   Reference OrderBook     FlatBook (Intrusive)");
    println!("-----------------------------------------------------------------------------------------");
    println!("  Total Commands Evaluated:            {:<24} {:<24}", ref_res.counters.total_commands, flat_res.counters.total_commands);
    println!("  Limit Orders Placed (Delta):         {:<24} {:<24}", ref_res.counters.limit_placed, flat_res.counters.limit_placed);
    println!("  Fills Executed:                      {:<24} {:<24}", ref_res.counters.fills_count, flat_res.counters.fills_count);
    println!("  Total Quantity Traded:               {:<24} {:<24}", ref_res.counters.filled_qty, flat_res.counters.filled_qty);
    println!("  Cancellations Succeeded:             {:<24} {:<24}", ref_res.counters.cancels_successful, flat_res.counters.cancels_successful);
    println!("  Cancellations Rejected (Unknown):    {:<24} {:<24}", ref_res.counters.cancels_rejected, flat_res.counters.cancels_rejected);
    println!("  Rejections (Duplicate ID):           {:<24} {:<24}", ref_res.counters.rejects_duplicate_id, flat_res.counters.rejects_duplicate_id);
    println!("  Rejections (Depth Exceeded):         {:<24} {:<24}", ref_res.counters.rejects_depth_exceeded, flat_res.counters.rejects_depth_exceeded);
    println!("  Final Resting Orders:                {:<24} {:<24}", ref_res.counters.final_resting_orders, flat_res.counters.final_resting_orders);
    println!("  Final Resting Quantity:              {:<24} {:<24}", ref_res.counters.final_resting_qty, flat_res.counters.final_resting_qty);
    println!("  Final Monotonic Sequence (seq):      {:<24} {:<24}", ref_res.counters.final_seq, flat_res.counters.final_seq);

    let parity_ok = ref_res.counters == flat_res.counters;
    println!("  ==> Correctness Parity:              {}", if parity_ok { "EXACT 100% MATCH [PASS]" } else { "MISMATCH [FAIL]" });
    println!("-----------------------------------------------------------------------------------------");
    println!("  Timing Overhead-Free Cost (Batched):");
    println!("    Batch Amortized Mean:              {:.2} ns                {:.2} ns ({:.2}x)",
        ref_res.batch_mean_ns,
        flat_res.batch_mean_ns,
        ref_res.batch_mean_ns / flat_res.batch_mean_ns
    );
    println!("    Sustained Throughput:              {:.2} Mops/s            {:.2} Mops/s",
        ref_res.batch_throughput_mops,
        flat_res.batch_throughput_mops
    );
    println!("-----------------------------------------------------------------------------------------");
    println!("  Individual Operation Percentiles (includes ~20ns QPC measurement floor):");
    println!("    p50 (Median):                      {} ns                 {} ns", ref_res.individual_p50_ns, flat_res.individual_p50_ns);
    println!("    p90:                               {} ns                 {} ns", ref_res.individual_p90_ns, flat_res.individual_p90_ns);
    println!("    p99:                               {} ns                {} ns", ref_res.individual_p99_ns, flat_res.individual_p99_ns);
    println!("    p99.9:                             {} ns                {} ns", ref_res.individual_p99_9_ns, flat_res.individual_p99_9_ns);
    println!("    Maximum Latency:                   {:.2} µs               {:.2} µs",
        ref_res.individual_max_ns as f64 / 1000.0,
        flat_res.individual_max_ns as f64 / 1000.0
    );
    println!("    Tail Spikes > 1µs:                 {} ({:.3}%)          {} ({:.3}%)",
        ref_res.individual_gt_1us_count,
        ref_res.individual_gt_1us_pct,
        flat_res.individual_gt_1us_count,
        flat_res.individual_gt_1us_pct,
    );
    println!("=========================================================================================\n");
}
