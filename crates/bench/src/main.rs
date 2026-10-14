//! Per-operation latency benchmark (Phase C).
//!
//! Measures engine-call latency with `hdrhistogram` over millions of
//! operations and reports p50 / p99 / p99.9 / max, overall and per
//! operation type. Warm-up operations run first and are discarded.
//!
//! What the number includes: `Instant::now()` pairs around a single engine
//! call, in-process, plus histogram recording between operations (which is
//! *outside* the measured window but adds idle time between ops). This is
//! engine-internal latency on one machine — not network latency.
//!
//! ```text
//! cargo run -p bench --release -- --workload mixed --ops 1000000
//! ```

use std::time::Instant;

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;

use bench::{ask_qty, two_sided, BASE_PRICE, DEFAULT_SEED};
use engine::{Order, OrderBook, OrderId, Price, Side};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Workload {
    /// Resting (non-crossing) limit orders only.
    Limit,
    /// Market orders against a continuously replenished ask side.
    Market,
    /// Cancels of randomly chosen resting orders, with unrecorded refills.
    Cancel,
    /// 60% limit / 30% cancel / 10% market.
    Mixed,
}

#[derive(Parser, Debug)]
#[command(
    about = "Engine per-operation latency percentiles via hdrhistogram",
    long_about = None
)]
struct Args {
    /// Recorded operations after warm-up.
    #[arg(long, default_value_t = 1_000_000)]
    ops: u64,

    /// Warm-up operations (discarded, never recorded).
    #[arg(long, default_value_t = 100_000)]
    warmup: u64,

    /// RNG seed — same seed, same operation stream.
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,

    /// Which operation mix to run.
    #[arg(long, value_enum, default_value_t = Workload::Mixed)]
    workload: Workload,

    /// Template depth in price levels.
    #[arg(long, default_value_t = 1_000)]
    depth: u64,

    /// Resting orders per price level in the template.
    #[arg(long, default_value_t = 4)]
    per_level: u64,
}

/// Operation executed in the timed window; selected during `prepare`.
enum Pending {
    Limit(Order),
    Cancel(OrderId),
    Market { id: OrderId, side: Side, qty: u64 },
}

/// Splits each iteration into unrecorded maintenance (`prepare`) and the
/// single timed engine call (`take`), so refills never pollute measurements.
struct Runner {
    workload: Workload,
    pending: Pending,
    next_id: u64,
    /// Band index for non-crossing limit prices.
    band: u64,
    /// Live cancel targets (Mixed / Cancel).
    pool: Vec<u64>,
    /// Ask-maker id counter (Market liquidity refill).
    maker_id: u64,
}

const POOL_LOW: usize = 1_000;
const PRICE_BAND: u64 = 1_000;

impl Runner {
    fn new(workload: Workload, book: &OrderBook) -> Self {
        let mut pool: Vec<u64> = book.index_entries().map(|(id, _, _)| id.0).collect();
        pool.sort_unstable(); // index iteration order is process-random
        Self {
            workload,
            pending: Pending::Cancel(OrderId(0)),
            next_id: book.index_len() as u64 + 1_000_000,
            band: 0,
            pool,
            maker_id: book.index_len() as u64 + 2_000_000,
        }
    }

    /// Unrecorded bookkeeping + selection of the next timed operation.
    fn prepare(&mut self, book: &mut OrderBook, rng: &mut ChaCha8Rng) {
        match self.workload {
            Workload::Limit => {
                let price = BASE_PRICE.saturating_sub(1 + (self.band % PRICE_BAND));
                self.band += 1;
                let id = self.next_id;
                self.next_id += 1;
                self.pending = Pending::Limit(Order {
                    id: OrderId(id),
                    side: Side::Bid,
                    price: Price(price),
                    qty: 1 + id % 5,
                });
            }
            Workload::Market => {
                // Replenish liquidity outside the timed window.
                if ask_qty(book) < 1_000 {
                    for i in 0..50 {
                        let id = self.maker_id;
                        self.maker_id += 1;
                        book.submit_limit(Order {
                            id: OrderId(id),
                            side: Side::Ask,
                            price: Price(BASE_PRICE + (self.band + i) % PRICE_BAND),
                            qty: 10,
                        });
                    }
                    self.band = self.band.wrapping_add(50);
                }
                let id = self.next_id;
                self.next_id += 1;
                self.pending = Pending::Market {
                    id: OrderId(id),
                    side: Side::Bid,
                    qty: 1 + rng.random_range(0..20),
                };
            }
            Workload::Cancel => {
                if self.pool.len() < POOL_LOW {
                    self.refill(book);
                }
                let idx = rng.random_range(0..self.pool.len());
                let id = self.pool.swap_remove(idx);
                self.pending = Pending::Cancel(OrderId(id));
            }
            Workload::Mixed => {
                if self.pool.len() < POOL_LOW {
                    self.refill(book);
                }
                let roll = rng.random_range(0u64..100);
                let op = if roll < 60 {
                    let side = if rng.random_bool(0.5) {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let price = match side {
                        Side::Ask => BASE_PRICE + PRICE_BAND + (self.band % PRICE_BAND),
                        Side::Bid => {
                            BASE_PRICE.saturating_sub(1 + PRICE_BAND + self.band % PRICE_BAND)
                        }
                    };
                    self.band += 1;
                    let id = self.next_id;
                    self.next_id += 1;
                    self.pool.push(id);
                    Pending::Limit(Order {
                        id: OrderId(id),
                        side,
                        price: Price(price),
                        qty: 1 + id % 7,
                    })
                } else if roll < 90 {
                    let idx = rng.random_range(0..self.pool.len());
                    let id = self.pool.swap_remove(idx);
                    Pending::Cancel(OrderId(id))
                } else {
                    let id = self.next_id;
                    self.next_id += 1;
                    Pending::Market {
                        id: OrderId(id),
                        side: Side::Bid,
                        qty: 1 + rng.random_range(0..20),
                    }
                };
                self.pending = op;
            }
        }
    }

    /// The single timed engine call.
    fn take(&mut self, book: &mut OrderBook) -> &'static str {
        match std::mem::replace(&mut self.pending, Pending::Cancel(OrderId(0))) {
            Pending::Limit(o) => {
                book.submit_limit(o);
                "limit"
            }
            Pending::Cancel(id) => {
                book.cancel(id);
                "cancel"
            }
            Pending::Market { id, side, qty } => {
                book.submit_market(id, side, qty);
                "market"
            }
        }
    }

    /// Unrecorded refill of the cancel-target pool (Cancel / Mixed).
    fn refill(&mut self, book: &mut OrderBook) {
        for i in 0..PRICE_BAND {
            let id = self.next_id;
            self.next_id += 1;
            book.submit_limit(Order {
                id: OrderId(id),
                side: Side::Bid,
                price: Price(BASE_PRICE.saturating_sub(1 + PRICE_BAND + i)),
                qty: 10,
            });
            self.pool.push(id);
        }
    }
}

fn hist() -> Histogram<u64> {
    // 1 ns .. 10 s, 3 significant figures.
    Histogram::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds")
}

fn fmt_ns(v: u64) -> String {
    if v < 1_000 {
        format!("{v}ns")
    } else if v < 1_000_000 {
        format!("{:.2}µs", v as f64 / 1_000.0)
    } else {
        format!("{:.2}ms", v as f64 / 1_000_000.0)
    }
}

fn report(name: &str, h: &Histogram<u64>) {
    if h.is_empty() {
        return;
    }
    println!(
        "  {name:<8} n={:<10} p50={:<10} p99={:<10} p99.9={:<10} max={}",
        h.len(),
        fmt_ns(h.value_at_quantile(0.50)),
        fmt_ns(h.value_at_quantile(0.99)),
        fmt_ns(h.value_at_quantile(0.999)),
        fmt_ns(h.max()),
    );
}

fn main() {
    let args = Args::parse();

    let mut book = match args.workload {
        Workload::Limit => {
            // Starts empty; grows with every resting insert.
            OrderBook::new()
        }
        Workload::Market => OrderBook::new(),
        Workload::Cancel => two_sided(args.depth, args.per_level),
        Workload::Mixed => two_sided(args.depth, args.per_level),
    };

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);
    let mut runner = Runner::new(args.workload, &book);

    let mut overall = hist();
    let mut limit_h = hist();
    let mut cancel_h = hist();
    let mut market_h = hist();

    let total = args.warmup + args.ops;
    let mut recorded_wall = std::time::Duration::ZERO;
    let mut record_start = None;

    for i in 0..total {
        runner.prepare(&mut book, &mut rng);
        let t0 = Instant::now();
        let label = runner.take(&mut book);
        let dt = t0.elapsed().as_nanos() as u64;

        if i >= args.warmup {
            if record_start.is_none() {
                record_start = Some(Instant::now());
            }
            let v = dt.max(1);
            overall.record(v).expect("record");
            match label {
                "limit" => limit_h.record(v).expect("record"),
                "cancel" => cancel_h.record(v).expect("record"),
                _ => market_h.record(v).expect("record"),
            }
        }
    }

    if let Some(start) = record_start {
        recorded_wall = start.elapsed();
    }

    println!(
        "workload={:?} ops={} warmup={} seed={} depth={} per_level={}",
        args.workload, args.ops, args.warmup, args.seed, args.depth, args.per_level
    );
    report("overall", &overall);
    report("limit", &limit_h);
    report("cancel", &cancel_h);
    report("market", &market_h);
    let secs = recorded_wall.as_secs_f64();
    if secs > 0.0 {
        println!(
            "  recorded wall: {secs:.3}s  throughput: {:.2} Mops/s",
            args.ops as f64 / secs / 1_000_000.0
        );
    }
    println!(
        "  final book: bids={} asks={} resting_qty={} seq={}",
        book.bid_levels().len(),
        book.ask_levels().len(),
        book.total_resting_qty(),
        book.seq()
    );
}
