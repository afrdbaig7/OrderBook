//! Criterion benchmarks for the matching engine.
//!
//! Four workloads × three depth variants:
//!
//! * `insert_noncrossing` — resting limit orders (book grows)
//! * `match_sweep`        — one marketable limit sweeping the whole side
//! * `cancel_order`       — cancel one resting order
//! * `mixed_script`       — 60% limit / 30% cancel / 10% market, 500-op batch
//!
//! Methodology: template construction, book cloning **and book teardown**
//! all happen outside the timer. `iter_batched_ref` is used (not
//! `iter_batched`) because the latter moves the input book into the routine
//! and drops it inside the timed window — at 100k depth that teardown cost
//! dominates the actual engine call. Only the engine call(s) are measured.
//!
//! Run with: `cargo bench -p bench`

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use bench::{ask_depth, ask_qty, bid_depth, mixed_script, two_sided, BASE_PRICE};
use engine::{Order, OrderBook, OrderId, Price, Side};

const DEPTHS: [u64; 3] = [10, 1_000, 100_000];
const PER_LEVEL: u64 = 4;
const INSERT_BATCH: u64 = 1_000;
const MIXED_LEN: usize = 500;

/// Depth >= 100k makes template clones expensive; cut sample count so the
/// suite stays practical while still producing stable medians.
fn tune(group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>, depth: u64) {
    if depth >= 100_000 {
        group.sample_size(20);
        group.measurement_time(std::time::Duration::from_secs(10));
    }
}

/// Non-crossing resting limits against a bid-only template of `depth`
/// levels. Metric: one iteration = 1,000 resting inserts.
fn bench_insert(c: &mut Criterion) {
    for depth in DEPTHS {
        let template = bid_depth(depth, PER_LEVEL);
        let base_id = depth * PER_LEVEL + 1;
        let mut group = c.benchmark_group(format!("insert_noncrossing/depth_{depth}"));
        tune(&mut group, depth);
        group.throughput(Throughput::Elements(INSERT_BATCH));
        group.bench_function("batch_1000", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book| {
                    for i in 0..INSERT_BATCH {
                        book.submit_limit(Order {
                            id: OrderId(base_id + i),
                            side: Side::Bid,
                            price: Price(BASE_PRICE + 10_000 + i), // no asks: never crosses
                            qty: 5,
                        });
                    }
                },
                BatchSize::SmallInput,
            )
        });
        group.finish();
    }
}

/// One marketable limit whose quantity drains the entire ask side.
/// Metric: one iteration = one taker order sweeping all `depth` levels.
fn bench_match(c: &mut Criterion) {
    for depth in DEPTHS {
        let template = ask_depth(depth, PER_LEVEL);
        let sweep_qty = ask_qty(&template);
        let mut group = c.benchmark_group(format!("match_sweep/depth_{depth}"));
        tune(&mut group, depth);
        group.throughput(Throughput::Elements(1));
        group.bench_function("single_taker", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book| {
                    book.submit_limit(Order {
                        id: OrderId(9_000_000_000),
                        side: Side::Bid,
                        price: Price(BASE_PRICE + depth + 10),
                        qty: sweep_qty,
                    });
                },
                BatchSize::SmallInput,
            )
        });
        group.finish();
    }
}

/// Cancel resting orders at front, middle, and back of a level queue.
/// Metric: one iteration = one `cancel` call.
fn bench_cancel(c: &mut Criterion) {
    for depth in DEPTHS {
        let template = bid_depth(depth, PER_LEVEL);
        let mut group = c.benchmark_group(format!("cancel_order/depth_{depth}"));
        tune(&mut group, depth);
        group.throughput(Throughput::Elements(1));

        // Front of queue (level 0, first order: ID 1)
        group.bench_function("front_pos", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book| {
                    book.cancel(OrderId(1));
                },
                BatchSize::SmallInput,
            )
        });

        // Middle of queue (level 0, middle order: ID 2)
        group.bench_function("middle_pos", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book| {
                    book.cancel(OrderId(PER_LEVEL / 2));
                },
                BatchSize::SmallInput,
            )
        });

        // Back of queue (level 0, last order: ID PER_LEVEL)
        group.bench_function("back_pos", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book| {
                    book.cancel(OrderId(PER_LEVEL));
                },
                BatchSize::SmallInput,
            )
        });

        group.finish();
    }
}

/// 500-op scripted mixed workload (60% limit / 30% cancel / 10% market)
/// applied to a fresh clone of a two-sided template each iteration.
/// Metric: one iteration = 500 operations (divide by 500 for per-op).
fn bench_mixed(c: &mut Criterion) {
    for depth in DEPTHS {
        let template = two_sided(depth, PER_LEVEL);
        let script = mixed_script(MIXED_LEN, depth, PER_LEVEL, bench::DEFAULT_SEED);
        let mut group = c.benchmark_group(format!("mixed_script/depth_{depth}"));
        tune(&mut group, depth);
        group.throughput(Throughput::Elements(MIXED_LEN as u64));
        group.bench_function("script_500_ops", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book| {
                    for op in &script {
                        op.apply(book);
                    }
                },
                BatchSize::SmallInput,
            )
        });
        group.finish();
    }
}

const QUEUE_LENGTHS: [usize; 3] = [4, 50, 500];

/// Cancel resting orders varying queue length at a single price level.
/// Characterizes O(n) VecDeque traversal cost in level queue length.
fn bench_cancel_queue_length(c: &mut Criterion) {
    for len in QUEUE_LENGTHS {
        let mut template = OrderBook::new();
        for i in 1..=len {
            template.submit_limit(Order {
                id: OrderId(i as u64),
                side: Side::Bid,
                price: Price(BASE_PRICE),
                qty: 10,
            });
        }

        let mut group = c.benchmark_group(format!("cancel_queue_length/len_{len}"));
        group.sample_size(10);
        group.measurement_time(Duration::from_millis(500));
        group.warm_up_time(Duration::from_millis(200));
        group.throughput(Throughput::Elements(1));

        // Front of queue (index 0)
        group.bench_function("front", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book: &mut OrderBook| {
                    book.cancel(OrderId(1));
                },
                BatchSize::SmallInput,
            )
        });

        // Middle of queue
        group.bench_function("middle", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book: &mut OrderBook| {
                    book.cancel(OrderId((len / 2) as u64));
                },
                BatchSize::SmallInput,
            )
        });

        // Back of queue
        group.bench_function("back", |b| {
            b.iter_batched_ref(
                || template.clone(),
                |book: &mut OrderBook| {
                    book.cancel(OrderId(len as u64));
                },
                BatchSize::SmallInput,
            )
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_insert,
    bench_match,
    bench_cancel,
    bench_cancel_queue_length,
    bench_mixed
);
criterion_main!(benches);
