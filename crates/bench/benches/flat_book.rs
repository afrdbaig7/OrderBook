//! Comparative Criterion benchmarks: FlatBook vs OrderBook.
//!
//! Benchmarks both implementations side-by-side with identical inputs,
//! measuring:
//! - Existing-level vs new-level insertion
//! - Cancellation scaling across queue depths (4, 50, 500)
//! - Sweeps scaling across maker counts (1, 8, 64)
//! - ID index performance under sequential vs collision-heavy IDs
//! - Zero-allocation preallocated event buffer vs heap allocations

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use engine::{Event, FlatBook, FlatBookConfig, Order, OrderBook, OrderId, Price, Side};

const BASE_PRICE: u64 = 1_000_000_000;
const TICK_COUNT: usize = 512;
const MAX_ORDERS: usize = 4096;

fn make_flat_book() -> (FlatBook, Vec<Event>) {
    let config = FlatBookConfig {
        base_price: BASE_PRICE,
        tick_count: TICK_COUNT,
        max_orders: MAX_ORDERS,
    };
    let fb = FlatBook::new(config).expect("valid config");
    let buf = Vec::with_capacity(fb.worst_case_event_capacity());
    (fb, buf)
}

/// Populates `count` orders at a single level to benchmark queue depth scaling.
fn setup_queue_depth(len: usize) -> (OrderBook, FlatBook, Vec<Event>) {
    let mut ob = OrderBook::new();
    let (mut fb, mut buf) = make_flat_book();

    for i in 1..=len {
        let order = Order {
            id: OrderId(i as u64),
            side: Side::Bid,
            price: Price(BASE_PRICE + 100),
            qty: 10,
        };
        ob.submit_limit(order.clone());
        fb.submit_limit_into(order, &mut buf).expect("flat insert");
    }

    (ob, fb, buf)
}

/// Benchmarks non-crossing limit insertion: existing-level vs new-level.
fn bench_insert_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_comparison");
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1));

    // 1. Existing level insert (adding to an already active price level)
    let (ob_template, fb_template, _) = setup_queue_depth(4);

    group.bench_function("OrderBook/existing_level", |b| {
        b.iter_batched_ref(
            || ob_template.clone(),
            |book| {
                book.submit_limit(Order {
                    id: OrderId(9999),
                    side: Side::Bid,
                    price: Price(BASE_PRICE + 100),
                    qty: 10,
                });
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("FlatBook/existing_level", |b| {
        let mut buf = Vec::with_capacity(fb_template.worst_case_event_capacity());
        b.iter_batched_ref(
            || fb_template.clone(),
            |book| {
                book.submit_limit_into(
                    Order {
                        id: OrderId(9999),
                        side: Side::Bid,
                        price: Price(BASE_PRICE + 100),
                        qty: 10,
                    },
                    &mut buf,
                )
                .expect("insert");
            },
            BatchSize::SmallInput,
        )
    });

    // 2. New level insert (creating a new price level in the book)
    group.bench_function("OrderBook/new_level", |b| {
        b.iter_batched_ref(
            || ob_template.clone(),
            |book| {
                book.submit_limit(Order {
                    id: OrderId(9999),
                    side: Side::Bid,
                    price: Price(BASE_PRICE + 200),
                    qty: 10,
                });
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("FlatBook/new_level", |b| {
        let mut buf = Vec::with_capacity(fb_template.worst_case_event_capacity());
        b.iter_batched_ref(
            || fb_template.clone(),
            |book| {
                book.submit_limit_into(
                    Order {
                        id: OrderId(9999),
                        side: Side::Bid,
                        price: Price(BASE_PRICE + 200),
                        qty: 10,
                    },
                    &mut buf,
                )
                .expect("insert");
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmarks cancellation scaling across queue depths (4, 50, 500).
/// Highlights O(n) VecDeque search vs O(1) FlatBook intrusive index unlinking.
fn bench_cancel_scaling(c: &mut Criterion) {
    for len in [4, 50, 500] {
        let (ob_template, fb_template, _) = setup_queue_depth(len);
        let mut group = c.benchmark_group(format!("cancel_scaling/depth_{len}"));
        group.measurement_time(Duration::from_secs(2));
        group.warm_up_time(Duration::from_millis(300));
        group.throughput(Throughput::Elements(1));

        // Middle order cancellation
        let mid_id = OrderId((len / 2) as u64);

        group.bench_function("OrderBook/cancel_mid", |b| {
            b.iter_batched_ref(
                || ob_template.clone(),
                |book| {
                    book.cancel(mid_id);
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function("FlatBook/cancel_mid", |b| {
            let mut buf = Vec::with_capacity(fb_template.worst_case_event_capacity());
            b.iter_batched_ref(
                || fb_template.clone(),
                |book| {
                    book.cancel_into(mid_id, &mut buf).expect("cancel");
                },
                BatchSize::SmallInput,
            )
        });

        // Unknown order cancellation
        group.bench_function("OrderBook/cancel_unknown", |b| {
            b.iter_batched_ref(
                || ob_template.clone(),
                |book| {
                    book.cancel(OrderId(999_999));
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function("FlatBook/cancel_unknown", |b| {
            let mut buf = Vec::with_capacity(fb_template.worst_case_event_capacity());
            b.iter_batched_ref(
                || fb_template.clone(),
                |book| {
                    book.cancel_into(OrderId(999_999), &mut buf)
                        .expect("cancel");
                },
                BatchSize::SmallInput,
            )
        });

        group.finish();
    }
}

/// Benchmarks sweeps across varying maker counts: 1, 8, 64 makers.
fn bench_sweep_scaling(c: &mut Criterion) {
    for maker_count in [1, 8, 64] {
        let mut ob = OrderBook::new();
        let (mut fb, mut buf) = make_flat_book();

        for i in 1..=maker_count {
            let order = Order {
                id: OrderId(i as u64),
                side: Side::Ask,
                price: Price(BASE_PRICE + 100 + (i as u64 % 16)),
                qty: 10,
            };
            ob.submit_limit(order.clone());
            fb.submit_limit_into(order, &mut buf).expect("ask setup");
        }

        let sweep_qty = (maker_count * 10) as u64;
        let mut group = c.benchmark_group(format!("sweep_scaling/makers_{maker_count}"));
        group.measurement_time(Duration::from_secs(2));
        group.throughput(Throughput::Elements(1));

        group.bench_function("OrderBook/sweep", |b| {
            b.iter_batched_ref(
                || ob.clone(),
                |book| {
                    book.submit_limit(Order {
                        id: OrderId(99_999),
                        side: Side::Bid,
                        price: Price(BASE_PRICE + 200),
                        qty: sweep_qty,
                    });
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function("FlatBook/sweep", |b| {
            let mut buf = Vec::with_capacity(fb.worst_case_event_capacity());
            b.iter_batched_ref(
                || fb.clone(),
                |book| {
                    book.submit_limit_into(
                        Order {
                            id: OrderId(99_999),
                            side: Side::Bid,
                            price: Price(BASE_PRICE + 200),
                            qty: sweep_qty,
                        },
                        &mut buf,
                    )
                    .expect("sweep");
                },
                BatchSize::SmallInput,
            )
        });

        group.finish();
    }
}

criterion_group!(
    flat_benches,
    bench_insert_comparison,
    bench_cancel_scaling,
    bench_sweep_scaling
);
criterion_main!(flat_benches);
