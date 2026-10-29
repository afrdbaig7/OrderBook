//! Dedicated allocation-counting test for FlatBook hot-path operations.
//!
//! Uses a custom GlobalAlloc to prove that zero heap allocations, reallocations,
//! or deallocations occur during limit insertion, cancellation, multi-level sweeps,
//! and rejection handling when using a preallocated event buffer.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use engine::{FlatBook, FlatBookConfig, Order, OrderId, Price, Side};

struct CountingAlloc;

static TRACKING: AtomicBool = AtomicBool::new(false);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACKING.load(Ordering::SeqCst) {
            ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.load(Ordering::SeqCst) {
            DEALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

const BASE_PRICE: u64 = 1_000_000_000;

#[test]
fn test_zero_allocations_on_hot_path() {
    let config = FlatBookConfig {
        base_price: BASE_PRICE,
        tick_count: 512,
        max_orders: 4096,
    };

    // 1. Setup and pre-allocation occurs outside the tracking window
    let mut book = FlatBook::new(config).expect("init");
    let mut events = Vec::with_capacity(book.worst_case_event_capacity());

    // Warm-up / pre-touch memory
    for i in 1..=10 {
        let order = Order {
            id: OrderId(i),
            side: Side::Bid,
            price: Price(BASE_PRICE + 100),
            qty: 10,
        };
        book.submit_limit_into(order, &mut events).expect("warmup");
    }
    for i in 1..=10 {
        book.cancel_into(OrderId(i), &mut events)
            .expect("warmup cancel");
    }
    events.clear();

    // 2. Start tracking allocations
    ALLOC_COUNT.store(0, Ordering::SeqCst);
    DEALLOC_COUNT.store(0, Ordering::SeqCst);
    TRACKING.store(true, Ordering::SeqCst);

    // --- A. Non-crossing limit order insertion (new level & existing level) ---
    for i in 100..200 {
        let order = Order {
            id: OrderId(i),
            side: Side::Bid,
            price: Price(BASE_PRICE + (i % 50)),
            qty: 10,
        };
        book.submit_limit_into(order, &mut events)
            .expect("zero-alloc insert");
    }

    // --- B. Cancellations (front, middle, back) ---
    for i in 100..150 {
        book.cancel_into(OrderId(i), &mut events)
            .expect("zero-alloc cancel");
    }

    // --- C. Rejections (unknown cancel, duplicate ID) ---
    book.cancel_into(OrderId(999_999), &mut events)
        .expect("unknown cancel reject");

    let duplicate_order = Order {
        id: OrderId(151), // already resting
        side: Side::Bid,
        price: Price(BASE_PRICE + 50),
        qty: 10,
    };
    book.submit_limit_into(duplicate_order, &mut events)
        .expect("duplicate reject");

    // --- D. Multi-maker matching and multi-level sweep ---
    // Rest asks across 10 levels
    for level in 0..10 {
        let order = Order {
            id: OrderId(500 + level),
            side: Side::Ask,
            price: Price(BASE_PRICE + 300 + level),
            qty: 20,
        };
        book.submit_limit_into(order, &mut events)
            .expect("ask setup");
    }

    // Sweeping taker buy: sweeps all 10 ask levels (200 qty) + rests 50 at 310
    let sweep_order = Order {
        id: OrderId(900),
        side: Side::Bid,
        price: Price(BASE_PRICE + 310),
        qty: 250,
    };
    book.submit_limit_into(sweep_order, &mut events)
        .expect("sweep match");

    // --- E. Market order execution ---
    book.submit_market_into(OrderId(901), Side::Ask, 30, &mut events)
        .expect("market order");

    // 3. Stop tracking allocations
    TRACKING.store(false, Ordering::SeqCst);

    let allocs = ALLOC_COUNT.load(Ordering::SeqCst);
    let deallocs = DEALLOC_COUNT.load(Ordering::SeqCst);

    assert_eq!(
        allocs, 0,
        "VIOLATION: {allocs} heap allocations detected on the hot path!"
    );
    assert_eq!(
        deallocs, 0,
        "VIOLATION: {deallocs} heap deallocations detected on the hot path!"
    );
}
