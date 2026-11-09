//! Ultra-low latency, zero-allocation matching with FlatBook.
//!
//! Run with `cargo run -p engine --example flat_matching`.

use engine::{Event, FlatBook, FlatBookConfig, Order, OrderId, Price, Side};

fn print_events(label: &str, events: &[Event]) {
    println!("\n--- {label} ---");
    for event in events {
        match event {
            Event::Ack { seq, id } => {
                println!("  [seq {seq}] Ack: order {:?}", id);
            }
            Event::Fill {
                seq,
                fill,
                taker_side,
            } => {
                println!(
                    "  [seq {seq}] Fill: maker {:?} matched by taker {:?} ({:?}) -> qty {} @ price {}",
                    fill.maker_id, fill.taker_id, taker_side, fill.qty, fill.price.0
                );
            }
            Event::Cancel { seq, id } => {
                println!("  [seq {seq}] Cancel: order {:?}", id);
            }
            Event::Reject { seq, id, reason } => {
                println!("  [seq {seq}] Reject: order {:?}, reason: {:?}", id, reason);
            }
            Event::Delta {
                seq,
                side,
                price,
                new_total_qty,
            } => {
                println!(
                    "  [seq {seq}] Delta: {:?} @ price {} -> new aggregate qty {}",
                    side, price.0, new_total_qty
                );
            }
        }
    }
}

fn main() {
    println!("=== Ultra-Low Latency Zero-Allocation Matching: FlatBook Example ===");

    // 1. Configure the contiguous tick array and capacity
    let config = FlatBookConfig {
        base_price: 100, // Lowest tick represented
        tick_count: 512, // Active window covering [100, 612)
        max_orders: 1024,
    };
    let mut book = FlatBook::new(config).expect("valid configuration");

    // 2. Preallocate caller-owned event buffer for zero dynamic allocations on hot path
    let mut events = Vec::with_capacity(book.worst_case_event_capacity());

    println!("\n1. Seeding resting liquidity (bids and asks)...");
    let resting_bid = Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(100),
        qty: 50,
    };
    book.submit_limit_into(resting_bid, &mut events)
        .expect("insert");
    print_events("Resting Bid Submitted", &events);

    let resting_ask = Order {
        id: OrderId(2),
        side: Side::Ask,
        price: Price(105),
        qty: 30,
    };
    book.submit_limit_into(resting_ask, &mut events)
        .expect("insert");
    print_events("Resting Ask Submitted", &events);

    println!(
        "\nTouch: Best Bid: {:?}, Best Ask: {:?}",
        book.best_bid(),
        book.best_ask()
    );

    // 3. Submitting an aggressive crossing limit order
    println!("\n2. Submitting aggressive crossing limit order (Bid 40 @ 106)...");
    let crossing_bid = Order {
        id: OrderId(4),
        side: Side::Bid,
        price: Price(106),
        qty: 40,
    };
    book.submit_limit_into(crossing_bid, &mut events)
        .expect("crossing limit");
    print_events(
        "Crossing Order Result (Maker-Price Fill + Resting Remainder)",
        &events,
    );

    // 4. Amending resting order with queue priority preservation
    println!("\n3. Amending resting order (OrderId 4) reducing qty from 10 to 5...");
    book.modify_into(OrderId(4), 5, Price(106), &mut events)
        .expect("amend");
    print_events("Order Amend Result (Priority Preserved)", &events);

    // 5. Cancelling resting bid order
    println!("\n4. Cancelling resting bid order (OrderId 1)...");
    book.cancel_into(OrderId(1), &mut events).expect("cancel");
    print_events("Cancellation Result", &events);

    // 6. Capturing snapshot
    println!("\n5. Capturing point-in-time L2 snapshot...");
    let snapshot = book.snapshot();
    println!("Snapshot at monotonic sequence {}:", snapshot.seq);
    println!("  Bids (price, qty): {:?}", snapshot.bids);
    println!("  Asks (price, qty): {:?}", snapshot.asks);

    println!("\n=== FlatBook Example Completed Successfully ===");
}
