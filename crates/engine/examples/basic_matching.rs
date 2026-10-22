//! Limit submission, matching, cancellation, and snapshots without a server.
//!
//! Run with `cargo run -p engine --example basic_matching`.

use engine::{Event, Order, OrderBook, OrderId, Price, Side};

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
    println!("=== Low-Latency Rust Matching Engine: Basic Example ===");

    let mut book = OrderBook::new();

    println!("\n1. Seeding resting liquidity...");
    let resting_bid = Order {
        id: OrderId(1),
        side: Side::Bid,
        price: Price(100),
        qty: 50,
    };
    let resting_ask_1 = Order {
        id: OrderId(2),
        side: Side::Ask,
        price: Price(105),
        qty: 30,
    };
    let resting_ask_2 = Order {
        id: OrderId(3),
        side: Side::Ask,
        price: Price(108),
        qty: 25,
    };

    let events = book.submit_limit(resting_bid);
    print_events("Resting Bid Submitted", &events);

    let events = book.submit_limit(resting_ask_1);
    print_events("Resting Ask 1 Submitted", &events);

    let events = book.submit_limit(resting_ask_2);
    print_events("Resting Ask 2 Submitted", &events);

    println!(
        "\nBest Bid: {:?}, Best Ask: {:?}",
        book.best_bid(),
        book.best_ask()
    );

    // Buy 30 at the maker's price of 105, then rest the remaining 10 at 106.
    // The ask at 108 is outside the buyer's limit.
    println!("\n2. Submitting aggressive crossing limit order (Bid 40 @ 106)...");
    let crossing_bid = Order {
        id: OrderId(4),
        side: Side::Bid,
        price: Price(106),
        qty: 40,
    };
    let match_events = book.submit_limit(crossing_bid);
    print_events(
        "Crossing Order Result (Maker Price Execution + Resting Remainder)",
        &match_events,
    );

    println!(
        "\nPost-Match Touch: Best Bid: {:?}, Best Ask: {:?}",
        book.best_bid(),
        book.best_ask()
    );

    println!("\n3. Cancelling resting bid order (OrderId 1)...");
    let cancel_events = book.cancel(OrderId(1));
    print_events("Cancellation Result", &cancel_events);

    println!("\n4. Capturing complete L2 snapshot...");
    let snapshot = book.snapshot(0); // 0 = complete depth within enforced limits
    println!("Snapshot at monotonic sequence {}:", snapshot.seq);
    println!("  Bids (price, qty): {:?}", snapshot.bids);
    println!("  Asks (price, qty): {:?}", snapshot.asks);

    println!("\n=== Example Completed Successfully ===");
}
