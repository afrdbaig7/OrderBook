//! Tests for graceful shutdown ordering.
//!
//! Verifies Requirement 2:
//! When shutdown begins while commands remain queued in the engine channel,
//! the engine finishes all queued commands, emits all resulting trades to the
//! DB channel, and only drops `db_tx` when done. The DB writer drains until
//! channel disconnect, ensuring zero trade loss upon shutdown.

use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam_channel::bounded;
use engine::{Order, OrderBook, OrderId, Price, Side};
use feed::Command;
use server::engine_thread::{self, EngineCommand, TradeRow};
use server::metrics::Metrics;
use tokio::sync::{broadcast, watch};

#[tokio::test]
async fn shutdown_drains_queued_engine_commands_and_trades() {
    let book = OrderBook::new();
    let (cmd_tx, cmd_rx) = bounded::<EngineCommand>(500);
    let (broadcast_tx, _broadcast_rx) = broadcast::channel(100);
    let (db_tx, db_rx) = bounded::<TradeRow>(500);
    let metrics = Metrics::new();

    // Spawn engine thread
    let engine_handle =
        engine_thread::spawn(book, cmd_rx, broadcast_tx, Some(db_tx), metrics.clone());

    // Populate engine with resting asks
    for i in 1..=20 {
        let cmd = Command::Limit(Order {
            id: OrderId(i),
            side: Side::Ask,
            price: Price(100 + i),
            qty: 10,
        });
        cmd_tx.send(EngineCommand::Feed(cmd)).unwrap();
    }

    // Queue 5 market orders that will each generate 2 trades
    for i in 1..=5 {
        let cmd = Command::Market {
            id: OrderId(100 + i),
            side: Side::Bid,
            qty: 20,
        };
        cmd_tx.send(EngineCommand::Feed(cmd)).unwrap();
    }

    // Now send Shutdown to the engine queue
    cmd_tx.send(EngineCommand::Shutdown).unwrap();

    // Start a simulated slow persistence writer draining db_rx
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let writer_metrics = metrics.clone();

    let writer_task = tokio::spawn(async move {
        let mut received = Vec::new();
        // Emulate slow writer draining with shutdown watch
        loop {
            match db_rx.try_recv() {
                Ok(row) => {
                    tokio::time::sleep(Duration::from_millis(5)).await; // simulate slow write
                    received.push(row);
                    writer_metrics.add_persisted(1);
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    if *shutdown_rx.borrow() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // Channel disconnected! Engine finished and dropped db_tx
                    break;
                }
            }
        }
        received
    });

    // 1. Tell components shutdown is requested
    let _ = shutdown_tx.send(true);

    // 2. Wait for engine thread to join
    let engine_join = tokio::task::spawn_blocking(move || engine_handle.join());
    let join_result = tokio::time::timeout(Duration::from_secs(5), engine_join)
        .await
        .expect("engine shutdown timed out")
        .expect("join handle ok");
    assert!(join_result.is_ok(), "engine thread panicked");

    // 3. Wait for writer task to finish draining until Disconnected
    let written = tokio::time::timeout(Duration::from_secs(5), writer_task)
        .await
        .expect("writer task timed out")
        .expect("writer join ok");

    // Each of the 5 market orders swept 20 qty = 2 levels (10 qty each) = 2 fills = 10 trades total
    assert_eq!(
        metrics.trades.load(Ordering::Relaxed),
        10,
        "engine generated 10 trades"
    );
    assert_eq!(
        written.len(),
        10,
        "persistence writer drained all 10 trades before exit"
    );
    assert_eq!(
        metrics.persisted_rows.load(Ordering::Relaxed),
        10,
        "all 10 rows recorded as persisted without loss"
    );
}
