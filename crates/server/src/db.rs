//! Batched TimescaleDB persistence for executed trades.
//!
//! # Design
//!
//! * The engine thread pushes [`TradeRow`]s with `try_send` into a **bounded**
//!   `crossbeam-channel`. It never awaits the database.
//! * This task accumulates rows and flushes with a single `INSERT ... SELECT
//!   FROM unnest(...)` every [`BATCH_ROWS`] rows **or** every
//!   [`FLUSH_INTERVAL`], whichever comes first. One round-trip per batch
//!   keeps the DB from becoming a latency source.
//! * If the channel is full the engine drops the row and increments a
//!   counter — under sustained overload persistence degrades, matching never
//!   stalls.
//! * Connection uses dialect-independent `unnest`, avoids `chrono`, and
//!   converts epoch milliseconds with `to_timestamp(ms / 1000.0)`. The
//!   `trades` hypertable's `ts` column is `TIMESTAMPTZ`.

use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::Receiver;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::engine_thread::TradeRow;
use crate::metrics::Metrics;
use crate::state::Persistence;

/// Flush after this many buffered rows.
const BATCH_ROWS: usize = 1000;
/// ...or after this long with a non-empty buffer.
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// One `INSERT ... unnest` for a whole batch. Columns are cast explicitly so
/// `sum(qty)`-style numerics never leak into the writer.
const INSERT_SQL: &str = "\
INSERT INTO trades (ts, seq, price, qty, taker_side)
SELECT to_timestamp(ms / 1000.0), seq, price, qty, side
FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::smallint[])
     AS batch(ms, seq, price, qty, side)";

/// Runs the persistence writer until the channel closes or `shutdown` fires.
pub async fn run(
    url: String,
    rx: Receiver<TradeRow>,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
    persistence: Persistence,
) {
    let pool = match connect_with_retry(&url, &mut shutdown).await {
        Some(pool) => pool,
        None => {
            warn!("persistence: gave up connecting; trades will not be stored");
            return;
        }
    };
    info!("persistence: connected to TimescaleDB");
    *persistence.write().await = Some(pool.clone());

    let mut batch: Vec<TradeRow> = Vec::with_capacity(BATCH_ROWS);
    let mut flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
    loop {
        // Flush if the elapsed time deadline expired while holding buffered rows
        let now = tokio::time::Instant::now();
        if !batch.is_empty() && now >= flush_deadline {
            flush(&pool, &mut batch, &metrics).await;
            flush_deadline = now + FLUSH_INTERVAL;
        }

        match rx.try_recv() {
            Ok(row) => {
                if batch.is_empty() {
                    flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
                }
                batch.push(row);
                if batch.len() >= BATCH_ROWS || tokio::time::Instant::now() >= flush_deadline {
                    flush(&pool, &mut batch, &metrics).await;
                    flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
                } else if batch.len().is_multiple_of(100) {
                    tokio::task::yield_now().await;
                }
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {
                let now = tokio::time::Instant::now();
                if !batch.is_empty() && now >= flush_deadline {
                    flush(&pool, &mut batch, &metrics).await;
                    flush_deadline = now + FLUSH_INTERVAL;
                }

                // If shutdown was requested, yield or sleep briefly and keep waiting
                // for the engine thread to finish processing queued commands and disconnect.
                let sleep_duration = if *shutdown.borrow() {
                    Duration::from_millis(1)
                } else if batch.is_empty() {
                    Duration::from_millis(10)
                } else if now >= flush_deadline {
                    Duration::from_millis(0)
                } else {
                    flush_deadline.saturating_duration_since(now)
                };

                if sleep_duration.is_zero() {
                    if !batch.is_empty() {
                        flush(&pool, &mut batch, &metrics).await;
                        flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
                    }
                } else {
                    tokio::select! {
                        _ = tokio::time::sleep(sleep_duration) => {
                            if !batch.is_empty() && tokio::time::Instant::now() >= flush_deadline {
                                flush(&pool, &mut batch, &metrics).await;
                                flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
                            }
                        }
                        _ = shutdown.changed() => {}
                    }
                }
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                // Engine thread finished all queued commands and dropped its sender.
                // Flush all remaining buffered rows and return.
                flush(&pool, &mut batch, &metrics).await;
                info!("persistence: channel disconnected, all queued trades flushed and stopped");
                return;
            }
        }
    }
}

/// Connects, retrying until the database answers or shutdown is requested.
async fn connect_with_retry(url: &str, shutdown: &mut watch::Receiver<bool>) -> Option<PgPool> {
    loop {
        if *shutdown.borrow() {
            return None;
        }
        match PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
        {
            Ok(pool) => return Some(pool),
            Err(error) => {
                warn!(%error, "persistence: DB not ready, retrying in 2s");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            return None;
                        }
                    }
                }
            }
        }
    }
}

/// Writes and clears `batch`. Failures are counted, not retried: a dropped
/// batch is preferable to an unbounded retry queue.
async fn flush(pool: &PgPool, batch: &mut Vec<TradeRow>, metrics: &Arc<Metrics>) {
    if batch.is_empty() {
        return;
    }
    let n = batch.len();
    let mut ms: Vec<i64> = Vec::with_capacity(n);
    let mut seq: Vec<i64> = Vec::with_capacity(n);
    let mut price: Vec<i64> = Vec::with_capacity(n);
    let mut qty: Vec<i64> = Vec::with_capacity(n);
    let mut side: Vec<i16> = Vec::with_capacity(n);
    for row in batch.iter() {
        ms.push(row.ts_ms);
        seq.push(row.seq);
        price.push(row.price);
        qty.push(row.qty);
        side.push(row.taker_side);
    }

    let result = sqlx::query(INSERT_SQL)
        .bind(ms)
        .bind(seq)
        .bind(price)
        .bind(qty)
        .bind(side)
        .execute(pool)
        .await;

    match result {
        Ok(_) => metrics.add_persisted(n as u64),
        Err(error) => {
            warn!(rows = n, %error, "persistence: batch insert failed; dropping batch");
            metrics.add_persistence_drops(n as u64);
        }
    }
    batch.clear();
}
