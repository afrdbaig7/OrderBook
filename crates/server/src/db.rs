//! Batched TimescaleDB persistence for `trades` (spec §36).
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

use crossbeam_channel::{Receiver, RecvTimeoutError};
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
    loop {
        // On shutdown, keep draining the channel until the engine drops its
        // sender rather than returning immediately: rows already queued are
        // committed, not silently lost (spec §32 — avoid data loss on exit).
        if *shutdown.borrow() {
            while let Ok(row) = rx.try_recv() {
                batch.push(row);
                if batch.len() >= BATCH_ROWS {
                    flush(&pool, &mut batch, &metrics).await;
                }
            }
            flush(&pool, &mut batch, &metrics).await;
            info!("persistence: flushed and stopped");
            return;
        }

        match rx.recv_timeout(FLUSH_INTERVAL) {
            Ok(row) => {
                batch.push(row);
                if batch.len() >= BATCH_ROWS {
                    flush(&pool, &mut batch, &metrics).await;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if !batch.is_empty() {
                    flush(&pool, &mut batch, &metrics).await;
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                // Engine thread dropped its sender (shutdown or exit).
                flush(&pool, &mut batch, &metrics).await;
                info!("persistence: channel closed; flushed and stopped");
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
