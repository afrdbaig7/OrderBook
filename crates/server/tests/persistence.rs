//! Phase E gate: persistence against a live TimescaleDB.
//!
//! Requires a reachable database, supplied via `TEST_DATABASE_URL`
//! (e.g. `postgres://orderbook:orderbook@localhost:5432/orderbook`, which
//! `docker compose up -d` provides after applying `db/schema.sql`). When the
//! variable is unset the test skips, so CI without Docker stays green.
//!
//! Verifies, end-to-end, that:
//!
//! * the server reports `persistence_enabled: true` from `/health`;
//! * trade rows actually land in the `trades` hypertable;
//! * the `candles_1s` continuous aggregate yields bars;
//! * the `/candles` HTTP endpoint returns them.

use std::process::Stdio;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::process::Command;

fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.is_empty())
}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

async fn http_request(port: u16, method: &str, path: &str, body: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(String::from_utf8_lossy(&response).into_owned())
}

async fn wait_for_persistence(port: u16) {
    for _ in 0..200 {
        if let Ok(body) = http_request(port, "GET", "/health", "").await {
            if body.contains("200 OK") && body.contains("persistence_enabled\":true") {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("server never reported persistence_enabled=true");
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn phase_e_gate_persists_trades_and_candles() {
    let Some(url) = database_url() else {
        eprintln!("TEST_DATABASE_URL unset — skipping persistence gate");
        return;
    };

    // The DB itself must be reachable, with the schema applied.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap_or_else(|error| panic!("cannot connect to database: {error}"));

    sqlx::query("SELECT count(*) FROM trades")
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|error| panic!("schema not applied? failed querying trades: {error}"));
    sqlx::query("SELECT count(*) FROM candles_1s WHERE bucket < now()")
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|error| panic!("candles_1s missing: {error}"));

    let port = free_port().await;
    let bin = env!("CARGO_BIN_EXE_server");
    let mut child = Command::new(bin)
        .args([
            format!("--port={port}"),
            "--feed-rate=2000".to_string(),
            "--seed=7".to_string(),
            "--snapshot-depth=20".to_string(),
        ])
        .env("DATABASE_URL", url)
        .env("RUST_LOG", "warn")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    wait_for_persistence(port).await;

    // Feed a few seconds of executions, then check what the DB absorbed.
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Trades landed.
    let trades: i64 = sqlx::query_scalar("SELECT count(*) FROM trades")
        .fetch_one(&pool)
        .await
        .expect("count trades");
    assert!(trades > 0, "expected persisted trades, got {trades}");

    // One-second bars (real-time aggregation covers the last second; the
    // refresh policy materializes the rest).
    let candles: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM candles_1s WHERE bucket >= to_timestamp($1 / 1000.0) - interval '10 seconds'",
    )
    .bind(now_ms())
    .fetch_one(&pool)
    .await
    .expect("count candles");
    assert!(candles > 0, "expected candle bars, got {candles}");

    // Pricing sanity: trades recorded within the last minute should have a
    // positive volume-weighted price consistent with the feed's mid ~100_000.
    let vwap: Option<f64> = sqlx::query_scalar(
        "SELECT sum(price * qty)::double precision / nullif(sum(qty)::double precision, 0.0) FROM trades WHERE ts >= now() - interval '1 minute'",
    )
    .fetch_one(&pool)
    .await
    .expect("vwap");
    let vwap = vwap.expect("no trades in the last minute");
    assert!(
        (99_000.0..=101_000.0).contains(&vwap),
        "vwap {} outside expected feed range",
        vwap
    );

    // The HTTP endpoint reads the same data back.
    let to = now_ms();
    let from = now_ms() - 60_000;
    let body = http_request(
        port,
        "GET",
        &format!("/candles?from={from}&to={to}&interval=1s"),
        "",
    )
    .await
    .expect("get /candles");
    assert!(body.contains("200 OK"), "{body}");
    println!(
        "phase_e: sample candles response: {}",
        &body[..body.len().min(400)]
    );

    child.kill().await.expect("kill server");
    let _ = child.wait().await;
    pool.close().await;
}
