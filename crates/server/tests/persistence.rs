//! Integration test: persistence against a live TimescaleDB.
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
async fn persists_trades_and_candles() {
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
            "--snapshot-depth=1000".to_string(),
            "--allow-public-feed-rate".to_string(),
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
    println!("sample candles response: {}", &body[..body.len().min(400)]);

    // ── Dedicated Database-Backed Candle Boundary Semantics ─────────────
    // Assert returned timestamps follow [from, to): include from, exclude to,
    // and handle empty and maximum-length windows correctly.
    let json_str = body.split("\r\n\r\n").nth(1).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).expect("parse json");
    let candle_list = parsed["candles"].as_array().expect("candles array");
    assert!(
        candle_list.len() >= 2,
        "expected at least 2 candle bars for boundary validation, got {}",
        candle_list.len()
    );

    let t0 = candle_list[0]["ts"].as_i64().expect("t0");
    let t1 = candle_list[1]["ts"].as_i64().expect("t1");
    assert!(
        t1 > t0,
        "consecutive candles should have increasing timestamps"
    );

    // 1. Exact half-open [t0, t1): must include t0 and exclude t1
    let resp = http_request(
        port,
        "GET",
        &format!("/candles?from={t0}&to={t1}&interval=1s"),
        "",
    )
    .await
    .unwrap();
    assert!(resp.contains("200 OK"), "{resp}");
    let b_json: serde_json::Value =
        serde_json::from_str(resp.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let b_candles = b_json["candles"].as_array().unwrap();
    assert_eq!(b_candles.len(), 1, "expected exactly 1 candle in [t0, t1)");
    assert_eq!(
        b_candles[0]["ts"].as_i64(),
        Some(t0),
        "must include lower bound t0 and exclude upper bound t1"
    );

    // 2. Exact half-open [t1, t1 + 1000): must include t1 and exclude t0
    let resp = http_request(
        port,
        "GET",
        &format!("/candles?from={t1}&to={}&interval=1s", t1 + 1000),
        "",
    )
    .await
    .unwrap();
    assert!(resp.contains("200 OK"), "{resp}");
    let b_json: serde_json::Value =
        serde_json::from_str(resp.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let b_candles = b_json["candles"].as_array().unwrap();
    assert_eq!(
        b_candles.len(),
        1,
        "expected exactly 1 candle in [t1, t1 + 1000)"
    );
    assert_eq!(
        b_candles[0]["ts"].as_i64(),
        Some(t1),
        "must include lower bound t1 and exclude prior bucket t0"
    );

    // 3. Empty window: [t0, t0): must return 200 OK with empty candles array
    let resp = http_request(
        port,
        "GET",
        &format!("/candles?from={t0}&to={t0}&interval=1s"),
        "",
    )
    .await
    .unwrap();
    assert!(resp.contains("200 OK"), "{resp}");
    let b_json: serde_json::Value =
        serde_json::from_str(resp.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let b_candles = b_json["candles"].as_array().unwrap();
    assert!(
        b_candles.is_empty(),
        "empty window [t0, t0) must return 0 candles"
    );

    // 4. Maximum-length window: exactly 86_400_000 ms (24 hours)
    let max_from = to - 86_400_000;
    let resp = http_request(
        port,
        "GET",
        &format!("/candles?from={max_from}&to={to}&interval=1s"),
        "",
    )
    .await
    .unwrap();
    assert!(
        resp.contains("200 OK"),
        "24h maximum boundary window must return 200 OK: {resp}"
    );

    // 5. Exceeding maximum-length window: 86_400_001 ms (24h + 1ms)
    let over_from = to - 86_400_001;
    let resp = http_request(
        port,
        "GET",
        &format!("/candles?from={over_from}&to={to}&interval=1s"),
        "",
    )
    .await
    .unwrap();
    assert!(
        resp.contains("400 Bad Request"),
        "over-24h window must return 400 Bad Request: {resp}"
    );

    // ── Focused Verification: Elapsed-Time Deadline Flush Contract ──────
    // Stop feed so no new trades are arriving and allow pending queue to commit
    let resp = http_request(port, "POST", "/feed-rate", "{\"orders_per_second\":0}")
        .await
        .unwrap();
    assert!(resp.contains("200 OK"));
    tokio::time::sleep(Duration::from_millis(350)).await;

    let health_before = http_request(port, "GET", "/health", "").await.unwrap();
    let json_before: serde_json::Value =
        serde_json::from_str(health_before.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let trades_before = json_before["trades"].as_u64().unwrap();
    let persisted_before = json_before["persisted_rows"].as_u64().unwrap();
    assert_eq!(
        trades_before, persisted_before,
        "prior trades must be fully flushed before deadline test"
    );

    // Turn feed on briefly at 500 orders/sec to generate a partial batch (<< 1,000)
    let _ = http_request(port, "POST", "/feed-rate", "{\"orders_per_second\":500}")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Halt feed again
    let _ = http_request(port, "POST", "/feed-rate", "{\"orders_per_second\":0}")
        .await
        .unwrap();

    let health_mid = http_request(port, "GET", "/health", "").await.unwrap();
    let json_mid: serde_json::Value =
        serde_json::from_str(health_mid.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let new_trades = json_mid["trades"].as_u64().unwrap() - trades_before;
    assert!(
        new_trades > 0 && new_trades < 1000,
        "expected partial batch (< 1000 rows), got {new_trades}"
    );

    // Allow the 100ms elapsed-time flush deadline to expire and commit
    tokio::time::sleep(Duration::from_millis(250)).await;

    let health_after = http_request(port, "GET", "/health", "").await.unwrap();
    let json_after: serde_json::Value =
        serde_json::from_str(health_after.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let persisted_after = json_after["persisted_rows"].as_u64().unwrap();
    assert_eq!(
        persisted_after - persisted_before,
        new_trades,
        "elapsed-time deadline (100ms) must flush partial batch without waiting for BATCH_ROWS (1000)"
    );

    // ── Focused Verification: Deterministic Candle Ordering (ts, seq) ──
    // Insert 4 trades with identical millisecond timestamp and distinct prices into a fresh bucket
    let test_bucket_ms: i64 = 1_850_000_000_000;
    let test_prices = [100i64, 108i64, 95i64, 102i64];
    for (i, &price) in test_prices.iter().enumerate() {
        sqlx::query(
            "INSERT INTO trades (ts, seq, price, qty, taker_side) VALUES (to_timestamp($1 / 1000.0), $2, $3, $4, 0)",
        )
        .bind(test_bucket_ms)
        .bind(10_000_000i64 + i as i64)
        .bind(price)
        .bind(10i64)
        .execute(&pool)
        .await
        .unwrap();
    }

    // Refresh the continuous aggregate window over this bucket so the view is up-to-date
    let _ = sqlx::query("CALL refresh_continuous_aggregate('candles_1s', to_timestamp($1 / 1000.0), to_timestamp($2 / 1000.0))")
        .bind(test_bucket_ms)
        .bind(test_bucket_ms + 1000)
        .execute(&pool)
        .await;

    let candle_resp = http_request(
        port,
        "GET",
        &format!(
            "/candles?from={test_bucket_ms}&to={}&interval=1s",
            test_bucket_ms + 1000
        ),
        "",
    )
    .await
    .unwrap();
    assert!(candle_resp.contains("200 OK"), "{candle_resp}");
    let c_json: serde_json::Value =
        serde_json::from_str(candle_resp.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let candles_arr = c_json["candles"].as_array().unwrap();
    assert_eq!(candles_arr.len(), 1, "expected 1 candle for test bucket");
    let bar = &candles_arr[0];
    assert_eq!(
        bar["open"].as_i64(),
        Some(100),
        "open must strictly equal first trade price (seq 0)"
    );
    assert_eq!(bar["high"].as_i64(), Some(108), "high must equal max price");
    assert_eq!(bar["low"].as_i64(), Some(95), "low must equal min price");
    assert_eq!(
        bar["close"].as_i64(),
        Some(102),
        "close must strictly equal last trade price (seq 3)"
    );

    child.kill().await.expect("kill server");
    let _ = child.wait().await;
    pool.close().await;
}
