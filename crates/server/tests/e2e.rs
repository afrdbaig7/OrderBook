//! End-to-end integration test: spawns the real `server` binary with no
//! database and drives it over real HTTP/WebSocket sockets.
//!
//! Verifies the Phase D gate on a live process:
//!
//! * `/health` comes up;
//! * a WebSocket client gets a `snapshot` then a *contiguous* `seq` stream
//!   (`delta`/`trade`/`ack`/`cancel`), with no gaps — the same rule the
//!   reference `ClientBook` enforces;
//! * a reconnected client starts from a fresh snapshot and stays contiguous;
//! * `POST /feed-rate` changes the live rate, `/candles` correctly reports
//!   persistence as unavailable.

use std::process::Stdio;
use std::time::Duration;

use feed::{ClientBook, Message};
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::timeout;

/// Picks a free TCP port by binding and immediately releasing `:0`.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

/// Waits until the server answers `/health` with 200, or panics.
async fn wait_for_health(port: u16) {
    for _ in 0..150 {
        if let Ok(body) = http_request(port, "GET", "/health", "").await {
            if body.contains("200 OK") && body.contains("ok") {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become healthy");
}

/// Minimal HTTP/1.1 request over a raw socket; returns the full response.
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

/// Connects and consumes messages, applying them to `book`, asserting
/// snapshot-first and strict `seq` contiguity thereafter.
async fn assert_continuous_stream(
    port: u16,
    book: &mut ClientBook,
    min_messages: usize,
    want_trade: bool,
) {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (mut socket, _) = timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("ws connect")
    .expect("ws handshake");

    // Wait for the snapshot bootstrap.
    let mut snapshot_seq = None;
    while snapshot_seq.is_none() {
        let item = timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("snapshot timeout")
            .expect("stream ended before snapshot");
        match item.expect("ws error") {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                let message = Message::decode(&text).expect("valid wire json");
                if let Message::Snapshot(snapshot) = &message {
                    snapshot_seq = Some(snapshot.seq);
                }
                assert!(
                    matches!(message, Message::Snapshot(_)),
                    "first message must be a snapshot, got {message:?}"
                );
                book.apply_json(&text).expect("apply");
            }
            other => panic!("unexpected ws frame before snapshot: {other:?}"),
        }
    }
    let mut last_seq = snapshot_seq.expect("snapshot seq");

    let window = std::time::Instant::now();
    let mut messages: usize = 0;
    let mut trades: usize = 0;
    let mut deltas: usize = 0;

    while messages < min_messages && window.elapsed() < Duration::from_secs(15) {
        let item = timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("stream timeout")
            .expect("stream ended early");
        match item.expect("ws error") {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                let message = Message::decode(&text).expect("valid wire json");
                assert_eq!(
                    message.seq(),
                    last_seq + 1,
                    "sequence gap after {}. This is a real packet loss.",
                    last_seq
                );
                if matches!(message, Message::Trade { .. }) {
                    trades += 1;
                }
                if matches!(message, Message::Delta { .. }) {
                    deltas += 1;
                }
                book.apply_json(&text).expect("apply");
                assert!(!book.is_desynced(), "ClientBook detected a gap");
                last_seq = message.seq();
                messages += 1;
            }
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }

    assert!(messages >= min_messages, "not enough messages in window");
    assert!(deltas > 0, "expected deltas in the stream");
    if want_trade {
        assert!(
            trades > 0,
            "expected at least one trade; deltas={deltas} messages={messages}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn phase_d_gate_live_process() {
    let port = free_port().await;
    let bin = env!("CARGO_BIN_EXE_server");

    let mut child = Command::new(bin)
        .args([
            format!("--port={port}"),
            "--feed-rate=5000".to_string(),
            "--seed=42".to_string(),
            "--snapshot-depth=20".to_string(),
        ])
        .env("RUST_LOG", "warn")
        .env("RUST_BACKTRACE", "1")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    wait_for_health(port).await;

    // 1. Snapshot + contiguous deltas/trades via the reference client.
    let mut book = ClientBook::new();
    assert_continuous_stream(port, &mut book, 2000, true).await;

    // 2. Reconnect: fresh snapshot, then a contiguous continuation.
    let mut book2 = ClientBook::new();
    assert_continuous_stream(port, &mut book2, 1000, false).await;

    // 3. Live rate control.
    let post = http_request(port, "POST", "/feed-rate", r#"{"orders_per_second":3000}"#)
        .await
        .expect("post /feed-rate");
    assert!(post.contains("200 OK"), "{post}");
    assert!(post.contains("3000"), "{post}");
    let get = http_request(port, "GET", "/feed-rate", "")
        .await
        .expect("get /feed-rate");
    assert!(get.contains("\"orders_per_second\":3000"), "{get}");

    // 4. Candles correctly unavailable without a database.
    let candles = http_request(port, "GET", "/candles?from=1&to=2&interval=1s", "")
        .await
        .expect("get /candles");
    assert!(candles.contains("503"), "{candles}");

    // 5. Health reports the new rate.
    let health = http_request(port, "GET", "/health", "")
        .await
        .expect("get /health");
    assert!(health.contains("\"feed_rate\":3000"), "{health}");

    child.kill().await.expect("kill server");
    let _ = child.wait().await;
}
