//! End-to-end integration test: spawns the real `server` binary with no
//! database and drives it over real HTTP/WebSocket sockets.
//!
//! Verifies end-to-end integration on a live process:
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
async fn live_server_stream() {
    let port = free_port().await;
    let bin = env!("CARGO_BIN_EXE_server");

    let mut child = Command::new(bin)
        .args([
            format!("--port={port}"),
            "--feed-rate=5000".to_string(),
            "--seed=42".to_string(),
            "--allow-public-feed-rate".to_string(),
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

    // 3. Live rate control & bounds validation.
    let post = http_request(port, "POST", "/feed-rate", r#"{"orders_per_second":3000}"#)
        .await
        .expect("post /feed-rate");
    assert!(post.contains("200 OK"), "{post}");
    assert!(post.contains("3000"), "{post}");
    let get = http_request(port, "GET", "/feed-rate", "")
        .await
        .expect("get /feed-rate");
    assert!(get.contains("\"orders_per_second\":3000"), "{get}");

    // Reject excessive feed rate.
    let post_overflow = http_request(
        port,
        "POST",
        "/feed-rate",
        r#"{"orders_per_second":100001}"#,
    )
    .await
    .expect("post /feed-rate overflow");
    assert!(post_overflow.contains("400 Bad Request"), "{post_overflow}");

    // 4. Candles query validation and persistence availability.
    // Invalid interval
    let bad_interval = http_request(port, "GET", "/candles?from=1&to=2&interval=5s", "")
        .await
        .expect("get /candles bad interval");
    assert!(bad_interval.contains("400 Bad Request"), "{bad_interval}");

    // Negative 'from'
    let negative_from = http_request(port, "GET", "/candles?from=-1&to=1000&interval=1s", "")
        .await
        .expect("get /candles negative from");
    assert!(negative_from.contains("400 Bad Request"), "{negative_from}");

    // 'to' before 'from'
    let inverted_range = http_request(port, "GET", "/candles?from=2000&to=1000&interval=1s", "")
        .await
        .expect("get /candles inverted");
    assert!(
        inverted_range.contains("400 Bad Request"),
        "{inverted_range}"
    );

    // Exceeding max range (24h = 86_400_000 ms) by 1 ms returns 400.
    let excessive_range = http_request(port, "GET", "/candles?from=0&to=86400001&interval=1s", "")
        .await
        .expect("get /candles excessive range");
    assert!(
        excessive_range.contains("400 Bad Request"),
        "{excessive_range}"
    );

    // Exact 24-hour boundary [0, 86400000) is valid (passes validation, returns 503 without DB).
    let max_valid_range = http_request(port, "GET", "/candles?from=0&to=86400000&interval=1s", "")
        .await
        .expect("get /candles max valid range");
    assert!(max_valid_range.contains("503"), "{max_valid_range}");

    // Empty half-open window [1000, 1000) is valid (passes validation, returns 503 without DB).
    let empty_window = http_request(port, "GET", "/candles?from=1000&to=1000&interval=1s", "")
        .await
        .expect("get /candles empty window");
    assert!(empty_window.contains("503"), "{empty_window}");

    // Standard valid range correctly reports 503 unavailable when persistence is disabled.
    let candles = http_request(port, "GET", "/candles?from=1000&to=2000&interval=1s", "")
        .await
        .expect("get /candles");
    assert!(candles.contains("503"), "{candles}");

    // 5. Health reports the rate.
    let health = http_request(port, "GET", "/health", "")
        .await
        .expect("get /health");
    assert!(health.contains("\"feed_rate\":3000"), "{health}");

    child.kill().await.expect("kill server");
    let _ = child.wait().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_client_eviction_and_recovery() {
    let port = free_port().await;
    let bin = env!("CARGO_BIN_EXE_server");

    // Launch server with small broadcast capacity (32 messages) and fast feed (2000 msgs/s)
    let mut child = Command::new(bin)
        .args([
            format!("--port={port}"),
            "--feed-rate=2000".to_string(),
            "--seed=42".to_string(),
            "--broadcast-capacity=32".to_string(),
            "--allow-public-feed-rate".to_string(),
        ])
        .env("RUST_LOG", "warn")
        .env("RUST_BACKTRACE", "1")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    wait_for_health(port).await;

    // 1. Slow client connects, reads snapshot, then stops reading.
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (mut slow_socket, _) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("ws connect")
    .expect("ws handshake");

    // Read the snapshot
    let first = slow_socket.next().await.expect("snapshot").expect("ws ok");
    assert!(first.is_text(), "expected snapshot text");

    // Intentionally stall for 200ms: at 2000 msgs/s, ~400 messages are produced,
    // easily overflowing the 32-slot ring buffer and causing RecvError::Lagged.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The server should evict the slow subscriber by closing the connection.
    let mut closed = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_millis(500), slow_socket.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) => {
                closed = true;
                break;
            }
            Ok(None) => {
                closed = true;
                break;
            }
            Ok(Some(Err(_))) => {
                closed = true;
                break;
            }
            Ok(Some(Ok(_))) => {
                // Drain any frames already buffered before eviction
            }
            Err(_) => {}
        }
    }
    assert!(closed, "slow client was not evicted/closed as expected");

    // 2. Client recovers: throttle feed rate so reconnected client receives messages comfortably.
    let _ = http_request(port, "POST", "/feed-rate", r#"{"orders_per_second":100}"#)
        .await
        .expect("throttle feed rate");

    let mut book = ClientBook::new();
    assert_continuous_stream(port, &mut book, 50, false).await;
    assert!(!book.is_desynced(), "reconnected client desynced");

    child.kill().await.expect("kill server");
    let _ = child.wait().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_configuration_validation_at_startup() {
    let bin = env!("CARGO_BIN_EXE_server");

    // 1. broadcast-capacity = 0 must be rejected
    let status_cap_zero = Command::new(bin)
        .arg("--broadcast-capacity=0")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .expect("spawn server");
    assert!(
        !status_cap_zero.success(),
        "broadcast-capacity=0 must exit with non-zero code"
    );

    // 2. feed-rate = 0 must be rejected
    let status_feed_zero = Command::new(bin)
        .arg("--feed-rate=0")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .expect("spawn server");
    assert!(
        !status_feed_zero.success(),
        "feed-rate=0 must exit with non-zero code"
    );

    // 3. feed-rate > 100,000 must be rejected
    let status_feed_high = Command::new(bin)
        .arg("--feed-rate=150000")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .expect("spawn server");
    assert!(
        !status_feed_high.success(),
        "excessive feed-rate must exit with non-zero code"
    );

    // 4. snapshot-depth > 0 must be rejected (snapshots must always be complete)
    let status_snap_positive = Command::new(bin)
        .arg("--snapshot-depth=100")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .expect("spawn server");
    assert!(
        !status_snap_positive.success(),
        "positive snapshot-depth must exit with non-zero code"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn server_bootstrap_snapshot_includes_all_resting_levels_exceeding_default_display_limit() {
    let port = free_port().await;
    let bin = env!("CARGO_BIN_EXE_server");

    let mut child = Command::new(bin)
        .args([
            format!("--port={port}"),
            "--feed-rate=5000".to_string(),
            "--seed=42".to_string(),
            "--allow-public-feed-rate".to_string(),
        ])
        .env("RUST_LOG", "warn")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    wait_for_health(port).await;

    // Allow feed to seed dozens of distinct price levels across both sides
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let url = format!("ws://127.0.0.1:{port}/ws");
    let (mut socket, _) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("ws connect timeout")
    .expect("ws handshake");

    let snap_frame = timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("snap timeout")
        .expect("snap stream end");

    let snap_text = match snap_frame.unwrap() {
        tokio_tungstenite::tungstenite::Message::Text(t) => t,
        other => panic!("expected text snapshot, got: {other:?}"),
    };

    let decoded = feed::Message::decode(&snap_text).expect("decode snapshot");
    match decoded {
        feed::Message::Snapshot(snap) => {
            // Verify that the server sends a complete bootstrap snapshot that exceeds
            // the historical 20-level display truncation limit.
            assert!(
                snap.bids.len() > 20,
                "expected bootstrap bids to exceed historical 20-level limit, got {}",
                snap.bids.len()
            );
            assert!(
                snap.asks.len() > 20,
                "expected bootstrap asks to exceed historical 20-level limit, got {}",
                snap.asks.len()
            );
        }
        other => panic!("expected Message::Snapshot, got: {other:?}"),
    }

    child.kill().await.expect("kill child");
    let _ = child.wait().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forced_process_kill_with_connected_clients() {
    let port = free_port().await;
    let bin = env!("CARGO_BIN_EXE_server");

    let mut child = Command::new(bin)
        .args([
            format!("--port={port}"),
            "--feed-rate=2000".to_string(),
            "--seed=42".to_string(),
            "--allow-public-feed-rate".to_string(),
        ])
        .env("RUST_LOG", "warn")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    wait_for_health(port).await;

    // Connect active WebSocket client
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (mut socket, _) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("ws connect")
    .expect("ws handshake");

    // Read snapshot
    let snap_item = timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("snap timeout")
        .expect("snap stream end");
    assert!(snap_item.unwrap().is_text(), "expected snapshot");

    // Read a few live messages to establish active streaming
    for _ in 0..5 {
        let msg = timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("msg timeout")
            .expect("stream ended");
        assert!(msg.unwrap().is_text());
    }

    // Forceful process kill demonstrates immediate termination
    child.kill().await.expect("kill child");
    let exit_status = timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("server failed to exit within 5 seconds")
        .expect("wait error");

    // Sockets are dropped abruptly upon SIGKILL/TerminateProcess.
    // Drain any remaining buffered frames in the TCP receive window until EOF or error.
    let mut socket_terminated = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_millis(500), socket.next()).await {
            Ok(Some(Ok(_buffered))) => {
                // Drain in-flight frame delivered prior to process termination
            }
            Ok(Some(Err(_))) | Ok(None) => {
                socket_terminated = true;
                break;
            }
            Err(_) => {
                // No more frames arrived after process kill
                socket_terminated = true;
                break;
            }
        }
    }
    assert!(
        socket_terminated,
        "expected socket termination on forced kill within deadline"
    );
    let _ = exit_status;
}

/// Integration test: graceful process shutdown on SIGTERM (Unix only).
///
/// Verifies process-level signal handling:
/// 1. The server handles SIGTERM cooperatively and exits with status code 0 within deadline.
/// 2. Active WebSocket connections receive a clean protocol Close frame before termination.
///
/// Scope note: This end-to-end process test does not attach a live database or assert persisted
/// trade counts; component-level queue draining and zero trade loss to persistence are independently
/// verified in `crates/server/tests/shutdown.rs`.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn graceful_process_shutdown_via_sigterm() {
    let port = free_port().await;
    let bin = env!("CARGO_BIN_EXE_server");

    let mut child = Command::new(bin)
        .args([
            format!("--port={port}"),
            "--feed-rate=2000".to_string(),
            "--seed=42".to_string(),
            "--allow-public-feed-rate".to_string(),
        ])
        .env("RUST_LOG", "warn")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    wait_for_health(port).await;

    // Connect active WebSocket client
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (mut socket, _) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("ws connect")
    .expect("ws handshake");

    // Read snapshot
    let snap_item = timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("snap timeout")
        .expect("snap stream end");
    assert!(snap_item.unwrap().is_text(), "expected snapshot");

    // Read a few live messages to establish active streaming
    for _ in 0..5 {
        let msg = timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("msg timeout")
            .expect("stream ended");
        assert!(msg.unwrap().is_text());
    }

    // Send SIGTERM to initiate cooperative graceful shutdown
    let pid = child.id().expect("child pid") as i32;
    let kill_res = unsafe { libc::kill(pid, libc::SIGTERM) };
    assert_eq!(kill_res, 0, "kill(SIGTERM) should succeed");

    // Server should exit successfully (code 0) within deadline upon SIGTERM
    let exit_status = timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("server failed to exit within 10s deadline upon SIGTERM")
        .expect("wait error");

    assert!(
        exit_status.success(),
        "graceful SIGTERM shutdown should exit with code 0, got: {exit_status:?}"
    );

    // Verify active WebSocket connection was cleanly closed with a Close frame
    let mut received_close_frame = false;
    while let Ok(Some(msg)) = timeout(Duration::from_millis(500), socket.next()).await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                received_close_frame = true;
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    assert!(
        received_close_frame,
        "expected WebSocket Close frame upon server graceful shutdown"
    );
}
