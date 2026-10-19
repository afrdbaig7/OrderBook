//! `ws_probe` — a small WebSocket client used for verification and manual
//! testing (the standalone `websocat` replacement).
//!
//! Connects to `/ws`, applies every message through the reference
//! [`ClientBook`], and exits nonzero if the stream desynchronizes:
//! snapshot → contiguous deltas → reconnect handling.

use std::time::{Duration, Instant};

use clap::Parser;
use feed::ClientBook;
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:8080/ws")]
    url: String,
    /// Stop after this many wire messages.
    #[arg(long)]
    count: Option<u64>,
    /// Stop after this many seconds.
    #[arg(long)]
    seconds: Option<u64>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let deadline = args
        .seconds
        .map(|s| Instant::now() + Duration::from_secs(s));

    let (mut socket, _) = connect_async(&args.url).await.expect("connect failed");
    let mut book = ClientBook::new();

    let start = Instant::now();
    let mut messages: u64 = 0;
    let mut trades: u64 = 0;
    let mut deltas: u64 = 0;
    let mut errors: u64 = 0;

    while let Some(item) = socket.next().await {
        match item {
            Ok(WsMessage::Text(text)) => {
                messages += 1;
                match feed::Message::decode(&text) {
                    Ok(message) => {
                        if matches!(message, feed::Message::Trade { .. }) {
                            trades += 1;
                        }
                        if matches!(message, feed::Message::Delta { .. }) {
                            deltas += 1;
                        }
                        book.apply_json(&text).expect("ClientBook parse");
                        if book.is_desynced() {
                            errors += 1;
                            error!("DESYNC at last_seq {}", book.last_seq());
                        }
                    }
                    Err(error) => {
                        errors += 1;
                        error!("bad json: {error}");
                    }
                }
                if args.count.is_some_and(|n| messages >= n) {
                    break;
                }
            }
            Ok(WsMessage::Close(_)) => break,
            Ok(_) => {}
            Err(error) => {
                errors += 1;
                error!("ws error: {error}");
                break;
            }
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    info!(
        messages,
        trades,
        deltas,
        errors,
        last_seq = book.last_seq(),
        synced = book.is_synced(),
        book_size = book.bids().len() + book.asks().len(),
        msg_per_sec = messages as f64 / elapsed,
        "probe complete"
    );

    if errors > 0 || !book.is_synced() {
        error!("probe FAILED");
        std::process::exit(1);
    }
}
