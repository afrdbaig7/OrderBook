//! `e2e_latency` — end-to-end latency of the running system (Phase G gate).
//!
//! Connects to `/ws`, and for every `trade` message compares the server-side
//! `ts` stamp against the local clock on receipt. Because the server stamps
//! at engine time and the client observes the same two-pipe host, that
//! difference measures feed → engine → broadcast → WebSocket → reader with
//! the process-local overhead included. Reported as an HDR histogram.

use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:8080/ws")]
    url: String,
    /// How long to sample, in seconds.
    #[arg(long, default_value_t = 10)]
    seconds: u64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let deadline = Instant::now() + Duration::from_secs(args.seconds);

    let (mut socket, _) = connect_async(&args.url).await.expect("connect failed");
    let mut histogram = Histogram::<u64>::new(3).expect("histogram");
    let mut trades: u64 = 0;

    while let Some(item) = socket.next().await {
        match item {
            Ok(WsMessage::Text(text)) => {
                if let Ok(feed::Message::Trade { ts, .. }) = feed::Message::decode(&text) {
                    let received = now_ms();
                    if ts > 0 && received >= ts {
                        let latency_ms = received - ts;
                        histogram.record(latency_ms).ok();
                        trades += 1;
                    }
                }
            }
            Ok(WsMessage::Ping(payload)) => {
                let _ = socket
                    .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                    .await;
            }
            Ok(WsMessage::Close(_)) => break,
            Ok(_) => {}
            Err(error) => {
                warn!("ws error: {error}");
                break;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
    }

    if trades == 0 {
        warn!("no trades observed");
        return;
    }

    let p50 = histogram.value_at_quantile(0.50);
    let p99 = histogram.value_at_quantile(0.99);
    let p999 = histogram.value_at_quantile(0.999);
    let max = histogram.max();
    let avg = histogram.mean();
    let per_sec = trades as f64 / args.seconds as f64;
    info!(
        trades,
        trades_per_sec = per_sec,
        p50_ms = p50,
        p99_ms = p99,
        p99_9_ms = p999,
        avg_ms = avg,
        max_ms = max,
        "e2e latency (ms)"
    );
}
