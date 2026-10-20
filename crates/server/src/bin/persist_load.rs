//! Load generator: drives the engine thread and the
//! batched persistence writer (standalone, without HTTP/WebSocket) to:
//!
//! 1. persist ≥ `--target-trades` fills into TimescaleDB, and
//! 2. benchmark the engine hot path with persistence **on** vs **off** to verify
//!    the DB writer adds no measurable latency overhead to matching.
//!
//! Uses the same `feed::SyntheticFeed` and `engine_thread::spawn` the
//! server does, so the benchmark is realistic. Run it twice with the same
//! seed for a fair comparison:
//!
//! ```text
//! cargo run --release -p server --bin persist_load -- --target-trades 1000000 --no-persist
//! cargo run --release -p server --bin persist_load -- --target-trades 1000000 \
//!     --database-url postgres://orderbook:orderbook@localhost:5434/orderbook
//! ```
//!
//! Output is one machine-readable `persist_load:` line per measurement.

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use crossbeam_channel::bounded;
use engine::OrderBook;
use feed::{FeedConfig, SyntheticFeed};
use server::engine_thread::{self, EngineCommand, ServerEvent};
use server::metrics::Metrics;
use server::{ENGINE_CHANNEL_CAPACITY, PERSIST_CAPACITY};
use tokio::sync::{broadcast, watch};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "persist_load",
    about = "Load generator for database persistence throughput"
)]
struct Args {
    /// Target number of fills (trades) to generate before stopping.
    #[arg(long = "target-trades", default_value_t = 1_000_000)]
    target_trades: u64,
    /// Deterministic feed seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Postgres/TimescaleDB DSN. When omitted (or with `--no-persist`) the
    /// engine runs without a persistence writer (the A/B control arm).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// Force the persistence-off control arm even if a DSN is present.
    #[arg(long = "no-persist", default_value_t = false)]
    no_persist: bool,
    /// Optional sustained command rate (commands/second). `0` = unthrottled
    /// (measures peak engine throughput). Set to a value the DB writer can
    /// absorb to persist without drops.
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Safety stop so a misconfigured run cannot spin forever.
    #[arg(long = "timeout-secs", default_value_t = 300)]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();
    let persist = args.database_url.is_some() && !args.no_persist;

    let metrics = Metrics::new();
    let (cmd_tx, cmd_rx) = bounded::<EngineCommand>(ENGINE_CHANNEL_CAPACITY);
    let (broadcast_tx, _) = broadcast::channel::<Arc<ServerEvent>>(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (db_tx, db_rx) = if persist {
        let (tx, rx) = bounded::<engine_thread::TradeRow>(PERSIST_CAPACITY);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let engine = engine_thread::spawn(
        OrderBook::new(),
        cmd_rx,
        broadcast_tx,
        db_tx,
        metrics.clone(),
    );

    // The writer runs on a worker thread while the producer loop below blocks
    // the main thread, so it drains the bounded channel concurrently.
    let db_task = match db_rx {
        Some(rx) if persist => {
            let url = args.database_url.clone().unwrap();
            let persistence: server::state::Persistence = Arc::new(tokio::sync::RwLock::new(None));
            Some(tokio::spawn(server::db::run(
                url,
                rx,
                metrics.clone(),
                shutdown_rx.clone(),
                persistence,
            )))
        }
        _ => None,
    };

    println!(
        "persist_load: seed={} target_trades={} persistence={} rate={}",
        args.seed, args.target_trades, persist, args.rate
    );

    let mut feed = SyntheticFeed::new(FeedConfig {
        seed: args.seed,
        ..FeedConfig::default()
    });

    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.timeout_secs);
    let mut last_check = Instant::now();
    let mut commands: u64 = 0;
    let rate = args.rate;

    // Tight, unpaced production: `send` blocks on a full channel, so the loop
    // is bounded by the engine's true consumption rate (not by us). With
    // `--rate > 0` we additionally pace to a sustained target so the DB writer
    // (draining on other threads) can keep up without dropping rows.
    loop {
        if cmd_tx
            .send(EngineCommand::Feed(feed.next_command()))
            .is_err()
        {
            break;
        }
        commands += 1;

        if rate > 0 {
            let expected = Duration::from_secs_f64(commands as f64 / rate as f64);
            let elapsed = start.elapsed();
            if expected > elapsed {
                std::thread::sleep(expected - elapsed);
            }
        }

        if last_check.elapsed() >= Duration::from_millis(20) {
            last_check = Instant::now();
            if metrics.trades.load(std::sync::atomic::Ordering::Relaxed) >= args.target_trades {
                break;
            }
            if Instant::now() >= deadline {
                eprintln!("persist_load: timeout after {commands} commands");
                break;
            }
        }
    }

    let producer_elapsed = start.elapsed();
    let producer_elapsed_s = producer_elapsed.as_secs_f64().max(1e-9);

    // Stop feeding; engine drains queued commands then exits, dropping its
    // persistence sender so the writer flushes its tail and returns.
    drop(cmd_tx);
    let _ = engine.join();
    let engine_elapsed = start.elapsed();
    let engine_elapsed_s = engine_elapsed.as_secs_f64().max(1e-9);

    let _ = shutdown_tx.send(true);
    if let Some(task) = db_task {
        let _ = task.await;
    }
    let persist_elapsed = start.elapsed();
    let persist_elapsed_s = persist_elapsed.as_secs_f64().max(1e-9);

    let snap = metrics.snapshot();
    println!(
        "persist_load: [producer]    elapsed_ms={:.1} commands={} commands_per_sec={:.0}",
        producer_elapsed.as_secs_f64() * 1000.0,
        commands,
        commands as f64 / producer_elapsed_s,
    );
    println!(
        "persist_load: [engine]      elapsed_ms={:.1} events={} trades={} \
         commands_per_sec={:.0} events_per_sec={:.0} trades_per_sec={:.0}",
        engine_elapsed.as_secs_f64() * 1000.0,
        snap.events_out,
        snap.trades,
        commands as f64 / engine_elapsed_s,
        snap.events_out as f64 / engine_elapsed_s,
        snap.trades as f64 / engine_elapsed_s,
    );
    println!(
        "persist_load: [persistence] elapsed_ms={:.1} persisted_rows={} persisted_per_sec={:.0} persistence_drops={} feed_drops={}",
        persist_elapsed.as_secs_f64() * 1000.0,
        snap.persisted_rows,
        snap.persisted_rows as f64 / persist_elapsed_s,
        snap.persistence_drops,
        snap.feed_drops,
    );
}
