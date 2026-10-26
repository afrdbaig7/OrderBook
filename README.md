# Order Book

An experimental Rust matching engine with a synthetic market-data dashboard. Built to explore deterministic execution semantics, stream reconstruction, failure recovery, and real-time continuous aggregation.

---

## Quick Start (Full Stack)

Start the entire system—TimescaleDB, Rust backend server, and Nginx-proxied React dashboard—with one command:

```bash
docker compose up --build
```

Open **<http://127.0.0.1:5173>** in your browser.

![Order Book Dashboard](docs/dashboard.png)

* The web dashboard displays the real-time depth ladder, live trade tape, 1-second candlestick chart, and throughput/price readouts (mid, spread, last, msg/s, trd/s, seq).
* In the local Docker stack, public feed rate control is enabled (`ALLOW_PUBLIC_FEED_RATE=true`) so you can experiment with different workloads directly from the web header. Published ports are bound strictly to `127.0.0.1` for local safety.
* To stop the stack while preserving database data: `docker compose down`.
* To stop and permanently remove database volumes: `docker compose down -v`.

---

## Runnable Library Example

The matching engine core ([`crates/engine`](crates/engine)) is a pure synchronous library with zero async, locking, or network dependencies. You can run the standalone matching example without starting Docker or the web interface:

```bash
cargo run -p engine --example basic_matching
```

This demonstrates:
1. Submitting resting limit orders (bids and asks).
2. Submitting an aggressive crossing limit order (triggering maker-price execution and resting remainder).
3. Cancelling an open resting order.
4. Capturing a complete point-in-time L2 snapshot with monotonically assigned sequence numbers.

---

## Architectural Overview

The matching core is isolated from network I/O, asynchronous execution, and database persistence:

```text
 ┌───────────────────────────┐      ┌─────────────────────────────┐      ┌──────────────────────────┐
 │     Feed Task (Tokio)     │      │  Engine Thread (std::thread)│      │  Broadcast Stream (Tokio)│
 │                           │      │                             │      │                          │
 │  SyntheticFeed            │      │  Exclusive single-thread    │      │  ServerEvent broadcast   │
 │  .next_command()          │───►  │  ownership (no async,       │───►  │  to connected WebSockets │
 │                           │      │  no locks on the book)      │      │  (/ws)                   │
 └───────────────────────────┘      └──────────────┬──────────────┘      └──────────────────────────┘
                                                   │
                                                   │ trades (try_send)
                                                   ▼
                                    ┌─────────────────────────────┐
                                    │    DB Writer Task (Tokio)   │
                                    │                             │
                                    │  Batched unnest insert      │───►  TimescaleDB (trades)
                                    │  (drops + counts if full)   │      Continuous aggregate (candles_1s)
                                    └─────────────────────────────┘
```

### Component Responsibilities

| Component | Path | Responsibility |
|---|---|---|
| **`engine`** | [`crates/engine`](crates/engine) | Pure matching core: integer prices, FIFO price levels, maker-price execution, sequence assignment, complete snapshots, and property tests. Enforces `#![forbid(unsafe_code)]`. |
| **`feed`** | [`crates/feed`](crates/feed) | Wire protocol serialization, deterministic synthetic order flow, and reference client-side book reconstruction (`ClientBook`). Enforces `#![forbid(unsafe_code)]`. |
| **`server`** | [`crates/server`](crates/server) | Dedicated OS matching thread, Tokio async runtime, WebSocket/HTTP endpoints, bounded channel coordination, metrics, and batched TimescaleDB writer. |
| **`bench`** | [`crates/bench`](crates/bench) | Criterion micro-benchmarks and HDR histogram latency harness. |
| **`replay`** | [`crates/replay`](crates/replay) | Offline command recording and verification of engine events and final state. |
| **`web`** | [`web`](web) | Vite, React, and TypeScript dashboard: live depth ladder, trade tape, 1s candlestick chart (`lightweight-charts`), and feed rate control. |

### Synchronization & Backpressure Model

* **Exclusive Single-Thread Ownership**: The order book is owned by a single standard OS thread without locks or async runtimes. Tokio tasks communicate with the engine thread strictly through bounded `crossbeam-channel` queues.
* **WebSocket Backpressure**: Live market events are broadcast through a bounded ring buffer (`BROADCAST_CAPACITY = 65,536`). If a slow consumer lags behind the buffer capacity, the server closes the connection, forcing the client to reconnect and resynchronize with a fresh snapshot.
* **Persistence Decoupling**: Executed trades are buffered in a bounded channel (`PERSIST_CAPACITY = 100,000`). If TimescaleDB falls behind, incoming trades are dropped and counted in the `persistence_drops` metric—disk I/O never blocks order matching.
* **Persistence Scope**: TimescaleDB stores executed trade market data and generates 1-second continuous aggregate candles (`candles_1s`). It is an analytical market-data store, **not** an order-level transaction journal or crash-recovery ledger.

---

## Design Invariants

* **Integer Prices & Quantities**: All prices and quantities in the matching engine are integers (`u64`/`i64`); floating-point math is forbidden in the book.
* **Maker-Price Execution**: Fills always execute at the resting maker order's price.
* **FIFO Priority**: Orders at the same price level execute in first-in, first-out order.
* **Complete Bootstrap Snapshots**: Bootstrap snapshots return all resting levels up to `MAX_BOOK_LEVELS_PER_SIDE = 10,000`. Visual depth truncation is managed strictly on the client.
* **Sequencing**: The engine assigns every sequence number (`seq`). Every event—including fills, cancellations, and rejections—consumes exactly one sequence number.
* **Quantity Conservation**: Checked with property tests (`proptest`):
  $$\text{submitted} = 2 \times \text{fills} + \text{cancelled} + \text{dropped} + \text{resting}$$

---

## Verified Benchmarks

Detailed methodology, workload descriptions, and instructions to reproduce are documented in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

> **Hardware & Environment**: Intel Core i5-12500H (12 cores / 16 threads), 16 GB RAM, Windows 11, Rust stable toolchain. Baseline measurements reflect commit `8d235ec` with LTO enabled.

### 1. In-Memory Engine Micro-Benchmarks (Criterion)

Measurements reflect single-threaded CPU execution time of library calls on [`OrderBook`](crates/engine/src/book.rs) (excluding network, serialization, and disk I/O):

| Operation | Depth 10 | Depth 1,000 | Depth 100,000 |
|---|---|---|---|
| **`submit_limit` (non-crossing batch)** | **255 ns** | **266 ns** | **542 ns** |
| **`match_sweep`** | **4.69 µs** | **382 µs** | **75.75 ms** |
| **`cancel` (short queue)** | **378 ns** | **857 ns** | **4.86 µs** |

* **Workload Scope**: The ~255 ns figure represents the *average amortized insertion cost* of adding a growing batch of 1,000 distinct price levels starting from depth 10.
* **Queue Length Cancellation**: Orders at a single price level are stored in a `VecDeque`, requiring an $O(n)$ search to cancel mid-queue. Measured scaling across queue depths (via `bench_cancel_queue_length`):
  * 4 orders/level: **~350 ns**
  * 50 orders/level: **~580 ns**
  * 500 orders/level: **~2.8 µs**

### 2. Server Throughput & Persistence Overhead

Matching throughput measured over 1,000,000 synthetic events:

* **Engine standalone**: **2,139,451 events/second**
* **Engine with TimescaleDB writer attached**: **2,133,230 events/second** ($\pm 0.3\%$ difference)
* **Trades persisted**: 1,000,082 trades generated and persisted with **0 dropped trades**.

### 3. End-to-End WebSocket Delivery Latency (HDR Histogram)

Measured over localhost loopback from server event timestamp (immediately post-match) to client receipt and JSON parsing (excludes engine matching and inbound network transit):

| Feed Rate (orders/s) | p50 | p99 | p99.9 | Max |
|---|---|---|---|---|
| **1,000** | 0 ms | 1 ms | 2 ms | 5 ms |
| **5,000** | 1 ms | 4 ms | 5 ms | 11 ms |
| **20,000** | 4 ms | 13 ms | 17 ms | 28 ms |
| **50,000** | 11 ms | 779 ms | 796 ms | 988 ms |

*Stream Health over 60 seconds at 5,000 orders/s: 298,185 messages, 84,413 trades, 0 sequence gaps, client synchronized throughout.*

---

## Command Recording and Replay

Run a recorded scenario without a server or database:

```bash
cargo run --locked -p replay -- verify docs/recordings/basic.recording.jsonl
```

The example covers FIFO partial fills, cancellation, unfilled market quantity,
and rejected commands. Verification compares every engine event (including
maker/taker IDs) and the complete final snapshot.

To create a recording from your own JSON Lines command file:

```bash
cargo run --locked -p replay -- record --commands docs/recordings/basic.commands.jsonl --output my-recording.jsonl
cargo run --locked -p replay -- verify my-recording.jsonl
```

Output files must be new; the recorder refuses to overwrite existing files.
Missing records, incomplete output, unsupported versions, and mismatched
results fail verification with a nonzero exit code.

This first version records offline scenarios from an empty book. It does not
capture the running server or provide crash recovery. See [the recording format
and API](docs/REPLAY.md) for details and limits.

## Native Development Workflows

You do not need Docker or Node to develop and test the Rust matching engine.

### Prerequisites

* Rust stable toolchain (`rustup default stable`)
* Node.js v20+ (only for web dashboard)
* Docker (only for TimescaleDB integration)

### Running Without Docker (Server Only)

#### POSIX (Linux)
```bash
cargo run -p server --bin server -- --port 8080 --feed-rate 1000 --seed 42
```

#### PowerShell (Windows)
```powershell
cargo run -p server --bin server -- --port 8080 --feed-rate 1000 --seed 42
```

Endpoints available:
* `GET  /health` — Liveness, live counters, sequence number, and channel depths.
* `GET  /ws` — WebSocket market data (bootstrap snapshot followed by contiguous deltas and trades).
* `GET  /candles?from=<ms>&to=<ms>&interval=1s` — 1s OHLCV bars over `[from, to)` (`503` when running without database).
* `GET  /feed-rate` · `POST /feed-rate {"orders_per_second": N}` — Live feed rate control (maximum 100,000).

### Native Development with TimescaleDB

To run TimescaleDB locally for persistence tests and candle aggregation:

```bash
# Start TimescaleDB with host port 5434 published
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d timescaledb

# Run server with persistence
DATABASE_URL="postgres://orderbook:orderbook@localhost:5434/orderbook" \
  cargo run -p server --bin server -- --feed-rate 2000
```

### Running the Web Dashboard

```bash
cd web
npm ci
npm run dev          # http://localhost:5173, dev proxy forwards /ws and API to :8080
```

---

## Verification & Testing

The test suite explicitly establishes what is verified versus what is out of scope:

| Check | What it establishes |
|---|---|
| **HTML loads** | Static frontend assets are delivered correctly through reverse proxy |
| **Browser renders** | UI components mount, initialize store, and render depth ladder |
| **Sequence probe passes** | WebSocket stream is contiguous; zero sequence gaps or dropped deltas |
| **Client state matches engine** | Complete bootstrap snapshot + delta reconstruction preserves full depth across boundary changes |
| **Graceful shutdown** | Component test verifies queue draining to persistence writer (`shutdown.rs`); integration test verifies SIGTERM exits with code 0 and emits WebSocket `Close` frames (`e2e.rs`) |

### Automated Commands

```bash
# Code formatting and linter checks
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings

# All workspace tests (44 engine tests, 14 feed tests, server e2e + graceful shutdown)
cargo test --workspace

# Database persistence & continuous aggregate migration test (requires TimescaleDB on 5434)
TEST_DATABASE_URL="postgres://orderbook:orderbook@localhost:5434/orderbook" \
  cargo test -p server --test persistence -- --nocapture

# Frontend tests and production build
cd web && npm ci && npm test && npm run build

# Automated full-stack container smoke test (runs in isolated test project, preserves demo data)
# POSIX:
./scripts/smoke_test.sh
# Windows PowerShell:
.\scripts\smoke_test.ps1
```

---

## Database Migration & Upgrades

The schema defines continuous aggregates using `first(price, seq)` and `last(price, seq)` with real-time continuous aggregation enabled (`timescaledb.materialized_only = false`).

To upgrade an existing installation created under earlier timestamp-based definitions:

```bash
psql "$DATABASE_URL" -f db/migrations/002_rebuild_candles_with_sequence.sql
```

Applying `db/schema.sql` directly also detects whether `candles_1s` lacks sequence ordering and rebuilds the view and refresh policy automatically without dropping historical raw trade data.

The server enforces strict input boundaries:
* `POST /feed-rate`: Validated to `0..=100_000` orders/second. Requests exceeding this return `400 Bad Request`.
* `GET /candles`: Evaluates the half-open interval `[from, to)`. Requires `interval=1s`, `from >= 0`, `to >= from`, and `(to - from) <= 86,400,000` ms (maximum 24-hour window, up to 86,400 bars). Requests exceeding these bounds return `400 Bad Request`.

---

## Known Limitations & Non-Goals

To maintain clear architectural boundaries and high code quality:
1. **Single Symbol**: The matching core maintains a single order book; multi-asset routing is not currently implemented.
2. **Synthetic Order Flow**: Market data is driven by a deterministic in-process synthetic generator, not an ITCH/OUCH, FIX, or live exchange gateway.
3. **Discrete Orders vs. Aggregate L2**: The generator produces discrete limit and cancel orders (L3); public exchange feeds (such as Binance `depthUpdate`) stream aggregated price-level volumes (L2) and require a separate order-reconstruction path.
4. **JSON Wire Protocol**: WebSocket frames use tagged JSON text rather than binary serialization (e.g. SBE, FlatBuffers).
5. **O(n) Cancellation**: Orders within a single price level are stored in a `VecDeque`, making cancellation $O(n)$ in level depth.
6. **Localhost Latency**: Benchmarks reflect loopback communication on a single workstation, not real-world wide-area network latency.
7. **Deterministic Candle Sequencing**: In `candles_1s`, `open` and `close` use `first(price, seq)` and `last(price, seq)` to sequence trades deterministically by monotonic sequence number when multiple trades execute within the same millisecond timestamp. Real-time aggregation is enabled (`materialized_only = false`) so queries incorporate raw trades from the live unmaterialized edge.
8. **Duplicate Order IDs**: A duplicate `OrderId` is rejected while it rests in the book. If an order has already fully filled or been cancelled and removed, re-using that `OrderId` is not tracked (documented engine boundary; the engine stores market data rather than a durable transaction ledger).

---

## Public Browser Demo Architecture: Hosted Backend vs. WASM Engine (Future Work)

A publicly accessible browser demo can be approached via two distinct architectures with different operational tradeoffs:

| Approach | Architecture | Advantages | Operational Tradeoffs |
|---|---|---|---|
| **Hosted Backend Demo** | Containerized Rust `server` + TimescaleDB deployed on a cloud container host, streaming market data to the web frontend over WebSockets. | Demonstrates the existing full stack (dedicated OS matching thread, live WebSocket streaming, channel backpressure, and TimescaleDB continuous aggregates) in a live environment. | Incurs recurring cloud infrastructure costs, requires connection concurrency limits, public API rate-limiting, and ongoing backend service monitoring. |
| **Client-Side WASM Demo** | Compiling `crates/engine` and `crates/feed` to WebAssembly (`wasm32-unknown-unknown`) and running the matching loop inside the browser. | Can use free static hosting and requires no running backend after loading; execution remains subject to browser resource limits and tab lifecycle. | Demonstrates the matching engine library in isolation, but bypasses the multi-threaded Tokio server, network socket handling, and TimescaleDB persistence. |

---

## Demonstration

The full stack is verified operating under sustained synthetic load at 5,000 orders/s with TimescaleDB continuous OHLCV aggregation and real-time WebSocket market data updates. Detailed verification logs, health metrics, and inspection captures are documented in [docs/VERIFICATION.md](docs/VERIFICATION.md).

To launch and explore the live interface locally, run `docker compose up -d` and open <http://localhost:5173>.

---

## Contributing & Security

* **[CONTRIBUTING.md](CONTRIBUTING.md)**: Development guidelines, code standards, and PR instructions.
* **[SECURITY.md](SECURITY.md)**: Vulnerability disclosure policy.
* **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)**: Community standards (Contributor Covenant 2.1).

---

## License

This project is dual-licensed under either:
* **[MIT License](LICENSE-MIT)**
* **[Apache License, Version 2.0](LICENSE-APACHE)**

at your option.
