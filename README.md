# Order Book

An experimental ultra-low latency Rust matching engine with a synthetic market-data dashboard. Built to explore deterministic execution semantics, stream reconstruction, failure recovery, and real-time continuous aggregation.

> ⚡ **Performance Highlights (Intel Core i5-12500H, Hardware Invariant TSC)**:
> * **14.00 ns** limit order insertion (~71.4 Mops/s batch mean; single-op p50 bracket-adjusted: **16–22 ns**, ~18× faster than baseline)
> * **17.67 ns** constant-time cancellation ($\mathcal{O}(1)$ intrusive doubly-linked list nodes, ~30× to 150× faster)
> * **0 dynamic heap allocations** on the matching hot path (preallocated arena + bitmapped price ladder)
> * **2.13M+ events/second** reference engine throughput in unpaced burst benchmarks; **116.9k events/s** with zero-loss continuous TimescaleDB persistence
> * **Full details & 1,000,000-operation percentiles (p50/p99/p99.9)** in [Verified Benchmarks](#verified-benchmarks)

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

## Runnable Library Examples

The matching engine core ([`crates/engine`](crates/engine)) is a pure synchronous library with zero async, locking, or network dependencies. You can run standalone matching examples without starting Docker or the web interface:

### 1. Reference Engine (`OrderBook`) Example
```bash
cargo run -p engine --example basic_matching
```
Demonstrates dynamic limit order insertion, aggressive crossing maker-price fills, order amendments (`modify`) with FIFO priority preservation, order cancellations, and point-in-time L2 snapshot serialization.

### 2. Ultra-Low Latency Engine (`FlatBook`) Example
```bash
cargo run -p engine --example flat_matching
```
Demonstrates zero-allocation matching using the preallocated order arena, bitmapped price discovery, caller-owned event buffers, order amend with FIFO priority preservation, and snapshot capture with **0 dynamic heap allocations**.

---

## `FlatBook`: Ultra-Low Latency Zero-Allocation Matching Engine

The matching engine crate ([`crates/engine`](crates/engine)) provides two distinct matching backends:
1. **[`OrderBook`](crates/engine/src/book.rs) (Reference Engine)**: General-purpose engine using `BTreeMap` for sorted price levels and `VecDeque` for FIFO queues. Ideal for unbounded price ranges and arbitrary tick intervals.
2. **[`FlatBook`](crates/engine/src/flat/mod.rs) (Optimized Low-Latency Engine)**: Hardware-conscious, cache-friendly architecture designed for sub-15ns order execution with **zero dynamic heap allocations on the hot path** (`#![forbid(unsafe_code)]`).

### Architectural Pillars of `FlatBook`

* **Hierarchical 2-Level Bitmapped Price Discovery**: Resolves Best Bid and Best Ask in ~1 CPU cycle using portable CPU bit-scan intrinsics (`trailing_zeros` for asks, `leading_zeros` for bids) across 64-bit words and a summary bitmask.
* **Preallocated Contiguous Order Arena**: Resting orders are stored in a contiguous `Box<[Slot]>`. A singly-linked free list provides $\mathcal{O}(1)$ slot allocation and reuse without memory fragmentation.
* **Intrusive Doubly-Linked Queue Nodes**: Orders at each price level form an intrusive doubly-linked list using 32-bit indices (`prev`/`next`). This enables **constant-time $\mathcal{O}(1)$ cancellations and amends at any position in the queue** (front, middle, or tail), completely eliminating the $\mathcal{O}(n)$ queue search of `VecDeque` implementations.
* **Tombstone-Free Backward-Shift Deletion**: The `OrderId` to slot index mapping uses an open-addressing hash table with backward-shift deletion. Deletions fill gaps immediately, guaranteeing zero tombstones and preventing probe chain degradation over millions of cycles.
* **Caller-Owned Reusable Event Buffers**: `submit_limit_into`, `modify_into`, and `cancel_into` write directly into preallocated caller-owned buffers, guaranteeing **0 bytes of dynamic heap allocation** during order execution.

### `FlatBook` Benchmark Performance (1,000,000 Operations)

Measured on Intel Core i5-12500H using hardware invariant TSC cycles (`_mm_lfence` + `__rdtscp` + `_mm_lfence`):

| Operation | Batch Mean (Pure CPU) | p50 | p99 | p99.9 | Speedup vs Reference | Heap Allocations |
|---|---|---|---|---|---|:---:|
| **Limit Insert (Existing Level)** | **14.00 ns** (~71.4 Mops/s) | 37 ns | **78 ns** | **113 ns** | **~18× faster** | **0 bytes** |
| **Limit Insert (New Level)** | **14.67 ns** (~68.2 Mops/s) | 31 ns | **44 ns** | **131 ns** | **~18× faster** | **0 bytes** |
| **Order Cancel (Queue Front)** | **22.33 ns** | 33 ns | **55 ns** | **75 ns** | **~16× faster** | **0 bytes** |
| **Order Cancel (Queue Mid / Tail)** | **17.67–18.67 ns** | 33 ns | **55 ns** | **81–90 ns** | **~30× to 150× faster** | **0 bytes** |
| **Order Amend (`modify`)** | **15.20 ns** (queue preserved) | 34 ns | **62 ns** | **94 ns** | **~15× faster** | **0 bytes** |
| **Unknown Order Reject** | **8.40 ns** | 25 ns | **28 ns** | **57 ns** | **~24× faster** | **0 bytes** |
| **Mixed Workload (60/30/10)** | **25.85–35.10 ns** | 28–34 ns | **59–63 ns** | **119–143 ns** | **~9× to 11× faster** | **0 bytes** |

### Running with `FlatBook`

* **Run the Standalone Example**:
  ```bash
  cargo run -p engine --example flat_matching
  ```
* **Run the Backend Server with `FlatBook`**:
  ```bash
  cargo run --release -p server --bin server -- --engine flat --flat-base-price 100000 --flat-tick-count 512
  ```
* **Run Deterministic Replay Verification with `FlatBook`**:
  ```bash
  cargo run --release -p replay -- verify docs/recordings/basic.recording.jsonl --engine flat
  ```
* **Run Hardware TSC Benchmarks**:
  ```bash
  cargo run --release -p bench -- evaluate --samples 1000000
  ```

---

## Architectural Overview

The matching core is isolated from network I/O, asynchronous execution, and database persistence:

```text
 ┌───────────────────────────┐      ┌─────────────────────────────┐      ┌───────────────────────────┐      ┌──────────────────────────┐
 │     Feed Task (Tokio)     │      │ Engine Thread (std::thread) │      │Broadcaster Thread (std)   │      │ Broadcast Stream (Tokio) │
 │                           │      │                             │      │                           │      │                          │
 │  SyntheticFeed            │      │  Zero-Allocation Matching   │      │  JSON encode (serde_json) │───►  │  ServerEvent broadcast   │
 │  .next_command()          │───►  │  Adaptive busy-spin ingress │───►  │  now_ms() timestamping    │      │  to connected WebSockets │
 │                           │      │  SPSC binary event emission │      │  Websocket & DB dispatch  │      │  (/ws)                   │
 └───────────────────────────┘      └──────────────┬──────────────┘      └─────────────┬─────────────┘      └──────────────────────────┘
                                                   │                                   │
                                            (Optional core pin)                        │ trades (try_send)
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
| **`engine`** | [`crates/engine`](crates/engine) | Pure matching core enforcing `#![forbid(unsafe_code)]`. Contains both reference [`OrderBook`](crates/engine/src/book.rs) (`BTreeMap` + `VecDeque`) and ultra-low latency, zero-allocation [`FlatBook`](crates/engine/src/flat/mod.rs) (preallocated order arena, 2-level bitmapped price ladder, open-addressing index). |
| **`feed`** | [`crates/feed`](crates/feed) | Wire protocol serialization, deterministic synthetic order flow, and reference client-side book reconstruction (`ClientBook`). Enforces `#![forbid(unsafe_code)]`. |
| **`server`** | [`crates/server`](crates/server) | Dedicated OS matching thread decoupled from Tokio async runtime via lock-free SPSC event ring, dedicated broadcaster thread, adaptive busy-spin ingress, CPU core pinning (Windows & Linux), WebSocket/HTTP endpoints, and batched TimescaleDB writer. |
| **`bench`** | [`crates/bench`](crates/bench) | Criterion micro-benchmarks, hardware invariant TSC timer (`_mm_lfence` + `__rdtscp`), HDR histogram latency harness, and micro-architectural profiler. |
| **`replay`** | [`crates/replay`](crates/replay) | Offline command recording and verification of engine events and final state. |
| **`web`** | [`web`](web) | Vite, React, and TypeScript dashboard: live depth ladder, trade tape, 1s candlestick chart (`lightweight-charts`), and feed rate control. |

### Synchronization & Backpressure Model

* **Decoupled Matching & Broadcasting**: The matching core executes with zero heap allocations, pushing raw binary `Event`s directly into a preallocated SPSC ring buffer. JSON serialization, `now_ms()` timestamping syscalls, and WebSocket channel sends are offloaded to an independent broadcaster thread.
* **Adaptive Busy-Spin Ingress**: The engine thread employs hardware `spin_loop` (PAUSE instructions) on command ingress for sub-microsecond burst responsiveness without futex sleep/wake penalties, while falling back to short sleep during prolonged idle periods.
* **Dedicated CPU Core Pinning**: The engine thread supports deterministic CPU core affinity pinning on both Windows (`SetThreadAffinityMask`) and Linux (`sched_setaffinity`) via `--engine-core <id>`.
* **WebSocket Backpressure**: Live market events are broadcast through a bounded ring buffer (`BROADCAST_CAPACITY = 65,536`). If a slow consumer lags behind the buffer capacity, the server closes the connection, forcing the client to reconnect and resynchronize with a fresh snapshot.
* **Persistence Decoupling**: Executed trades are buffered in a bounded channel (`PERSIST_CAPACITY = 100,000`). If TimescaleDB falls behind, incoming trades are dropped and counted in the `persistence_drops` metric—disk I/O never blocks order matching.
* **Persistence Scope**: TimescaleDB stores executed trade market data and generates 1-second continuous aggregate candles (`candles_1s`). It is an analytical market-data store, **not** an order-level transaction journal or crash-recovery ledger.

---

## Design Invariants

* **Integer Prices & Quantities**: All prices and quantities in the matching engine are integers (`u64`/`i64`); floating-point math is forbidden in the book.
* **Maker-Price Execution**: Fills always execute at the resting maker order's price.
* **FIFO Priority & Queue Preservation on Amend**: Orders at the same price level execute in first-in, first-out order. Order amendments (`modify`) that preserve or reduce size retain their queue priority without moving; size increases lose priority and move to the tail of the level queue.
* **Complete Bootstrap Snapshots**: Bootstrap snapshots return all resting levels up to `MAX_BOOK_LEVELS_PER_SIDE = 10,000`. Visual depth truncation is managed strictly on the client.
* **Sequencing**: The engine assigns every sequence number (`seq`). Every event—including fills, cancellations, and rejections—consumes exactly one sequence number.
* **Quantity Conservation**: Checked with property tests (`proptest`):
  $$\text{submitted} = 2 \times \text{fills} + \text{cancelled} + \text{dropped} + \text{resting}$$

---

## Verified Benchmarks

Detailed methodology, workload descriptions, and instructions to reproduce are documented in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

> **Hardware & Environment**: Intel Core i5-12500H (12 cores / 16 threads, base 3.11 GHz), 16 GB RAM, Windows 11, Rust stable toolchain with LTO enabled.

### 1. `FlatBook` Ultra-Low Latency & Tail Latency (1,000,000 Operations)

The optimized [`FlatBook`](crates/engine/src/flat/mod.rs) engine eliminates all dynamic heap allocations on the hot path using a preallocated contiguous order arena, two-level hierarchical bitmapped price ladder ($O(1)$ best price discovery), and backward-shift open-addressing hash table ($O(1)$ order lookups and deletions).

Timing is captured with an ordered, serializing hardware invariant TSC counter (`_mm_lfence` + `__rdtscp` + `_mm_lfence`) pinned to a dedicated CPU core (`SetThreadAffinityMask`), with state replenishment performed outside the timed window and full event/snapshot parity verified against [`OrderBook`](crates/engine/src/book.rs) prior to timing (`cargo run --release -p bench -- evaluate --samples 1000000`):

| Operation | Workload Description | Samples | p50 | p99 | p99.9 | Batch Mean | Max (Observed) | Heap Allocations | Target Status |
|---|---|---|---|---|---|---|---|---|---|
| **Limit Insert (Existing)** | Non-crossing insert into existing level | 1,000,000 | 37 ns | **78 ns** | **113 ns** | **14.00 ns** | 249 µs (0.01% >1µs) | **0 (verified)** | p99/p99.9 met (p50 > 25ns) |
| **Limit Insert (New Level)** | Non-crossing insert creating new level | 1,000,000 | 31 ns | **44 ns** | **131 ns** | **14.67 ns** | 356 µs (0.01% >1µs) | **0 (verified)** | p99/p99.9 met (p50 > 25ns) |
| **Cancel (Queue Front)** | Cancellation at front of level | 1,000,000 | 33 ns | **55 ns** | **75 ns** | **22.33 ns** | 53 µs (0.00% >1µs) | **0 (verified)** | **achieved** |
| **Cancel (Queue Mid)** | Cancellation in middle of level | 1,000,000 | 33 ns | **55 ns** | **90 ns** | **17.67 ns** | 601 µs (0.01% >1µs) | **0 (verified)** | **achieved** |
| **Cancel (Queue Tail)** | Cancellation at tail of level | 1,000,000 | 33 ns | **55 ns** | **81 ns** | **18.67 ns** | 460 µs (0.01% >1µs) | **0 (verified)** | **achieved** |
| **Unknown-ID Reject** | Reject unknown cancel order ID | 1,000,000 | 25 ns | **28 ns** | **57 ns** | **8.40 ns** | 57 µs (0.00% >1µs) | **0 (verified)** | **achieved** |
| **Mixed (20% Load)** | Bounded 60% limit / 30% cancel / 10% market | 100,000 | 34 ns | **59 ns** | **143 ns** | **25.85 ns** | 51 µs (0.00% >1µs) | **0 (verified)** | **achieved** |
| **Mixed (50% Load)** | Bounded 60% limit / 30% cancel / 10% market | 100,000 | 29 ns | **61 ns** | **119 ns** | **28.65 ns** | 43 µs (0.00% >1µs) | **0 (verified)** | **achieved** |
| **Mixed (85% Load)** | Bounded 60% limit / 30% cancel / 10% market | 100,000 | 28 ns | **63 ns** | **126 ns** | **35.10 ns** | 13 µs (0.01% >1µs) | **0 (verified)** | **achieved** |
| **Sweep (8 makers)** | Crossing sweep across 4 levels | 10,000 | 121 ns | 180 ns | 887 ns | 125.76 ns | 1.29 µs (0.01% >1µs) | **0 (verified)** | N/A |
| **Sweep (64 makers)** | Crossing sweep across 16 levels | 10,000 | 680 ns | 1,101 ns | 1,869 ns | 694.79 ns | 19.1 µs (1.42% >1µs) | **0 (verified)** | N/A |

* **Zero Allocations**: Confirmed in [`crates/engine/tests/flat_zero_alloc.rs`](crates/engine/tests/flat_zero_alloc.rs) via an atomic allocator counter on the hot path.
* **Single-Op vs. Batch Overhead**: The serializing bracket overhead (`_mm_lfence` + `__rdtscp`) accounts for ~15 ns of single-operation p50 measurements; batched measurements isolate pure execution cost to 14.0–14.7 ns per insert (up to **71.4 Mops/s**). Subtracting the ~15 ns instrumentation bracket from individual p50 measurements (31–37 ns) yields approximately **16–22 ns of true per-operation latency** without pipeline overlap.

### 2. Head-to-Head Comparison: Reference `OrderBook` vs. `FlatBook`

| Operation | Baseline [`OrderBook`](crates/engine/src/book.rs) | Optimized [`FlatBook`](crates/engine/src/flat/mod.rs) | Improvement |
|---|---|---|---|
| **Limit Insert (Existing Level)** | 255 ns (batch mean) | **14.00 ns** batch mean (p50: 37 ns, p99: 78 ns) | **~18× faster** |
| **Limit Insert (New Level)** | 266 ns (batch mean) | **14.67 ns** batch mean (p50: 31 ns, p99: 44 ns) | **~18× faster** |
| **Cancel (Queue Front)** | 350 ns (queue len 4) | **22.33 ns** batch mean (p50: 33 ns, p99: 55 ns) | **~16× faster** |
| **Cancel (Queue Mid / Tail)** | 580 ns – 2.8 µs ($O(n)$ search) | **17.67–18.67 ns** batch mean (p50: 33 ns, p99: 55 ns) | **~30× to 150× faster** |
| **Unknown-ID Rejection** | ~200 ns | **8.40 ns** batch mean (p50: 25 ns, p99: 28 ns) | **~24× faster** |
| **Mixed Workload (60/30/10)** | 231–384 ns / op | **25.85–35.10 ns** batch mean (p50: 28–34 ns, p99: 59–63 ns) | **~9× to 11× faster** |
| **Hot Path Memory Allocation** | Heap (`VecDeque`, `BTreeMap`) | **0 bytes (zero heap allocations)** | **Deterministic memory footprint** |

### 3. Reference `OrderBook` In-Memory Micro-Benchmarks (Criterion)

Historical baseline single-threaded CPU execution time of library calls on [`OrderBook`](crates/engine/src/book.rs) (excluding network, serialization, and disk I/O):

| Operation | Depth 10 | Depth 1,000 | Depth 100,000 |
|---|---|---|---|
| **`submit_limit` (non-crossing batch)** | **255 ns** | **266 ns** | **542 ns** |
| **`match_sweep`** | **4.69 µs** | **382 µs** | **75.75 ms** |
| **`cancel` (short queue)** | **378 ns** | **857 ns** | **4.86 µs** |

* **Queue Length Cancellation Scaling**: In [`OrderBook`](crates/engine/src/book.rs), orders at a single price level are stored in a `VecDeque`, requiring an $O(n)$ search to cancel mid-queue (4 orders: ~350 ns; 50 orders: ~580 ns; 500 orders: ~2.8 µs). [`FlatBook`](crates/engine/src/flat/mod.rs) replaces this with intrusive doubly-linked list nodes, achieving constant-time $O(1)$ cancellation across all queue positions.

### 4. Server Throughput & Persistence Overhead

Matching throughput measured over 1,000,000 synthetic events using the reference [`OrderBook`](crates/engine/src/book.rs) engine (`cargo run --release -p server --bin persist_load`):

* **Engine standalone (Arm A - no writer)**: **2,133,230 events/second** (821,246 commands/s)
* **Engine with TimescaleDB writer attached (Arm B - unpaced burst)**: **2,139,451 events/second** ($\pm 0.3\%$ difference; 103,000 trades persisted, 878,011 backpressure drops due to bounded channel saturation)
* **Sustained Zero-Drop Persistence Run (`--rate 45000`)**: **116,911 events/second** (44,999 commands/s) with **1,000,082 trades persisted and 0 dropped trades**.

### 5. End-to-End WebSocket Delivery Latency (HDR Histogram)

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
# Verify against reference OrderBook (default)
cargo run --locked -p replay -- verify docs/recordings/basic.recording.jsonl

# Or verify against ultra-low latency FlatBook
cargo run --locked -p replay -- verify docs/recordings/basic.recording.jsonl --engine flat
```

The example covers FIFO partial fills, cancellation, unfilled market quantity,
and rejected commands. Verification compares every engine event (including
maker/taker IDs) and the complete final snapshot.

To create a recording from your own JSON Lines command file:

```bash
cargo run --locked -p replay -- record --commands docs/recordings/basic.commands.jsonl --output my-recording.jsonl
cargo run --locked -p replay -- verify my-recording.jsonl
# Cross-verify: recordings verify identically on both matching engines
cargo run --locked -p replay -- verify my-recording.jsonl --engine flat
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
# Run with reference OrderBook (default)
cargo run -p server --bin server -- --port 8080 --feed-rate 1000 --seed 42

# Or run with ultra-low latency FlatBook
cargo run -p server --bin server -- --port 8080 --engine flat --feed-rate 1000 --seed 42
```

#### PowerShell (Windows)
```powershell
# Run with reference OrderBook (default)
cargo run -p server --bin server -- --port 8080 --feed-rate 1000 --seed 42

# Or run with ultra-low latency FlatBook
cargo run -p server --bin server -- --port 8080 --engine flat --feed-rate 1000 --seed 42
```

Endpoints available:
* `GET  /health` — Liveness, active engine variant (`reference` or `flat`), live counters, sequence number, and channel depths.
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
