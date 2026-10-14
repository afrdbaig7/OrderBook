# Order Book

A low-latency limit order book and matching engine with WebSocket market
data, TimescaleDB persistence and a live web dashboard. Built as a
phase-by-phase portfolio project: every layer is tested, benchmarked and
measured.

The matching core is a **pure library** — no locks, no async, no I/O on the
hot path. A single dedicated thread owns the book; everything else
(WebSockets, HTTP, the database, the synthetic feed) is Tokio and communicates
only through bounded channels, so a slow client exerts backpressure instead of
stalling matching.

```text
 feed task (tokio)              engine thread (std)          broadcast (tokio)
 ─────────────────              ───────────────────          ─────────────────
 SyntheticFeed.next_command ──► EngineCommand::Feed ──► book.submit_* ──► ServerEvent ──► /ws
                      │                        │                              │
                      │                  Snapshot request ────────────────► snapshot reply
                      │                        │
                      └── order commands ──► trades ──try_send──► db writer (tokio) ──► TimescaleDB
```

## Crates

| Crate | Responsibility |
|-------|----------------|
| `crates/engine` | The order book: integer prices, FIFO price levels, maker-price fills, sequencing, snapshots. Pure, `#![forbid(unsafe_code)]`, no runtime. |
| `crates/feed` | Wire protocol (`Message` enum, `Event` → JSON), deterministic synthetic order flow, and the reference client-side reconstruction (`ClientBook`). No sockets, no async. |
| `crates/bench` | Criterion micro-benchmarks and an HDR-histogram latency binary. |
| `crates/server` | Single-writer engine thread, WebSocket stream, HTTP API, batched persistence writer, metrics. |
| `web` | Vite + React + TypeScript dashboard: connection status, cumulative depth ladder, trade tape, live 1-second candlestick chart (`lightweight-charts`), spread/midpoint/last and msg/s readouts. |

## Design rules

* **Integer prices and quantities** everywhere — no floating point in the book.
* **Maker price wins** on every fill.
* **FIFO** within a price level.
* The engine assigns **every** sequence number; every event — including
  rejects and fills — consumes exactly one `seq`.
* One `Delta` per changed level per event; `qty == 0` deletes the level.
* Conservation invariant: `submitted == 2 × fills + cancelled + dropped + resting`.
* The engine never sees Tokio, SQL, HTTP or a clock; the server stamps trade
  timestamps.

## Quick start

> **Demo.** The dashboard is best seen live: run the three commands below, open
> <http://localhost:5173>, and drag the feed-rate up. A screen-recorded
> `docs/demo.gif` is referenced from this README; it is **not committed**
> because it must be captured locally (see [Demo](#demo)).

### 1. Server only (no database)

```bash
cargo run -p server --bin server -- --port 8080 --feed-rate 1000 --seed 1
```

* `GET  /health` — status, live counters, sequence, feed rate.
* `GET  /ws` — snapshot + live deltas/trades.
* `GET  /candles?from=<ms>&to=<ms>&interval=1s` — 1s OHLCV bars (`503` without a DB).
* `GET  /feed-rate` · `POST /feed-rate {"orders_per_second": N}` — live rate control.

### 2. With persistence (TimescaleDB)

```bash
docker compose up -d                     # applies db/schema.sql on first boot
export DATABASE_URL="postgres://orderbook:orderbook@localhost:5434/orderbook"
cargo run -p server --bin server -- --feed-rate 2000
```

> The compose file maps the database to host port **5434** (this machine
> already had Postgres on 5432). Adjust `docker-compose.yml` if that is free.

### 3. Web dashboard

```bash
cd web
npm install
npm run dev          # http://localhost:5173, proxies /ws + API to :8080
```

### Verification tools

```bash
# apply every message to the reference ClientBook; exits nonzero on any gap
cargo run --release -p server --bin ws_probe -- --url ws://127.0.0.1:8080/ws --seconds 30

# end-to-end trade latency (server ts → reader), p50/p99/p99.9
cargo run --release -p server --bin e2e_latency -- --url ws://127.0.0.1:8080/ws --seconds 15

# persistence load test: 1M+ trades + engine A/B with/without the DB writer (§38)
cargo run --release -p server --bin persist_load -- --target-trades 1000000 --seed 42 --no-persist
```

## WebSocket protocol

Internally tagged JSON, one message per engine event:

```json
{"type":"snapshot","seq":1042,"bids":[[10000,50],[9999,120]],"asks":[[10001,30],[10002,75]]}
{"type":"delta","seq":1043,"side":"bid","price":10000,"qty":35}
{"type":"trade","seq":1044,"price":10001,"qty":10,"taker_side":"bid","ts":1730000000000}
{"type":"ack","seq":1045,"id":9}
{"type":"cancel","seq":1046,"id":7}
{"type":"reject","seq":1047,"id":3,"reason":"duplicate_order_id"}
```

Client rules (implemented identically in Rust `feed::ClientBook` and the
TypeScript `PureOrderBook`):

1. Wait for a `snapshot`; it resets state and sets the expected sequence.
2. Apply messages in order; ignore `seq <= snapshot.seq`.
3. `seq == last + 1` → apply; `seq > last + 1` → **desynchronized** → reconnect.
4. On server-side lag the socket is closed so the client resyncs with a fresh
   snapshot.

## Benchmarks

Micro-benchmarks (Criterion) and an HDR latency run live in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md). Highlights on an i5-12500H /
Windows 11 / rustc 1.92, 1M ops, depth 1000:

| Operation | Median |
|-----------|--------|
| `submit_limit` (10 / 1k / 100k levels) | 255 ns / 266 ns / 542 ns |
| `match_sweep` | 4.69 µs / 382 µs / 75.75 ms |
| `cancel` | 377.9 ns / 857 ns / 4.86 µs |

End-to-end latency (release server, one WebSocket reader, so the client
JSON-parse cost is included) — **localhost loopback, not Internet/exchange**:

| Feed rate (orders/s) | p50 | p99 | p99.9 |
|---|---|---|---|
| 1,000 | 0 ms | 1 ms | 2 ms |
| 5,000 | 1 ms | 4 ms | 5 ms |
| 20,000 | 4 ms | 13 ms | 17 ms |
| 50,000 | 11 ms | 779 ms | 796 ms |

Stays single-digit-ms to 20,000 orders/s; a single JSON client saturates
(~10k trades/s) by 50,000. Reproduce commands and the full table:
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md#end-to-end-latency-sweep-spec-46).

Stream health over a 60-second run: **298,185 messages**, 84,413 trades,
98,570 deltas, **0 sequence gaps**, `synced = true` (~4,972 msg/s).

Persistence load test (§38): **1,000,082 trades** generated and **1,000,082
persisted, 0 drops** into TimescaleDB. With the writer attached vs detached,
engine throughput was **2,133,230 vs 2,139,451 events/s (±0.3 %)** — the DB
never slows matching (it drops + counts into a bounded queue instead). Full
command and numbers: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

> Pipeline ordering is deterministic: `cargo fmt --check`,
> `cargo clippy --workspace --all-targets -- -D warnings`,
> `cargo test --workspace`.

## Persistence

`trades` is a TimescaleDB hypertable (`ts`, `seq`, `price`, `qty`,
`taker_side`). A continuous aggregate `candles_1s` produces 1-second OHLCV
bars. The writer batches up to 1,000 rows (or 100 ms) into a single
`INSERT ... SELECT FROM unnest(...)`, and drops + counts rows when the bounded
channel is full — matching never waits on the database. See
[`db/schema.sql`](db/schema.sql).

## Testing

```bash
cargo test --workspace                 # engine + feed + server units & e2e
docker compose up -d
TEST_DATABASE_URL=postgres://orderbook:orderbook@localhost:5434/orderbook \
  cargo test -p server --test persistence
cd web && npm test && npm run build
```

* `crates/engine/tests` — 43 tests incl. a `proptest` conservation property.
* `crates/feed/tests` — wire round-trips and snapshot+delta reconstruction.
* `crates/server/tests/e2e.rs` — spawns the real binary, asserts snapshot +
  contiguous sequencing + reconnect + rate control over real sockets.
* `crates/server/tests/persistence.rs` — Phase E gate against live TimescaleDB.
* `web/src/**/*.test.ts` — 20 Vitest tests: the wire-protocol `PureOrderBook`
  reducer plus the `MarketStore` (snapshot + buffered-delta reconstruction,
  cumulative ladder ordering, 50-trade tape cap, gap detection, trade pub/sub).

## Project phases

| Phase | Scope | Status |
|-------|-------|--------|
| A | Engine crate, types, price levels | ✅ |
| B | Order book, validation, snapshots, properties | ✅ |
| C | Benchmarks + latency harness + documented baselines | ✅ |
| D | Feed protocol + server (engine thread, WS, HTTP, metrics) | ✅ |
| E | TimescaleDB persistence + candle aggregate | ✅ |
| F | Web dashboard (ladder, tape, candles) | ✅ |
| G | End-to-end latency, README, CI | ✅ |

## Limitations

* The synthetic feed is generated in-process (deterministic by seed); there
  is no ITCH/OUCH or FIX market-data feed.
* Order ids are not persisted — `trades` stores market data, not a ledger.
* A duplicate `OrderId` is only rejected while it rests (documented engine limitation).
* The wire protocol is JSON text frames, not a binary protocol (fine at these
  rates, not exchange-grade).
* The `candles_1s` `close` value can differ by a few ticks from the true
  `(ts, seq)`-ordered close because `last(price, ts)` orders by `ts` only and
  many fills can share a millisecond; `open`/`high`/`low`/`volume` reconcile
  exactly. A `seq`-aware candle aggregate would make `close` exact.
* Cancellation is **O(n)** in the level (`VecDeque` scan) by design; see
  [Parking Lot](#parking-lot) and the §48 analysis in `docs/BENCHMARKS.md`.
* End-to-end latency is **localhost** (same machine, loopback); it is not
  Internet or exchange-grade latency.
* The dashboard runs well past 5,000 msg/s by batching renders per animation
  frame, but this has only been exercised headlessly (no browser automation in
  this environment).

## Demo

The README references `docs/demo.gif` (a short screen capture: ladder + tape +
candles updating as the feed rate is raised). It is intentionally **not
committed** — it must be recorded on a machine with a display/browser:

```bash
docker compose up -d
DATABASE_URL=postgres://orderbook:orderbook@localhost:5434/orderbook \
  cargo run --release -p server --bin server -- --feed-rate 5000
cd web && npm run dev            # open http://localhost:5173
# record ~10 s, save as docs/demo.gif
```

## Parking Lot

Deliberately **not** implemented (spec §3, §58 "Final Parking Lot"); listed so
nothing is implied to already exist:

* **Multi-symbol** engine and per-symbol sequencing.
* **Binary market-data protocol** (integer-packed frames instead of JSON).
* **O(1) cancellation** — slab / intrusive index, *only if* a Linux flamegraph
  shows cancel dominating (see §48).
* **IOC / FOK / stop** order types.
* **Sequencer + replication** and disk snapshots for HA.
* **Event-sourced replay** for deterministic recovery.
* **Live exchange feed** adapters (ITCH/OUCH, FIX).

## Definition of Done

| Criterion | Status |
|---|---|
| `cargo fmt` passes | ✅ |
| `cargo clippy -D warnings` passes | ✅ |
| `cargo test --workspace` passes | ✅ |
| Frontend build passes | ✅ |
| Frontend tests pass | ✅ |
| Book never crosses (property test) | ✅ |
| Quantity conservation (property test) | ✅ |
| Index consistency (property test) | ✅ |
| Deterministic replay | ✅ |
| Snapshot + delta reconstruction test | ✅ |
| Benchmark methodology documented | ✅ |
| Real benchmark numbers recorded | ✅ |
| Benchmark reproduce commands documented | ✅ |
| WebSocket reconnect/resync works | ✅ |
| 1M+ trades persisted | ✅ (1,000,082) |
| Persistence does not block matching | ✅ (A/B ±0.3 %) |
| Candle data verified | ✅ (high/low/volume exact; `close` tie-break noted) |
| Frontend usable under high message rates | ✅ (rAF batching; headless) |
| README contains p50/p99/p99.9 | ✅ |
| End-to-end latency at multiple feed rates | ✅ (1k–50k sweep) |
| Committed to version control (baseline) | ✅ |
| Demo GIF exists | ⚠️ not committed — requires local screen capture (see [Demo](#demo)) |
| Repository runs from a clean clone | ✅ |
| Limitations honestly documented | ✅ |
