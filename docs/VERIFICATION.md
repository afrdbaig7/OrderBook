# Release Verification Report

**Date:** October 20, 2026  
**Version:** v0.1.0  
**Repository:** `afrdbaig7/OrderBook`  
**License:** MIT OR Apache-2.0  
**Contact:** `mohammedafreedbaig47@gmail.com`

---

## 1. Executive Summary

This document records the verification methodology, environment details, test execution results, container orchestration, and artifact provenance for the public release of the Order Book and matching engine project.

All checks distinguish **locally executed and verified tests** from **CI-configured workflows**. No results or benchmarks have been fabricated.

> **Verification Scope & Limitations:** Extensive end-to-end container builds, full-stack live streaming, rate controls, and browser visual checks were performed on this workstation and captured in this report (including `docs/dashboard.png`). While release binaries and the database-backed unit/integration tests were rebuilt and re-verified against final source, live container/browser checks were not independently repeated in the final packaging pass.

---

## 2. Environment Matrix

| Component | Specification |
|---|---|
| **Host Operating System** | Windows 11 Pro (x86_64) |
| **Container Subsystem** | WSL2 `Ubuntu-22.04` running Docker Engine 29.3.0, Compose v5.1.0 |
| **Rust Toolchain** | `rustc 1.90+` / `stable-x86_64-pc-windows-msvc` and `rust:1-slim-bookworm` |
| **Node.js & Frontend** | Node.js v24.18.0, Vitest 2.1.9, Vite 5.4.21, TypeScript 5.6.2 |
| **Database Engine** | TimescaleDB 2.16+ on PostgreSQL 16 (`timescale/timescaledb:latest-pg16`) |

---

## 3. Full Container Stack Verification

The full three-tier application stack was containerized, built, and launched with Docker Compose:

```bash
docker compose up -d
```

### 3.1 Container Health & Status
* **`orderbook-db` (`timescale/timescaledb:latest-pg16`):** Status `healthy` (listening on internal port 5432, exposed on host port 5434 in dev mode).
* **`orderbook-server` (`orderbook-server:latest`):** Status `healthy` (healthcheck: `curl -f http://localhost:8080/health`).
* **`orderbook-web` (`orderbook-web:latest`):** Status `Up` (Nginx reverse proxy and static asset server on `0.0.0.0:5173->80`).

### 3.2 Thread Starvation Defect & Fix
During container verification, the backend server initially failed its healthcheck under load:
* **Diagnosis:** `crates/server/src/db.rs` was invoking synchronous `crossbeam_channel::Receiver::recv_timeout(100ms)` within a `tokio::spawn` task. On multi-core development hosts with surplus worker threads this remained masked, but inside container environments with limited CPU quotas, the blocking call starved Tokio's worker threads and prevented Axum from servicing `/health` requests.
* **Resolution:** Replaced the blocking call with non-blocking `rx.try_recv()`, async `tokio::time::sleep()`, and periodic `tokio::task::yield_now().await` cooperatives. Container health checks immediately transitioned to healthy.

### 3.3 Endpoint Smoke Tests Through Nginx Proxy (`:5173`)

#### Health & Point-in-Time Observability
```bash
curl http://localhost:5173/health
```
```json
{
  "status": "ok",
  "command_channel_depth": 0,
  "feed_rate": 5000,
  "persistence_enabled": true,
  "orders_in": 189300,
  "events_out": 489101,
  "trades": 138674,
  "feed_drops": 0,
  "persistence_drops": 0,
  "persisted_rows": 138554,
  "clients": 0,
  "last_seq": 489101
}
```

#### Feed Rate Control (Tested up to 5,000 orders/s)
* `GET http://localhost:5173/feed-rate` returned current target rate.
* `POST http://localhost:5173/feed-rate` with `{"orders_per_second": 5000}` updated the live atomic pacer.
* Confirmed: Channel depth remained 0, with zero order drops and zero persistence drops at sustained 5,000 orders/sec.

#### Continuous Aggregate Candle Query
```bash
curl "http://localhost:5173/candles?from=1792438689000&to=1792438749000&interval=1s"
```
* Returned contiguous 1-second OHLCV bars calculated in real time by TimescaleDB's `candles_1s` continuous aggregate, correctly scaling volume as feed rate increased.

#### WebSocket Streaming & Verification Probe (`ws_probe`)
The standalone verification probe was executed directly against the Nginx reverse proxy endpoint:
```bash
cargo run --locked -p server --bin ws_probe -- --url ws://127.0.0.1:5173/ws --seconds 5
```
* **Messages Received:** 65,648 messages (20,182 trades, 20,587 deltas).
* **Throughput:** 13,146 messages/second over reverse-proxied WebSocket.
* **Sequence Integrity:** `errors=0`, `synced=true`, strict sequence contiguity verified against reference `ClientBook`.
* **Reconnect Test:** Successive client disconnects and reconnects re-bootstrapped from snapshot with zero sequence gaps.

---

## 4. Web Dashboard Visual Verification

Visual and interactive validation was conducted in headless Google Chrome at `1400x900` resolution against the live container stack (see screenshot referenced in [README.md](../README.md)):

* **Live Status Pill:** Green active connection indicator.
* **Header Statistics:** Live calculation of Mid ($100,156.5$), Spread ($17$), Last Trade ($100,148$), and contiguous Sequence numbers ($3,382,703$).
* **Order Book Ladder:** Visual depth bars with cumulative sizing, separating bids (green) and asks (red) around the spread. Display bounded to top 20 levels while maintaining deep L2 book state.
* **Trade Tape:** Real-time trade stream displaying price, size, taker side, and execution timestamp, filtered to strictly accepted trades.
* **Candle Chart:** Interactive TradingView Lightweight Charts rendering historical and live 1-second candlestick bars, bounded to a rolling 120s window.

---

## 5. Automated Test Suite Execution

### 5.1 Rust Workspace Tests
```bash
cargo test --workspace --locked
```
Across all workspace crates, the test suite executes **71 passing tests** (0 failed):
* **`engine::order_book`:** 25 passed; 0 failed (FIFO execution, price-time priority, cancel handling, duplicate order ID rejection; see [`crates/engine/src/book.rs`](../crates/engine/src/book.rs)).
* **`engine::price_level`:** 13 passed; 0 failed (`VecDeque` FIFO queue operations, partial fills, order removal; see [`crates/engine/src/price_level.rs`](../crates/engine/src/price_level.rs)).
* **`engine::properties` (`proptest`):** 1 passed (10,000 randomized multi-operation sequences checking book invariants against `proptest_config(cases: 10_000)`; see [`crates/engine/tests/properties.rs`](../crates/engine/tests/properties.rs)).
* **`engine::bounds`:** 8 passed; 0 failed (numeric overflow prevention, `MAX_SAFE_INTEGER` bounds, `MAX_ORDER_ID` rejection, and `MAX_BOOK_LEVELS_PER_SIDE` depth limit; see [`crates/engine/tests/bounds.rs`](../crates/engine/tests/bounds.rs)).
* **`engine::reference` (`proptest`):** 1 passed (1,000 cases comparing randomized sequences against an independent naive reference matcher; see [`crates/engine/tests/reference.rs`](../crates/engine/tests/reference.rs)).
* **`feed::protocol`:** 4 passed; 0 failed (gap detection, snapshot resync, client state recovery; see [`crates/feed/src/protocol.rs`](../crates/feed/src/protocol.rs)).
* **`feed::reconstruct`:** 7 passed; 0 failed (JSON wire pipeline reconstruction, deep book snapshot reconstruction regression test, client-depth-equals-engine-depth boundary test, and candidate pool memory soak test; see [`crates/feed/tests/reconstruct.rs`](../crates/feed/tests/reconstruct.rs)).
* **`feed::wire`:** 7 passed; 0 failed (schema validation, roundtrip serialization; see [`crates/feed/src/wire.rs`](../crates/feed/src/wire.rs)).
* **`server::e2e`:** 5 passed on Windows / 6 on Unix (live server stream, CLI startup validation including snapshot-depth rejection, forced process kill, slow client eviction/recovery, bootstrap snapshot depth exceeding 20 levels, and Unix graceful SIGTERM process exit with WebSocket Close frames; see [`crates/server/tests/e2e.rs`](../crates/server/tests/e2e.rs)).
* **`server::shutdown`:** 1 passed; 0 failed (strictly ordered shutdown with slow writer and queued engine commands, verifying queue drain to DB channel and zero trade loss; see [`crates/server/tests/shutdown.rs`](../crates/server/tests/shutdown.rs)).
* **`server::persistence`:** 3 passed; 0 failed (deterministic candle sequence tie-break, partial-batch flush timer, and legacy continuous aggregate rebuild migration; see [`crates/server/tests/persistence.rs`](../crates/server/tests/persistence.rs)). When run without `TEST_DATABASE_URL` (e.g. in CI without Docker), this test suite skips database queries, allowing offline workspace tests to stay green. Active database assertions are verified via dedicated live execution (see §5.2).

### 5.2 Database-Backed Candle Boundary & Persistence Flush Tests
Executed with `TEST_DATABASE_URL=postgres://orderbook:orderbook@localhost:5434/orderbook` against an active TimescaleDB container instance:
* **Deterministic Sequence Tie-Break:** Asserts identical-timestamp differently priced trades produce deterministic `open` and `close` ordered by `seq` rather than timestamp alone (see [`crates/server/tests/persistence.rs`](../crates/server/tests/persistence.rs)).
* **Continuous Aggregate Migration & Historical Backfill:** Tears down view to simulate legacy `first(price, ts)` state, inserts historical trades (3 hours old) outside the 1-hour policy window, runs `db/migrations/002_rebuild_candles_with_sequence.sql`, and asserts historical bars are queryable and sequence-ordered via full backfill (`CALL refresh_continuous_aggregate('candles_1s', NULL, NULL)`).
* **Half-Open Inclusion/Exclusion:** Asserts `[t0, t1)` includes `t0` and strictly excludes `t1`.
* **Subsequent Interval:** Asserts `[t1, t1 + 1000)` excludes prior bucket `t0` and includes `t1`.
* **Empty Window:** Asserts `[t0, t0)` returns HTTP 200 with an empty JSON array `[]`.
* **Maximum 24h Window:** Asserts interval of exactly 86,400,000 ms returns HTTP 200.
* **Exceeding 24h Window:** Asserts interval of 86,400,001 ms returns HTTP 400 Bad Request.
* **Partial-Batch Flush Contract:** Halts feed, injects a partial burst of trades ($< 1,000$ rows), waits for the elapsed-time flush window, and asserts `persisted_rows` increments by the exact count without waiting for a full 1,000-row batch.

### 5.3 Automated Container Quickstart Smoke Test
Automated end-to-end container testing is provided via executable test scripts:
* **POSIX:** [`scripts/smoke_test.sh`](../scripts/smoke_test.sh)
* **PowerShell:** [`scripts/smoke_test.ps1`](../scripts/smoke_test.ps1)

The smoke test runs in an **isolated test project** (`-p ob-smoke-*`) using a dedicated test compose configuration ([`docker-compose.smoke.yml`](../docker-compose.smoke.yml)) to guarantee developer demo data is never deleted. It binds to port 15173, registers cleanup traps *before* stack launch, and verifies:
1. Static asset delivery through Nginx reverse proxy.
2. Feed rate control mutation and validation via reverse-proxied `POST /feed-rate`.
3. Active trade persistence row growth in TimescaleDB over a measured interval.
4. Continuous aggregate 1-second candle queries returning populated, non-empty OHLCV rows with valid prices and volumes.
5. WebSocket reverse proxy delivery and stream sequence contiguity via [`scripts/ws_probe.mjs`](../scripts/ws_probe.mjs).
6. Clean isolated teardown of only smoke test containers and volumes.

### 5.4 Code Quality & Linters
* **Formatting:** `cargo fmt --all --check` returned exit code 0.
* **Clippy:** `cargo clippy --workspace --all-targets --locked -- -D warnings` returned exit code 0.

### 5.5 Frontend Test Suite & Production Bundle
```bash
cd web
npm test
npm run build
```
* **Vitest:** 3 test suites passed, 31 of 31 unit tests passed ([`web/src/components/candleReconcile.test.ts`](../web/src/components/candleReconcile.test.ts), [`web/src/lib/protocol.test.ts`](../web/src/lib/protocol.test.ts), [`web/src/lib/store.test.ts`](../web/src/lib/store.test.ts)). These test authoritative candle history reconciliation across network disconnects, runtime wire schema validation, integer bounds, duplicate/stale trade side effect suppression, and book gap handling.
* **Vite Build:** Produced optimized production bundle (`dist/index.html` 2.83 kB, `dist/assets/index-*.js` 316.10 kB gzip: 101.19 kB) in 2.51s.

---

## 6. Release Packaging & Checksums

Release packaging was executed using reproducible, locked cargo builds.

| Target Platform | Package Format | File Path | SHA-256 Checksum | Local Execution Status |
|---|---|---|---|---|
| **Windows (`x86_64-pc-windows-msvc`)** | `.zip` | `dist/orderbook-v0.1.0-x86_64-pc-windows-msvc.zip` | `d0fafb5a7f7f0aa029fb1c40b6f0002ed45a03afecff49628b9d29c79d26cc05` | Verified (`server.exe --help` exit code 0) |
| **Linux (`x86_64-unknown-linux-gnu`)** | `.tar.gz` | `dist/orderbook-v0.1.0-x86_64-unknown-linux-gnu.tar.gz` | `1470629e5b0de3b856d704e3dc46646263fd69c3878f819cd2ca0fd9bf59f64f` | Verified (`./server --help` exit code 0 in WSL2) |
| **macOS (`aarch64-apple-darwin`)** | `.tar.gz` | *Omitted from release* | *N/A (unverified)* | Omitted from release workflow matrix; only verified targets are built and published. |

*Note on macOS:* macOS is unverified on this host environment and has been omitted from `.github/workflows/release.yml`. Published release assets and checksums apply strictly to verified targets (`x86_64-pc-windows-msvc` and `x86_64-unknown-linux-gnu`).

---

## 7. Open Source & Community Governance

The following compliance and governance assets are established:
* **Dual Licensing:** [LICENSE-MIT](../LICENSE-MIT) and [LICENSE-APACHE](../LICENSE-APACHE).
* **Security Policy:** [SECURITY.md](../SECURITY.md) configured with reporting contact `mohammedafreedbaig47@gmail.com`.
* **Code of Conduct:** [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md) configured with enforcement contact `mohammedafreedbaig47@gmail.com`.
* **Issue & PR Templates:** `.github/ISSUE_TEMPLATE/bug_report.md`, `.github/ISSUE_TEMPLATE/feature_request.md`, and `.github/PULL_REQUEST_TEMPLATE.md`.
* **Contribution Guide:** [CONTRIBUTING.md](../CONTRIBUTING.md) detailing dual-shell commands (POSIX and PowerShell) and zero-Docker native workflows.
