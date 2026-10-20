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

## 4. Visual Dashboard Verification

The dashboard was loaded and rendered in headless Google Chrome at `1400x900` resolution against the live container stack:

![Live Dashboard](dashboard.png)

* **Live Status Pill:** Green active connection indicator.
* **Header Statistics:** Live calculation of Mid ($100,156.5$), Spread ($17$), Last Trade ($100,148$), and contiguous Sequence numbers ($3,382,703$).
* **Order Book Ladder:** Visual depth bars with cumulative sizing, separating bids (green) and asks (red) around the spread.
* **Trade Tape:** Real-time trade stream displaying price, size, taker side, and execution timestamp.
* **Candle Chart:** Interactive TradingView Lightweight Charts rendering historical and live 1-second candlestick bars.

---

## 5. Automated Test Suite Execution

### 5.1 Rust Workspace Tests
```bash
cargo test --workspace --locked
```
Across all workspace crates, the test suite executes **56 passing tests** (0 failed):
* **`engine::order_book`:** 25 passed; 0 failed (FIFO execution, price-time priority, cancel handling, duplicate order ID rejection).
* **`engine::price_level`:** 13 passed; 0 failed (linked-list operations, partial fills, order removal).
* **`engine::properties` (`proptest`):** 1 passed (100 randomized multi-operation sequences checking book invariants).
* **`feed::protocol`:** 4 passed; 0 failed (gap detection, snapshot resync, client state recovery).
* **`feed::reconstruct`:** 4 passed; 0 failed (JSON wire pipeline reconstruction).
* **`feed::wire`:** 7 passed; 0 failed (schema validation, roundtrip serialization).
* **`server::e2e`:** 1 passed; 0 failed (live server streaming, rate limiting bounds, 24h candle limits).
* **`server::persistence`:** 1 passed; 0 failed. When run without `TEST_DATABASE_URL` (e.g. in CI without Docker), this test returns early and skips database queries, allowing offline workspace tests to stay green. Active database assertions are verified via dedicated live execution (see §5.2).

### 5.2 Database-Backed Candle Boundary & Persistence Flush Tests
Executed with `TEST_DATABASE_URL=postgres://orderbook:orderbook@localhost:5434/orderbook` against an active TimescaleDB container instance:
* **Half-Open Inclusion/Exclusion:** Asserts `[t0, t1)` includes `t0` and strictly excludes `t1`.
* **Subsequent Interval:** Asserts `[t1, t1 + 1000)` excludes prior bucket `t0` and includes `t1`.
* **Empty Window:** Asserts `[t0, t0)` returns HTTP 200 with an empty JSON array `[]`.
* **Maximum 24h Window:** Asserts interval of exactly 86,400,000 ms returns HTTP 200.
* **Exceeding 24h Window:** Asserts interval of 86,400,001 ms returns HTTP 400 Bad Request.
* **Partial-Batch Flush Contract:** Halts feed, injects a partial burst of trades ($< 1,000$ rows), waits for the elapsed-time flush window, and asserts `persisted_rows` increments by the exact count without waiting for a full 1,000-row batch. (Note: this integration test demonstrates that sub-1,000-row batches flush automatically via the elapsed timer rather than waiting indefinitely for `BATCH_ROWS`; due to test sleep margins, it demonstrates eventual elapsed flushing rather than an exact hard real-time sub-millisecond 100 ms precision guarantee).

### 5.3 Code Quality & Linters
* **Formatting:** `cargo fmt --all --check` returned exit code 0.
* **Clippy:** `cargo clippy --workspace --all-targets --locked -- -D warnings` returned exit code 0.

### 5.4 Frontend Test Suite & Production Bundle
```bash
cd web
npm test
npm run build
```
* **Vitest:** 2 test suites passed, 20 of 20 protocol and store unit tests passed (`protocol.test.ts`, `store.test.ts`). These verify the wire protocol sequence reconstruction and reactive store state management independently of browser DOM rendering.
* **Vite Build:** Produced optimized production bundle (`dist/index.html` 2.83 kB, `dist/assets/index-*.js` 314.58 kB gzip: 100.68 kB) in 834 ms.

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
