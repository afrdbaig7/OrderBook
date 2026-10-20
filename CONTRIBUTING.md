# Contributing to Order Book

Thank you for your interest in contributing to Order Book.

This project is licensed under either the [MIT License](LICENSE-MIT) or the [Apache License (Version 2.0)](LICENSE-APACHE) at your option. All contributions will be licensed under this dual-license model.

---

## Architecture Boundaries

The workspace is organized into focused crates with strict architectural boundaries:

* **`crates/engine`**: Synchronous limit order book matching core.
  - `#![forbid(unsafe_code)]`.
  - No Tokio, no async, no I/O, no system clocks, no database calls.
  - Changes must preserve core invariants: conservation of quantity, FIFO within price levels, maker-price execution, and book-never-crossed properties.
  - Any logic change must update and pass the property tests in `crates/engine/tests/properties.rs`.
* **`crates/feed`**: Wire protocol definitions, synthetic feed generator, and reference `ClientBook` reconstruction.
  - `#![forbid(unsafe_code)]`.
  - Changes to message structure must ensure backward compatibility and roundtrip JSON deserialization in `crates/feed/tests/wire.rs`.
* **`crates/server`**: Tokio runtime, single-writer thread coordination, bounded channels, WebSocket/HTTP server, and TimescaleDB batched persistence.
* **`crates/bench`**: Criterion micro-benchmarks and HDR latency harness.
* **`web`**: React, TypeScript, and Vite frontend dashboard.

---

## Development Workflows

### 1. Rust-Only Workflow (No Node or Docker required)

You do **not** need Node or Docker to work on the matching core, feed protocol, or server endpoints.

**Prerequisites:** Rust stable toolchain (managed via `rustup`).

#### POSIX (Linux / macOS)
```bash
# Check formatting
cargo fmt --all --check

# Run linter
cargo clippy --workspace --all-targets -- -D warnings

# Run all workspace tests (engine, feed, server e2e)
cargo test --workspace

# Run Criterion benchmarks
cargo bench -p bench --bench book
```

#### PowerShell (Windows)
```powershell
# Check formatting
cargo fmt --all --check

# Run linter
cargo clippy --workspace --all-targets -- -D warnings

# Run all workspace tests
cargo test --workspace

# Run Criterion benchmarks
cargo bench -p bench --bench book
```

---

### 2. Database Integration Workflow (When TimescaleDB is needed)

TimescaleDB is **only** required when modifying `crates/server/src/db.rs`, candle aggregation, or running `crates/server/tests/persistence.rs`.

#### POSIX (Linux / macOS)
```bash
# Start TimescaleDB in background with host port 5434 published
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d timescaledb

# Run the persistence integration tests
TEST_DATABASE_URL="postgres://orderbook:orderbook@localhost:5434/orderbook" \
  cargo test -p server --test persistence -- --nocapture
```

#### PowerShell (Windows)
```powershell
# Start TimescaleDB in background with host port 5434 published
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d timescaledb

# Run the persistence integration tests
$env:TEST_DATABASE_URL = "postgres://orderbook:orderbook@localhost:5434/orderbook"
cargo test -p server --test persistence -- --nocapture
```

---

### 3. Frontend Workflow (When Node is needed)

Node.js (v20+) is **only** required when working on the web dashboard in `web/`.

```bash
cd web

# Install dependencies strictly from lockfile
npm ci

# Run Vitest tests
npm test

# Check TypeScript types and build production bundle
npm run build
```

---

## Pull Request Guidelines

1. **Keep PRs focused**: Address one issue or feature per pull request.
2. **Preserve invariants**: Do not alter matching semantics without corresponding updates to property tests in `crates/engine/tests`.
3. **Verify locally before submitting**:
   - `cargo fmt --all --check`
   - `cargo clippy --workspace --all-targets -- -D warnings`
   - `cargo test --workspace`
   - `npm test` & `npm run build` (if frontend files were modified)
4. **Honest benchmarking**: If submitting a performance change, run benchmarks before and after under identical conditions and paste the Criterion output in the pull request description.
