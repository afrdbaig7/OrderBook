#!/usr/bin/env bash
# scripts/smoke_test.sh — Automated container quickstart smoke test.
#
# Validates the full-stack container environment end-to-end using an ISOLATED
# test configuration to protect developer demo data and port allocations:
#
# Check                          What it establishes
# ------------------------------ ----------------------------------------------------
# HTML loads                     Static files are served through Nginx reverse proxy
# Rate control policy mutates    Protected/public API policy functions via reverse proxy
# Persistence growth             Trades land in TimescaleDB hypertable
# Candle aggregate validation    Continuous aggregate produces non-empty OHLCV bars
# WebSocket stream probe         Reverse proxy routes WS; snapshot + contiguous deltas
# Clean exit & isolated teardown Isolated containers & test volumes cleaned up safely

set -euo pipefail

# 1. Project & Port Isolation
PROJECT_NAME="ob-smoke-${RANDOM:-$$}"
SMOKE_PORT="${SMOKE_PORT:-15173}"
export SMOKE_PORT

# 2. Dependency validation
if ! command -v node >/dev/null 2>&1; then
    echo "Error: Node.js (v20+) is required to run container smoke tests" >&2
    exit 1
fi

echo "==> Smoke test initializing with isolated project '$PROJECT_NAME' on port $SMOKE_PORT..."

# 2. Pre-register cleanup before running any docker commands
cleanup() {
    local exit_code=$?
    echo "==> Tearing down isolated smoke test stack ($PROJECT_NAME)..."
    docker compose -p "$PROJECT_NAME" -f docker-compose.smoke.yml down -v --remove-orphans 2>/dev/null || true
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

# 3. Build and launch isolated stack
echo "==> Building and launching isolated Docker stack..."
docker compose -p "$PROJECT_NAME" -f docker-compose.smoke.yml up --build -d

BASE_URL="http://127.0.0.1:${SMOKE_PORT}"
WS_URL="ws://127.0.0.1:${SMOKE_PORT}/ws"

echo "==> Waiting for services to become healthy at $BASE_URL..."
for i in {1..60}; do
    if curl -s -f "$BASE_URL/health" 2>/dev/null | grep -q '"status":"ok"'; then
        echo "Server is healthy!"
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        echo "Error: Server did not become healthy within 60s"
        docker compose -p "$PROJECT_NAME" -f docker-compose.smoke.yml logs
        exit 1
    fi
done

echo "==> Check 1: Dashboard HTML delivery through Nginx (port $SMOKE_PORT)..."
HTML_BODY=$(curl -s -f "$BASE_URL/")
if ! echo "$HTML_BODY" | grep -q 'root' && ! echo "$HTML_BODY" | grep -q 'Low-Latency Order Book'; then
    echo "Error: Unexpected HTML content"
    exit 1
fi
echo "Static assets delivered successfully!"

echo "==> Check 2: Feed rate control mutation policy via reverse proxy..."
curl -s -f -X POST "$BASE_URL/feed-rate" \
    -H "Content-Type: application/json" \
    -d '{"orders_per_second": 3000}' | grep -q '3000'
curl -s -f "$BASE_URL/feed-rate" | grep -q '"orders_per_second":3000'
echo "Rate control policy verified!"

echo "==> Check 3: Persistence growth in TimescaleDB..."
INITIAL_ROWS=$(curl -s -f "$BASE_URL/health" | grep -o '"persisted_rows":[0-9]*' | cut -d: -f2)
sleep 2
FINAL_ROWS=$(curl -s -f "$BASE_URL/health" | grep -o '"persisted_rows":[0-9]*' | cut -d: -f2)
if [ "$FINAL_ROWS" -le "$INITIAL_ROWS" ]; then
    echo "Error: persisted_rows did not increase ($INITIAL_ROWS -> $FINAL_ROWS)"
    exit 1
fi
echo "Persistence verified: rows increased from $INITIAL_ROWS to $FINAL_ROWS!"

echo "==> Check 4: Continuous aggregate candle query with non-empty validation..."
# Allow TimescaleDB continuous aggregate policy to produce at least 1 bar
CANDLES_VERIFIED=false
for i in {1..20}; do
    NOW_MS=$(date +%s000)
    FROM_MS=$((NOW_MS - 60000))
    RESP=$(curl -s -f "$BASE_URL/candles?from=${FROM_MS}&to=${NOW_MS}&interval=1s" || true)
    if [ -n "$RESP" ]; then
        if node -e '
            const resp = JSON.parse(process.argv[1]);
            if (!Array.isArray(resp.candles) || resp.candles.length === 0) process.exit(1);
            const c = resp.candles[0];
            if (typeof c.ts !== "number" || typeof c.open !== "number" || typeof c.high !== "number" ||
                typeof c.low !== "number" || typeof c.close !== "number" || typeof c.volume !== "number" || c.volume <= 0) {
                process.exit(1);
            }
            console.log(`Verified valid OHLCV candle row: ts=${c.ts}, O=${c.open}, H=${c.high}, L=${c.low}, C=${c.close}, V=${c.volume}`);
            process.exit(0);
        ' "$RESP" 2>/dev/null; then
            CANDLES_VERIFIED=true
            break
        fi
    fi
    sleep 1
done

if [ "$CANDLES_VERIFIED" != "true" ]; then
    echo "Error: No populated OHLCV candle bars produced within 20s"
    exit 1
fi

echo "==> Check 5: WebSocket streaming through Nginx reverse proxy..."
node --experimental-websocket scripts/ws_probe.mjs "$WS_URL" 25

echo "==> All isolated container smoke test checks passed successfully!"
