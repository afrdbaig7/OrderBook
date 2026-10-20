#!/usr/bin/env bash
# scripts/smoke_test.sh — Automated container quickstart smoke test.
#
# Validates the full-stack container environment end-to-end:
# 1. Builds and launches TimescaleDB, backend server, and Nginx web proxy.
# 2. Asserts healthcheck and reverse proxy readiness.
# 3. Validates dashboard static delivery through Nginx.
# 4. Validates rate control mutation policy.
# 5. Validates active trade persistence growth.
# 6. Validates continuous aggregate candle queries.
# 7. Cleans up containers and volumes.

set -euo pipefail

echo "==> Building and launching Docker stack..."
docker compose up --build -d

cleanup() {
    echo "==> Tearing down Docker stack..."
    docker compose down -v
}
trap cleanup EXIT

echo "==> Waiting for services to become healthy..."
for i in {1..60}; do
    if curl -s -f http://localhost:5173/health | grep -q '"status":"ok"'; then
        echo "Server is healthy!"
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        echo "Error: Server did not become healthy within 60s"
        docker compose logs
        exit 1
    fi
done

echo "==> Testing 1: Dashboard HTML delivery through Nginx (:5173)..."
curl -s -f http://localhost:5173/ | grep -q 'Low-Latency Order Book' || curl -s -f http://localhost:5173/ | grep -q 'root'
echo "Dashboard delivered successfully!"

echo "==> Testing 2: Feed rate control policy via reverse proxy..."
curl -s -f -X POST http://localhost:5173/feed-rate \
    -H "Content-Type: application/json" \
    -d '{"orders_per_second": 3000}' | grep -q '3000'
curl -s -f http://localhost:5173/feed-rate | grep -q '"orders_per_second":3000'
echo "Rate control policy verified!"

echo "==> Testing 3: Persistence growth in TimescaleDB..."
INITIAL_ROWS=$(curl -s -f http://localhost:5173/health | grep -o '"persisted_rows":[0-9]*' | cut -d: -f2)
sleep 2
FINAL_ROWS=$(curl -s -f http://localhost:5173/health | grep -o '"persisted_rows":[0-9]*' | cut -d: -f2)
if [ "$FINAL_ROWS" -le "$INITIAL_ROWS" ]; then
    echo "Error: persisted_rows did not increase ($INITIAL_ROWS -> $FINAL_ROWS)"
    exit 1
fi
echo "Persistence verified: rows increased from $INITIAL_ROWS to $FINAL_ROWS!"

echo "==> Testing 4: Continuous aggregate candle query..."
NOW_MS=$(date +%s000)
FROM_MS=$((NOW_MS - 60000))
curl -s -f "http://localhost:5173/candles?from=${FROM_MS}&to=${NOW_MS}&interval=1s" | grep -q '"candles":'
echo "Continuous aggregate candle retrieval verified!"

echo "==> All full-stack container smoke tests passed successfully!"
