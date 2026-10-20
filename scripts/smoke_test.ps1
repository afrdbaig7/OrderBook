# scripts/smoke_test.ps1 — Automated container quickstart smoke test for PowerShell.
#
# Validates the full-stack container environment end-to-end:
# 1. Builds and launches TimescaleDB, backend server, and Nginx web proxy.
# 2. Asserts healthcheck and reverse proxy readiness.
# 3. Validates dashboard static delivery through Nginx.
# 4. Validates rate control mutation policy.
# 5. Validates active trade persistence growth.
# 6. Validates continuous aggregate candle queries.
# 7. Cleans up containers and volumes.

$ErrorActionPreference = "Stop"

Write-Host "==> Building and launching Docker stack..."
docker compose up --build -d

try {
    Write-Host "==> Waiting for services to become healthy..."
    $healthy = $false
    for ($i = 1; $i -le 60; $i++) {
        try {
            $resp = Invoke-RestMethod -Uri "http://localhost:5173/health" -Method Get -TimeoutSec 2
            if ($resp.status -eq "ok") {
                $healthy = $true
                Write-Host "Server is healthy!"
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    if (-not $healthy) {
        Write-Error "Server did not become healthy within 60s"
        docker compose logs
        exit 1
    }

    Write-Host "==> Testing 1: Dashboard HTML delivery through Nginx (:5173)..."
    $html = Invoke-WebRequest -Uri "http://localhost:5173/" -Method Get
    if ($html.Content -notmatch "root" -and $html.Content -notmatch "Low-Latency Order Book") {
        Write-Error "Dashboard HTML failed delivery"
    }
    Write-Host "Dashboard delivered successfully!"

    Write-Host "==> Testing 2: Feed rate control policy via reverse proxy..."
    $rateBody = '{"orders_per_second": 3000}'
    $postRate = Invoke-RestMethod -Uri "http://localhost:5173/feed-rate" -Method Post -Body $rateBody -ContentType "application/json"
    if ($postRate.orders_per_second -ne 3000) {
        Write-Error "Failed to update feed rate to 3000"
    }
    $getRate = Invoke-RestMethod -Uri "http://localhost:5173/feed-rate" -Method Get
    if ($getRate.orders_per_second -ne 3000) {
        Write-Error "Failed to verify feed rate 3000"
    }
    Write-Host "Rate control policy verified!"

    Write-Host "==> Testing 3: Persistence growth in TimescaleDB..."
    $initialHealth = Invoke-RestMethod -Uri "http://localhost:5173/health" -Method Get
    $initialRows = $initialHealth.persisted_rows
    Start-Sleep -Seconds 2
    $finalHealth = Invoke-RestMethod -Uri "http://localhost:5173/health" -Method Get
    $finalRows = $finalHealth.persisted_rows

    if ($finalRows -le $initialRows) {
        Write-Error "Persisted rows did not increase ($initialRows -> $finalRows)"
    }
    Write-Host "Persistence verified: rows increased from $initialRows to $finalRows!"

    Write-Host "==> Testing 4: Continuous aggregate candle query..."
    $nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $fromMs = $nowMs - 60000
    $candles = Invoke-RestMethod -Uri "http://localhost:5173/candles?from=$fromMs&to=$nowMs&interval=1s" -Method Get
    if ($null -eq $candles.candles) {
        Write-Error "Failed to retrieve candles array"
    }
    Write-Host "Continuous aggregate candle retrieval verified!"

    Write-Host "==> All full-stack container smoke tests passed successfully!"
} finally {
    Write-Host "==> Tearing down Docker stack..."
    docker compose down -v
}
