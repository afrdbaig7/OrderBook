# scripts/smoke_test.ps1 — Automated container quickstart smoke test for PowerShell.
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

$ErrorActionPreference = "Stop"

$smokePort = if ($env:SMOKE_PORT) { $env:SMOKE_PORT } else { "15173" }
$env:SMOKE_PORT = $smokePort
$randomId = Get-Random -Minimum 1000 -Maximum 9999
$projectName = "ob-smoke-$randomId"

# Dependency validation
if (-not (Get-Command node -ErrorAction SilentlyContinue) -and -not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Error "Required WebSocket probe dependencies missing: 'node' or 'cargo' must be available in PATH"
    exit 1
}

Write-Host "==> Smoke test initializing with isolated project '$projectName' on port $smokePort..."

# Register cleanup in try/finally to ensure cleanup runs even if startup fails
try {
    Write-Host "==> Building and launching isolated Docker stack..."
    docker compose -p $projectName -f docker-compose.smoke.yml up --build -d

    $baseUrl = "http://127.0.0.1:$smokePort"
    $wsUrl = "ws://127.0.0.1:$smokePort/ws"

    Write-Host "==> Waiting for services to become healthy at $baseUrl..."
    $healthy = $false
    for ($i = 1; $i -le 60; $i++) {
        try {
            $resp = Invoke-RestMethod -Uri "$baseUrl/health" -Method Get -TimeoutSec 2
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
        docker compose -p $projectName -f docker-compose.smoke.yml logs
        exit 1
    }

    Write-Host "==> Check 1: Dashboard HTML delivery through Nginx (port $smokePort)..."
    $html = Invoke-WebRequest -Uri "$baseUrl/" -Method Get
    if ($html.Content -notmatch "root" -and $html.Content -notmatch "Low-Latency Order Book") {
        Write-Error "Dashboard HTML failed delivery"
    }
    Write-Host "Static assets delivered successfully!"

    Write-Host "==> Check 2: Feed rate control policy via reverse proxy..."
    $rateBody = '{"orders_per_second": 3000}'
    $postRate = Invoke-RestMethod -Uri "$baseUrl/feed-rate" -Method Post -Body $rateBody -ContentType "application/json"
    if ($postRate.orders_per_second -ne 3000) {
        Write-Error "Failed to update feed rate to 3000"
    }
    $getRate = Invoke-RestMethod -Uri "$baseUrl/feed-rate" -Method Get
    if ($getRate.orders_per_second -ne 3000) {
        Write-Error "Failed to verify feed rate 3000"
    }
    Write-Host "Rate control policy verified!"

    Write-Host "==> Check 3: Persistence growth in TimescaleDB..."
    $initialHealth = Invoke-RestMethod -Uri "$baseUrl/health" -Method Get
    $initialRows = $initialHealth.persisted_rows
    Start-Sleep -Seconds 2
    $finalHealth = Invoke-RestMethod -Uri "$baseUrl/health" -Method Get
    $finalRows = $finalHealth.persisted_rows

    if ($finalRows -le $initialRows) {
        Write-Error "Persisted rows did not increase ($initialRows -> $finalRows)"
    }
    Write-Host "Persistence verified: rows increased from $initialRows to $finalRows!"

    Write-Host "==> Check 4: Continuous aggregate candle query with non-empty validation..."
    $candlesVerified = $false
    for ($i = 1; $i -le 20; $i++) {
        $nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        $fromMs = $nowMs - 60000
        try {
            $candleResp = Invoke-RestMethod -Uri "$baseUrl/candles?from=$fromMs&to=$nowMs&interval=1s" -Method Get
            if ($candleResp.candles -and $candleResp.candles.Count -gt 0) {
                $firstBar = $candleResp.candles[0]
                if ($null -ne $firstBar.ts -and $null -ne $firstBar.open -and $null -ne $firstBar.high -and $null -ne $firstBar.low -and $null -ne $firstBar.close -and $null -ne $firstBar.volume -and $firstBar.volume -gt 0) {
                    $candlesVerified = $true
                    Write-Host "Verified valid OHLCV candle row: ts=$($firstBar.ts), open=$($firstBar.open), high=$($firstBar.high), low=$($firstBar.low), close=$($firstBar.close), vol=$($firstBar.volume)"
                    break
                }
            }
        } catch {}
        Start-Sleep -Seconds 1
    }

    if (-not $candlesVerified) {
        Write-Error "No populated OHLCV candle bars produced within 20s"
    }

    Write-Host "==> Check 5: WebSocket streaming through Nginx reverse proxy..."
    if (Get-Command node -ErrorAction SilentlyContinue) {
        node scripts/ws_probe.mjs $wsUrl 25
        if ($LASTEXITCODE -ne 0) {
            Write-Error "WebSocket probe failed with exit code $LASTEXITCODE"
        }
    } elseif (Get-Command cargo -ErrorAction SilentlyContinue) {
        cargo run -p server --bin ws_probe -- --url $wsUrl --count 25
        if ($LASTEXITCODE -ne 0) {
            Write-Error "WebSocket probe failed with exit code $LASTEXITCODE"
        }
    } else {
        Write-Error "Required WebSocket probe dependencies missing (node or cargo required)"
    }

    Write-Host "==> All isolated container smoke test checks passed successfully!"
} finally {
    Write-Host "==> Tearing down isolated smoke test stack ($projectName)..."
    docker compose -p $projectName -f docker-compose.smoke.yml down -v --remove-orphans
}
