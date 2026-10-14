//! HTTP endpoints: `/health`, `/candles`, `/feed-rate` (spec §5, §31, §35).

use std::sync::atomic::Ordering;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::metrics::MetricsSnapshot;
use crate::state::AppState;

/// `GET /health` — liveness plus a point-in-time metrics read.
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    /// Depth of the bounded engine input channel (0 ⇒ engine keeping up).
    pub command_channel_depth: usize,
    /// Current target feed rate in orders/second.
    pub feed_rate: u64,
    /// `true` when a `DATABASE_URL` was configured.
    pub persistence_enabled: bool,
    #[serde(flatten)]
    pub metrics: MetricsSnapshot,
}

/// `GET /health`.
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let persistence_enabled = state.persistence.read().await.is_some();
    Json(HealthResponse {
        status: "ok",
        command_channel_depth: state.cmd_tx.len(),
        feed_rate: state.feed_rate.load(Ordering::Relaxed),
        persistence_enabled,
        metrics: state.metrics.snapshot(),
    })
}

/// Query string for `GET /candles`.
#[derive(Deserialize)]
pub struct CandlesQuery {
    /// Window start, epoch milliseconds.
    pub from: i64,
    /// Window end, epoch milliseconds.
    pub to: i64,
    /// Only `"1s"` is supported; anything else is a `400`.
    #[serde(default = "default_interval")]
    pub interval: String,
}

fn default_interval() -> String {
    "1s".to_string()
}

/// One OHLCV bar.
#[derive(Serialize)]
pub struct Candle {
    /// Bucket start, epoch milliseconds.
    pub ts: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
}

#[derive(Serialize)]
pub struct CandlesResponse {
    pub interval: String,
    pub candles: Vec<Candle>,
}

/// Reads 1-second candles from the TimescaleDB continuous aggregate.
const CANDLES_SQL: &str = "\
SELECT (extract(epoch FROM bucket) * 1000)::bigint AS ts,
       open::bigint   AS open,
       high::bigint   AS high,
       low::bigint    AS low,
       close::bigint  AS close,
       volume::bigint AS volume
FROM candles_1s
WHERE bucket >= to_timestamp($1 / 1000.0)
  AND bucket <= to_timestamp($2 / 1000.0)
ORDER BY bucket";

/// `GET /candles?from=<ms>&to=<ms>&interval=1s`.
pub async fn candles(
    State(state): State<AppState>,
    Query(query): Query<CandlesQuery>,
) -> Result<Json<CandlesResponse>, (StatusCode, String)> {
    if query.interval != "1s" {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "unsupported interval '{}': only '1s' is available",
                query.interval
            ),
        ));
    }
    if query.to < query.from {
        return Err((
            StatusCode::BAD_REQUEST,
            "'to' must be greater than or equal to 'from'".to_string(),
        ));
    }
    let guard = state.persistence.read().await;
    let pool = guard.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "persistence is disabled (no DATABASE_URL configured)".to_string(),
    ))?;

    let rows = sqlx::query(CANDLES_SQL)
        .bind(query.from)
        .bind(query.to)
        .fetch_all(pool)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("candle query failed: {error}"),
            )
        })?;

    let candles = rows
        .iter()
        .map(|row| Candle {
            ts: row.get("ts"),
            open: row.get("open"),
            high: row.get("high"),
            low: row.get("low"),
            close: row.get("close"),
            volume: row.get("volume"),
        })
        .collect();

    Ok(Json(CandlesResponse {
        interval: query.interval,
        candles,
    }))
}

/// `GET /feed-rate`.
pub async fn get_feed_rate(State(state): State<AppState>) -> Json<FeedRateResponse> {
    Json(FeedRateResponse {
        orders_per_second: state.feed_rate.load(Ordering::Relaxed),
    })
}

/// Body for `POST /feed-rate`.
#[derive(Deserialize)]
pub struct SetFeedRate {
    pub orders_per_second: u64,
}

/// `POST /feed-rate`.
pub async fn set_feed_rate(
    State(state): State<AppState>,
    Json(body): Json<SetFeedRate>,
) -> Json<FeedRateResponse> {
    state
        .feed_rate
        .store(body.orders_per_second, Ordering::Relaxed);
    Json(FeedRateResponse {
        orders_per_second: body.orders_per_second,
    })
}

#[derive(Serialize)]
pub struct FeedRateResponse {
    pub orders_per_second: u64,
}
