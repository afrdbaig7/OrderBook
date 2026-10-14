-- db/schema.sql — applied to a fresh TimescaleDB instance by
-- docker-compose (mounted into /docker-entrypoint-initdb.d), or manually:
--
--   psql "$DATABASE_URL" -f db/schema.sql

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- §34 — one row per executed trade. Integer prices/quantities; taker side is
-- an SMALLINT (0 = bid, 1 = ask). The table records *market data*, not a
-- ledger: no order identities are stored.
CREATE TABLE IF NOT EXISTS trades (
    ts         TIMESTAMPTZ NOT NULL,          -- server receive time
    seq        BIGINT      NOT NULL,          -- engine sequence number
    price      BIGINT      NOT NULL,          -- maker (resting) price
    qty        BIGINT      NOT NULL,          -- executed quantity
    taker_side SMALLINT    NOT NULL,          -- 0 = bid, 1 = ask
    PRIMARY KEY (ts, seq)
);

SELECT create_hypertable('trades', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS trades_seq_idx ON trades (seq);

-- §35 — 1-second OHLCV continuous aggregate. `first`/`last` are
-- TimescaleDB's sort-based aggregates; everything is cast back to bigint so
-- the API layer reads plain integers.
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1s
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 second', ts) AS bucket,
       first(price, ts)::bigint AS open,
       max(price)           AS high,
       min(price)           AS low,
       last(price, ts)::bigint AS close,
       sum(qty)::bigint     AS volume
FROM trades
GROUP BY bucket
WITH NO DATA;

-- Keep a 1-hour window materialized just behind the live edge; the most
-- recent second is always covered by real-time aggregation, so `/candles`
-- returns fresh results immediately.
SELECT add_continuous_aggregate_policy('candles_1s',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 second',
    schedule_interval => INTERVAL '1 second',
    if_not_exists     => TRUE);