-- db/schema.sql — applied to a fresh TimescaleDB instance by
-- docker-compose (mounted into /docker-entrypoint-initdb.d), or manually:
--
--   psql "$DATABASE_URL" -f db/schema.sql

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- One row per executed trade. Integer prices/quantities; taker side is
-- a SMALLINT (0 = bid, 1 = ask). The table records market data, not a
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

-- Upgrade path for existing databases:
-- If candles_1s was created under the older schema (ordered by timestamp instead of sequence),
-- drop it so the statement below can recreate it with `first(price, seq)`.
-- Also ensure `materialized_only = false` is active if already upgraded.
DO $$
DECLARE
    view_def TEXT;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'candles_1s') THEN
        SELECT pg_get_viewdef('candles_1s'::regclass) INTO view_def;
        IF view_def NOT LIKE '%seq%' THEN
            RAISE NOTICE 'Upgrading candles_1s continuous aggregate to sequence-ordered open/close...';
            PERFORM remove_continuous_aggregate_policy('candles_1s', if_exists => true);
            DROP MATERIALIZED VIEW candles_1s CASCADE;
        ELSE
            ALTER MATERIALIZED VIEW candles_1s SET (timescaledb.materialized_only = false);
        END IF;
    END IF;
END $$;

-- 1-second OHLCV continuous aggregate.
-- `first(price, seq)` and `last(price, seq)` order strictly by engine sequence number,
-- guaranteeing deterministic open and close prices even when hundreds of trades share
-- the exact same millisecond timestamp.
--
-- `timescaledb.materialized_only = false` explicitly enables real-time aggregation
-- (which TimescaleDB disables by default starting in version 2.13), ensuring queries
-- combine materialized historical data with the unmaterialized live edge.
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1s
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 second', ts) AS bucket,
       first(price, seq)::bigint AS open,
       max(price)                AS high,
       min(price)                AS low,
       last(price, seq)::bigint  AS close,
       sum(qty)::bigint          AS volume
FROM trades
GROUP BY bucket
WITH NO DATA;

-- Explicit top-level historical backfill. Must be executed outside any DO $$
-- or transaction blocks to satisfy TimescaleDB procedural execution requirements.
-- Backfill historical trades BEFORE adding the refresh policy so the initial backfill
-- does not collide with a concurrent background policy execution.
CALL refresh_continuous_aggregate('candles_1s', NULL, NULL);

-- Keep a 1-hour window materialized just behind the live edge; the most
-- recent second is always covered by real-time aggregation, so `/candles`
-- returns fresh results immediately.
SELECT add_continuous_aggregate_policy('candles_1s',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 second',
    schedule_interval => INTERVAL '1 second',
    if_not_exists     => TRUE);