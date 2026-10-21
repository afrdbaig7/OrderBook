-- db/migrations/002_rebuild_candles_with_sequence.sql
--
-- Migration: Rebuild candles_1s continuous aggregate with sequence-ordered open/close.
--
-- Background:
-- Earlier schema versions defined candles_1s using `first(price, ts)` and `last(price, ts)`.
-- When hundreds of trades execute within the same millisecond timestamp, timestamp ordering
-- causes non-deterministic open and close prices.
--
-- This migration drops the old continuous aggregate and rebuilds it using `first(price, seq)`
-- and `last(price, seq)` to sequence trades strictly by monotonic engine sequence number.
-- It also sets `timescaledb.materialized_only = false` to enable real-time continuous aggregation.

-- 1. Remove continuous aggregate policy if present
SELECT remove_continuous_aggregate_policy('candles_1s', if_exists => true);

-- 2. Drop existing materialized view
DROP MATERIALIZED VIEW IF EXISTS candles_1s CASCADE;

-- 3. Recreate with sequence-ordered open/close and real-time aggregation enabled
CREATE MATERIALIZED VIEW candles_1s
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

-- 4. Re-add continuous aggregate policy
SELECT add_continuous_aggregate_policy('candles_1s',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 second',
    schedule_interval => INTERVAL '1 second',
    if_not_exists     => TRUE);
