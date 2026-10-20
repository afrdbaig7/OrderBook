import type { Candle } from "../hooks/useCandles";

/**
 * Reconciles historical candles fetched from TimescaleDB with local client buckets.
 *
 * Rules:
 * 1. For any completed candle (candle.ts < activeSecondTs), the historical candle from
 *    TimescaleDB is authoritative: it completely replaces the bucket (including open,
 *    high, low, close, volume), repairing any trades missed during disconnects.
 * 2. For the currently active candle (candle.ts === activeSecondTs), live trade updates
 *    take precedence for the latest close:
 *    - open: preserved from existing live open or history
 *    - high: Math.max(existing.high, candle.high)
 *    - low: Math.min(existing.low, candle.low)
 *    - close: existing.close (the latest live trade of the ongoing second)
 *    - volume: Math.max(existing.volume, candle.volume)
 * 3. Prunes buckets outside the rolling retention window (maxTs - seconds * 1000).
 */
export function reconcileHistoryCandles(
  buckets: Map<number, Candle>,
  history: Candle[],
  activeSecondTs: number,
  retentionSeconds: number
): void {
  let maxTs = 0;

  for (const candle of history) {
    if (candle.ts > maxTs) {
      maxTs = candle.ts;
    }
    const existing = buckets.get(candle.ts);
    if (!existing) {
      buckets.set(candle.ts, { ...candle });
    } else if (candle.ts < activeSecondTs) {
      // Completed candle: authoritative history completely replaces local bucket
      buckets.set(candle.ts, { ...candle });
    } else {
      // In-progress active candle: merge high/low/volume but preserve the latest live trade close
      existing.open = existing.open || candle.open;
      existing.high = Math.max(existing.high, candle.high);
      existing.low = Math.min(existing.low, candle.low);
      existing.volume = Math.max(existing.volume, candle.volume);
    }
  }

  // Prune buckets to the fixed rolling history window (e.g. 120s)
  if (maxTs > 0) {
    const cutoff = maxTs - retentionSeconds * 1000;
    for (const key of buckets.keys()) {
      if (key < cutoff) {
        buckets.delete(key);
      }
    }
  }
}
