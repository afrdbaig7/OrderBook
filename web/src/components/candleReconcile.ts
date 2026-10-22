import type { Candle } from "../hooks/useCandles";

/**
 * Repairs settled candles from history while preserving the active live close.
 *
 * History restores trades missed during a disconnect. For an active bucket,
 * merge its range and volume without overwriting newer live prices.
 * Retention is measured from the latest historical bucket.
 */
export function reconcileHistoryCandles(
  buckets: Map<number, Candle>,
  history: Candle[],
  activeSecondTs: number,
  retentionSeconds: number
): void {
  let latestHistoryTs = 0;

  for (const candle of history) {
    if (candle.ts > latestHistoryTs) {
      latestHistoryTs = candle.ts;
    }
    const existing = buckets.get(candle.ts);
    if (!existing) {
      buckets.set(candle.ts, { ...candle });
    } else if (candle.ts < activeSecondTs) {
      // Settled history repairs any trades missed while disconnected.
      buckets.set(candle.ts, { ...candle });
    } else {
      // The live close may be newer than this response.
      existing.open = existing.open || candle.open;
      existing.high = Math.max(existing.high, candle.high);
      existing.low = Math.min(existing.low, candle.low);
      existing.volume = Math.max(existing.volume, candle.volume);
    }
  }

  if (latestHistoryTs > 0) {
    const cutoff = latestHistoryTs - retentionSeconds * 1000;
    for (const key of buckets.keys()) {
      if (key < cutoff) {
        buckets.delete(key);
      }
    }
  }
}
