import { describe, expect, it } from "vitest";

import type { Candle } from "../hooks/useCandles";
import { reconcileHistoryCandles } from "./candleReconcile";

describe("reconcileHistoryCandles", () => {
  it("repairs a settled candle after missed trades", () => {
    const buckets = new Map<number, Candle>();

    // Disconnect partway through the bucket starting at 10,000 ms.
    buckets.set(10000, {
      ts: 10000,
      open: 98,
      high: 102,
      low: 97,
      close: 100,
      volume: 5,
    });

    const activeSecondTs = 11000;

    // History includes the trades that arrived after the disconnect.
    const history: Candle[] = [
      {
        ts: 10000,
        open: 98,
        high: 105,
        low: 97,
        close: 104,
        volume: 25,
      },
    ];

    reconcileHistoryCandles(buckets, history, activeSecondTs, 120);

    const reconciled = buckets.get(10000)!;
    expect(reconciled).toBeDefined();
    expect(reconciled.close).toBe(104);
    expect(reconciled.high).toBe(105);
    expect(reconciled.volume).toBe(25);
    expect(reconciled.open).toBe(98);
  });

  it("keeps the live close when active history is older", () => {
    const buckets = new Map<number, Candle>();

    const activeSecondTs = 12000;

    // The client has already seen a newer trade at 108.
    buckets.set(12000, {
      ts: 12000,
      open: 100,
      high: 108,
      low: 99,
      close: 108,
      volume: 15,
    });

    // The response was captured before the live trade at 108.
    const history: Candle[] = [
      {
        ts: 12000,
        open: 100,
        high: 105,
        low: 98, // new lower bound from history
        close: 102, // earlier close
        volume: 10,
      },
    ];

    reconcileHistoryCandles(buckets, history, activeSecondTs, 120);

    const reconciled = buckets.get(12000)!;
    expect(reconciled.close).toBe(108);
    expect(reconciled.low).toBe(98);
    expect(reconciled.high).toBe(108);
  });

  it("prunes buckets outside the rolling retention window", () => {
    const buckets = new Map<number, Candle>();

    // Populate candles across 150 seconds (100,000 to 250,000)
    for (let s = 100; s <= 250; s += 10) {
      const ts = s * 1000;
      buckets.set(ts, {
        ts,
        open: 100,
        high: 105,
        low: 95,
        close: 100,
        volume: 10,
      });
    }

    const history: Candle[] = [
      {
        ts: 250000,
        open: 100,
        high: 105,
        low: 95,
        close: 102,
        volume: 10,
      },
    ];

    // Retention window = 60 seconds (cutoff = 250,000 - 60,000 = 190,000)
    reconcileHistoryCandles(buckets, history, 250000, 60);

    expect(buckets.has(100000)).toBe(false);
    expect(buckets.has(180000)).toBe(false);
    expect(buckets.has(190000)).toBe(true);
    expect(buckets.has(250000)).toBe(true);
  });
});
