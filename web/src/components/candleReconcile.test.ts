import { describe, expect, it } from "vitest";

import type { Candle } from "../hooks/useCandles";
import { reconcileHistoryCandles } from "./candleReconcile";

describe("reconcileHistoryCandles", () => {
  it("replaces completed candle with authoritative history after disconnect and missed trades", () => {
    const buckets = new Map<number, Candle>();

    // Client had observed early trades at second 10,000, but disconnected at 10,200 ms.
    // Client has close = 100, volume = 5.
    buckets.set(10000, {
      ts: 10000,
      open: 98,
      high: 102,
      low: 97,
      close: 100,
      volume: 5,
    });

    // Currently at second 11,000 (second 10,000 is completed).
    const activeSecondTs = 11000;

    // Database reports the authoritative completed candle for second 10,000:
    // After disconnect, trades traded up to 105 and closed at 104 with total volume 25.
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
    // Authoritative close, high, and volume replaced the stale disconnected bucket
    expect(reconciled.close).toBe(104);
    expect(reconciled.high).toBe(105);
    expect(reconciled.volume).toBe(25);
    expect(reconciled.open).toBe(98);
  });

  it("preserves active candle live close while reconciling bounds", () => {
    const buckets = new Map<number, Candle>();

    // Active ongoing second is 12,000.
    const activeSecondTs = 12000;

    // Client has already seen fresh live trade closing at 108
    buckets.set(12000, {
      ts: 12000,
      open: 100,
      high: 108,
      low: 99,
      close: 108,
      volume: 15,
    });

    // History query snapshot arrives for 12,000 from an earlier millisecond (close was 102)
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
    // Live latest close must be preserved!
    expect(reconciled.close).toBe(108);
    // Lower bound merged
    expect(reconciled.low).toBe(98);
    // Upper bound merged
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
