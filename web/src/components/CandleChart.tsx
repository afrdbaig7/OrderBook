import { useEffect, useRef } from "react";
import {
  ColorType,
  CrosshairMode,
  createChart,
  type CandlestickData,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";

import { useCandles, type Candle } from "../hooks/useCandles";
import type { MarketStore } from "../lib/store";

function toBar(candle: Candle): CandlestickData<UTCTimestamp> {
  return {
    time: (candle.ts / 1000) as UTCTimestamp,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
  };
}

/**
 * 1-second candle chart.
 *
 * History comes from `GET /candles` (TimescaleDB); the live edge is maintained
 * from trade messages so the newest bar updates in real time even before the
 * aggregate refreshes. Live trades are folded into the in-progress bar and
 * pushed to the chart on `requestAnimationFrame`, so a high trade rate still
 * costs at most one chart update per frame.
 */
export function CandleChart({
  store,
  seconds = 120,
}: {
  store: MarketStore;
  seconds?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const bucketsRef = useRef<Map<number, Candle>>(new Map());
  const dirtyRef = useRef<Set<number>>(new Set());
  const rafRef = useRef<number | undefined>(undefined);

  const { candles, error } = useCandles(seconds);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) {
      return;
    }

    const chart = createChart(element, {
      height: 260,
      layout: {
        background: { type: ColorType.Solid, color: "#1d2027" },
        textColor: "#9aa4b2",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "#242832" },
        horzLines: { color: "#242832" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      timeScale: { timeVisible: true, secondsVisible: true, borderColor: "#2a2f38" },
      rightPriceScale: { borderColor: "#2a2f38" },
    });
    const series = chart.addCandlestickSeries({
      upColor: "#0f9960",
      downColor: "#d64545",
      borderVisible: false,
      wickUpColor: "#0f9960",
      wickDownColor: "#d64545",
    });
    chartRef.current = chart;
    seriesRef.current = series;

    const resize = () => chart.applyOptions({ width: element.clientWidth });
    resize();
    const observer = new ResizeObserver(resize);
    observer.observe(element);

    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  // History: merge /candles data, preserving active live bar and pruning outside window.
  useEffect(() => {
    const series = seriesRef.current;
    if (!series) {
      return;
    }
    const buckets = bucketsRef.current;
    let maxTs = 0;

    for (const candle of candles) {
      if (candle.ts > maxTs) {
        maxTs = candle.ts;
      }
      const existing = buckets.get(candle.ts);
      if (!existing) {
        buckets.set(candle.ts, candle);
      } else {
        // Reconcile: historical settled candle updates past buckets,
        // but for active/newer bars, merge high/low/volume and preserve latest live close.
        existing.high = Math.max(existing.high, candle.high);
        existing.low = Math.min(existing.low, candle.low);
        existing.volume = Math.max(existing.volume, candle.volume);
      }
    }

    // Prune buckets to the fixed rolling history window (e.g. 120s)
    if (maxTs > 0) {
      const cutoff = maxTs - seconds * 1000;
      for (const key of buckets.keys()) {
        if (key < cutoff) {
          buckets.delete(key);
        }
      }
    }

    const data = [...buckets.values()]
      .sort((a, b) => a.ts - b.ts)
      .map(toBar);
    series.setData(data);
  }, [candles, seconds]);

  // Live edge: fold trade messages into the current bar, flush per frame.
  useEffect(() => {
    const flush = () => {
      rafRef.current = undefined;
      const series = seriesRef.current;
      if (!series) {
        return;
      }
      const buckets = bucketsRef.current;
      for (const key of dirtyRef.current) {
        const candle = buckets.get(key);
        if (candle) {
          series.update(toBar(candle));
        }
      }
      dirtyRef.current.clear();
    };

    const unsubscribe = store.onTrade((trade) => {
      const ts = Math.floor(trade.ts / 1000) * 1000;
      const buckets = bucketsRef.current;
      const candle = buckets.get(ts);
      if (candle) {
        candle.high = Math.max(candle.high, trade.price);
        candle.low = Math.min(candle.low, trade.price);
        candle.close = trade.price;
        candle.volume += trade.qty;
      } else {
        buckets.set(ts, {
          ts,
          open: trade.price,
          high: trade.price,
          low: trade.price,
          close: trade.price,
          volume: trade.qty,
        });

        // Rolling window prune on new second bucket
        const cutoff = ts - seconds * 1000;
        for (const key of buckets.keys()) {
          if (key < cutoff) {
            buckets.delete(key);
          }
        }
      }
      dirtyRef.current.add(ts);
      if (rafRef.current === undefined) {
        rafRef.current = requestAnimationFrame(flush);
      }
    });

    return () => {
      unsubscribe();
      if (rafRef.current !== undefined) {
        cancelAnimationFrame(rafRef.current);
      }
    };
  }, [store, seconds]);

  return (
    <section className="chart">
      <h2>Candles (1s)</h2>
      {error && (
        <div className="muted" style={{ padding: "0.25rem 0.75rem" }}>
          history unavailable ({error}) — live bars still update from trades
        </div>
      )}
      <div ref={containerRef} />
    </section>
  );
}
