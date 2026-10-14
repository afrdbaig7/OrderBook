import { useEffect, useState } from "react";

export interface Candle {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

/**
 * Loads 1-second candle history from `/candles` (the TimescaleDB continuous
 * aggregate) and refreshes it periodically. The live edge is maintained
 * separately from trade messages (see `CandleChart`), so a slow refresh here
 * never freezes the chart. When persistence is disabled the endpoint 503s and
 * the chart simply runs on live trades.
 */
export function useCandles(seconds = 120, interval = "1s") {
  const [candles, setCandles] = useState<Candle[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let disposed = false;

    const load = async () => {
      const to = Date.now();
      const from = to - seconds * 1000;
      try {
        const res = await fetch(`/candles?from=${from}&to=${to}&interval=${interval}`);
        if (!res.ok) {
          if (!disposed) {
            setError(`HTTP ${res.status}`);
          }
          return;
        }
        const data = (await res.json()) as { candles: Candle[] };
        if (!disposed) {
          setCandles(data.candles);
          setError(null);
        }
      } catch {
        if (!disposed) {
          setError("candles endpoint unreachable");
        }
      }
    };

    void load();
    const timer = window.setInterval(load, 5000);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [seconds, interval]);

  return { candles, error };
}