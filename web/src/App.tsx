import { useState } from "react";

import { CandleChart } from "./components/CandleChart";
import { Ladder } from "./components/Ladder";
import { Stats } from "./components/Stats";
import { Tape } from "./components/Tape";
import { useOrderBook } from "./hooks/useOrderBook";

export default function App() {
  const { state, store } = useOrderBook("/ws", 20);
  const [feedRate, setFeedRate] = useState(1000);
  const [savedRate, setSavedRate] = useState<number | null>(null);
  const [rateError, setRateError] = useState<string | null>(null);

  const applyFeedRate = async () => {
    setRateError(null);
    try {
      const res = await fetch("/feed-rate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ orders_per_second: feedRate }),
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const body = (await res.json()) as { orders_per_second: number };
      setSavedRate(body.orders_per_second);
    } catch (error) {
      setRateError(String(error));
    }
  };

  return (
    <>
      <header>
        <h1>Low-Latency Order Book</h1>
        <Stats state={state} status={state.status} />
        <label className="muted feed-control">
          feed (orders/s){" "}
          <input
            type="number"
            min={0}
            value={feedRate}
            onChange={(event) => setFeedRate(Number(event.target.value))}
          />
        </label>
        <button onClick={() => void applyFeedRate()}>set</button>
        {savedRate !== null && !rateError && <span className="muted">→ {savedRate}</span>}
        {rateError && <span className="pill dead">{rateError}</span>}
      </header>
      <main>
        <Ladder ladder={state.snapshot.ladder} />
        <Tape trades={state.snapshot.tape} />
        <CandleChart store={store} seconds={120} />
      </main>
    </>
  );
}
