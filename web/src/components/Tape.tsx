import type { Trade } from "../lib/protocol";

function formatTime(ts: number): string {
  const date = new Date(ts);
  const pad = (n: number, width = 2) => String(n).padStart(width, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(
    date.getMilliseconds(),
    3,
  )}`;
}

/**
 * Trade tape (spec §42): the latest [`MAX`] executions, newest first, bounded
 * (the store caps the list). Bid-taking trades print green, ask-taking red.
 */
export function Tape({ trades }: { trades: Trade[] }) {
  return (
    <section className="tape">
      <h2>Trades</h2>
      <div className="row header">
        <span>price</span>
        <span className="num">size</span>
        <span className="num">time</span>
      </div>
      {trades.length === 0 ? (
        <div className="muted" style={{ padding: "0.5rem 0.75rem" }}>
          waiting for executions…
        </div>
      ) : (
        trades.map((trade) => (
          <div className={`row ${trade.taker_side}`} key={trade.seq}>
            <span>{trade.price}</span>
            <span className="num">{trade.qty}</span>
            <span className="num muted">{formatTime(trade.ts)}</span>
          </div>
        ))
      )}
    </section>
  );
}
