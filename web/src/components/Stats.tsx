import type { ConnectionStatus, OrderBookState } from "../hooks/useOrderBook";

function Stat({ label, value, tone }: { label: string; value: string; tone?: "bid" | "ask" }) {
  return (
    <div className="stat">
      <span className="stat-label">{label}</span>
      <span className={`stat-value ${tone ?? ""}`}>{value}</span>
    </div>
  );
}

/** Header readouts: connection, spread/midpoint/last, and throughput (spec §39). */
export function Stats({
  state,
  status,
}: {
  state: OrderBookState;
  status: ConnectionStatus;
}) {
  const { snapshot, messagesPerSecond, tradesPerSecond } = state;
  const lastTone = snapshot.lastPrice === null ? undefined : "bid";

  return (
    <div className="stats">
      <span className={`pill ${status === "live" ? "live" : "dead"}`}>{status}</span>
      <Stat
        label="mid"
        value={snapshot.midpoint === null ? "–" : snapshot.midpoint.toFixed(1)}
      />
      <Stat label="spread" value={snapshot.spread === null ? "–" : String(snapshot.spread)} />
      <Stat
        label="last"
        value={snapshot.lastPrice === null ? "–" : String(snapshot.lastPrice)}
        tone={lastTone}
      />
      <Stat label="msg/s" value={messagesPerSecond.toLocaleString()} />
      <Stat label="trd/s" value={tradesPerSecond.toLocaleString()} />
      <Stat label="seq" value={snapshot.seq.toLocaleString()} />
    </div>
  );
}
