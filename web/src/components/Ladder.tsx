import type { LadderSnapshot, LadderRow } from "../lib/store";

/**
 * Depth ladder: asks with the best (lowest) at the bottom, spread,
 * then bids with the best (highest) at the top — so the two best prices sit
 * adjacent around the spread. Columns are price / size / cumulative size; the
 * depth bar is proportional to **cumulative** size. Values stay integers here;
 * formatting happens at the edge.
 */
export function Ladder({ ladder }: { ladder: LadderSnapshot }) {
  const maxCumulative = Math.max(
    1,
    ...ladder.bids.map((row) => row.cumulative),
    ...ladder.asks.map((row) => row.cumulative),
  );

  const spread =
    ladder.asks.length > 0 && ladder.bids.length > 0
      ? ladder.asks[0].price - ladder.bids[0].price
      : null;

  const renderRow = (row: LadderRow, side: "bid" | "ask") => {
    const width = Math.round((row.cumulative / maxCumulative) * 100);
    return (
      <div className={`row ${side}`} key={`${side}-${row.price}`} style={{ position: "relative" }}>
        <div
          className="depth-bar"
          style={{
            width: `${width}%`,
            background: side === "bid" ? "rgba(15,153,96,.16)" : "rgba(214,69,69,.16)",
          }}
        />
        <span>{row.price}</span>
        <span className="num">{row.qty}</span>
        <span className="num">{row.cumulative}</span>
      </div>
    );
  };

  // Asks: worst (highest price) at top, best at bottom.
  const asks = ladder.asks.slice().reverse();

  return (
    <section className="book">
      <h2>Depth</h2>
      <div className="row header">
        <span>price</span>
        <span className="num">size</span>
        <span className="num">cum</span>
      </div>
      {asks.map((row) => renderRow(row, "ask"))}
      <div className="row spread-row">
        <span>spread {spread === null ? "–" : spread}</span>
      </div>
      {ladder.bids.map((row) => renderRow(row, "bid"))}
    </section>
  );
}
