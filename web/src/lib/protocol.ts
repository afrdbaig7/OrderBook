// Wire protocol for the `/ws` market data stream, plus a pure TypeScript
// reference implementation of the client-side book.

export type Side = "bid" | "ask";

export interface Snapshot {
  type: "snapshot";
  seq: number;
  bids: Array<[number, number]>;
  asks: Array<[number, number]>;
}

export interface Delta {
  type: "delta";
  seq: number;
  side: Side;
  price: number;
  qty: number;
}

export interface Trade {
  type: "trade";
  seq: number;
  price: number;
  qty: number;
  taker_side: Side;
  ts: number;
}

export interface Ack {
  type: "ack";
  seq: number;
  id: number;
}

export interface Cancel {
  type: "cancel";
  seq: number;
  id: number;
}

export interface Reject {
  type: "reject";
  seq: number;
  id: number;
  reason: string;
}

export type WireMessage = Snapshot | Delta | Trade | Ack | Cancel | Reject;

// ── Client-side reconstruction (mirror of crate `feed`'s `ClientBook`) ──

/**
 * A price-indexed book image rebuilt purely from wire messages.
 *
 * Same rules as the Rust reference:
 * - a `snapshot` replaces all state and resyncs;
 * - messages before the first snapshot are ignored;
 * - `seq <= lastSeq` is stale → ignored;
 * - `seq === lastSeq + 1` → applied;
 * - `seq > lastSeq + 1` → a message was dropped → `desynced`;
 *   the client must reconnect and take a fresh snapshot.
 */
export class PureOrderBook {
  bids = new Map<number, number>();
  asks = new Map<number, number>();
  lastSeq = 0;
  synced = false;
  desynced = false;

  apply(message: WireMessage): void {
    if (message.type === "snapshot") {
      this.bids = new Map(message.bids);
      this.asks = new Map(message.asks);
      this.lastSeq = message.seq;
      this.synced = true;
      this.desynced = false;
      return;
    }

    if (!this.synced || this.desynced) {
      return;
    }

    if (message.seq <= this.lastSeq) {
      return;
    }
    if (message.seq !== this.lastSeq + 1) {
      this.desynced = true;
      return;
    }

    if (message.type === "delta") {
      const levels = message.side === "bid" ? this.bids : this.asks;
      if (message.qty === 0) {
        levels.delete(message.price);
      } else {
        levels.set(message.price, message.qty);
      }
    }

    this.lastSeq = message.seq;
  }

  /** Best bids (descending) and best asks (ascending). */
  ladder(depth: number): { bids: Array<[number, number]>; asks: Array<[number, number]> } {
    return {
      bids: [...this.bids.entries()].sort((a, b) => b[0] - a[0]).slice(0, depth),
      asks: [...this.asks.entries()].sort((a, b) => a[0] - b[0]).slice(0, depth),
    };
  }
}

export function isWireMessage(value: unknown): value is WireMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { type?: unknown }).type === "string"
  );
}