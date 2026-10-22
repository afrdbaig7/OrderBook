// WebSocket messages update this store without rendering React. The hook
// publishes a display snapshot at most once per animation frame.

import { PureOrderBook, type Trade, type WireMessage } from "./protocol";

/** Trade tape is bounded. */
export const TAPE_LIMIT = 50;
/** Default ladder depth published to the UI. */
export const DEFAULT_DEPTH = 20;

export interface LadderRow {
  price: number;
  qty: number;
  cumulative: number;
}

export interface LadderSnapshot {
  bids: LadderRow[];
  asks: LadderRow[];
}

/** Immutable point-in-time image handed to React. */
export interface BookSnapshot {
  ladder: LadderSnapshot;
  /** Most recent trades, newest first, capped at [`TAPE_LIMIT`]. */
  tape: Trade[];
  seq: number;
  synced: boolean;
  desynced: boolean;
  midpoint: number | null;
  spread: number | null;
  lastPrice: number | null;
  messages: number;
  trades: number;
}

export type TradeListener = (trade: Trade) => void;

/** Adds a running total to each level, best-to-worst order preserved. */
export function withCumulative(levels: Array<[number, number]>): LadderRow[] {
  let cumulative = 0;
  return levels.map(([price, qty]) => {
    cumulative += qty;
    return { price, qty, cumulative };
  });
}

/**
 * Owns the reconstructed book, the tape and the counters. Applies the exact
 * client synchronization rules of the Rust reference (`crate feed::ClientBook`):
 *
 * 1. buffer deltas that arrive before the first snapshot;
 * 2. on the snapshot, replace state, then replay the buffer;
 * 3. apply later messages only in strict `seq` order;
 * 4. a gap marks the book `desynced` (the caller reconnects for a new snapshot);
 * 5. a delta with `qty === 0` deletes the level.
 */
export class MarketStore {
  readonly book = new PureOrderBook();
  private tape: Trade[] = [];
  private buffer: WireMessage[] | null = null;
  private readonly listeners = new Set<TradeListener>();

  messages = 0;
  trades = 0;
  lastPrice: number | null = null;

  /** Subscribes to every executed trade as it arrives (used by the chart). */
  onTrade(listener: TradeListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  apply(message: WireMessage): void {
    this.messages += 1;

    if (!this.book.synced) {
      if (message.type === "snapshot") {
        this.book.apply(message);
        const buffered = this.buffer;
        this.buffer = null;
        if (buffered) {
          for (const pending of buffered) {
            this.applyToBook(pending);
          }
        }
      } else {
        (this.buffer ??= []).push(message);
      }
      return;
    }

    this.applyToBook(message);
  }

  private applyToBook(message: WireMessage): void {
    const result = this.book.apply(message);
    if (result.status !== "applied") {
      return;
    }
    if (message.type === "trade") {
      this.trades += 1;
      this.lastPrice = message.price;
      this.tape.unshift(message);
      if (this.tape.length > TAPE_LIMIT) {
        this.tape.length = TAPE_LIMIT;
      }
      for (const listener of this.listeners) {
        listener(message);
      }
    }
  }

  /** Builds an immutable snapshot for rendering. */
  render(depth: number = DEFAULT_DEPTH): BookSnapshot {
    const { bids, asks } = this.book.ladder(depth);
    const bestBid = bids.length > 0 ? bids[0][0] : null;
    const bestAsk = asks.length > 0 ? asks[0][0] : null;

    return {
      ladder: { bids: withCumulative(bids), asks: withCumulative(asks) },
      tape: this.tape.slice(),
      seq: this.book.lastSeq,
      synced: this.book.synced,
      desynced: this.book.desynced,
      midpoint:
        bestBid !== null && bestAsk !== null ? (bestBid + bestAsk) / 2 : null,
      spread: bestBid !== null && bestAsk !== null ? bestAsk - bestBid : null,
      lastPrice: this.lastPrice,
      messages: this.messages,
      trades: this.trades,
    };
  }
}

/** An empty snapshot, used before the first frame is published. */
export const EMPTY_SNAPSHOT: BookSnapshot = {
  ladder: { bids: [], asks: [] },
  tape: [],
  seq: 0,
  synced: false,
  desynced: false,
  midpoint: null,
  spread: null,
  lastPrice: null,
  messages: 0,
  trades: 0,
};
