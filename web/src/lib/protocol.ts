// Wire types, runtime validation, and book reconstruction for the /ws stream.

export const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER; // 2^53 - 1 (9,007,199,254,740,991)

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

export type ApplyStatus = "applied" | "ignored" | "gap";

export interface ApplyResult {
  status: ApplyStatus;
  message?: string;
}

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

  apply(message: WireMessage): ApplyResult {
    if (message.type === "snapshot") {
      this.bids = new Map(message.bids);
      this.asks = new Map(message.asks);
      this.lastSeq = message.seq;
      this.synced = true;
      this.desynced = false;
      return { status: "applied" };
    }

    if (!this.synced || this.desynced) {
      return { status: "ignored", message: "book is not synced or is desynced" };
    }

    if (message.seq <= this.lastSeq) {
      return {
        status: "ignored",
        message: `stale message: seq ${message.seq} <= lastSeq ${this.lastSeq}`,
      };
    }
    if (message.seq !== this.lastSeq + 1) {
      this.desynced = true;
      return {
        status: "gap",
        message: `sequence gap: expected ${this.lastSeq + 1}, got ${message.seq}`,
      };
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
    return { status: "applied" };
  }

  /** Best bids (descending) and best asks (ascending). */
  ladder(depth: number): { bids: Array<[number, number]>; asks: Array<[number, number]> } {
    return {
      bids: [...this.bids.entries()].sort((a, b) => b[0] - a[0]).slice(0, depth),
      asks: [...this.asks.entries()].sort((a, b) => a[0] - b[0]).slice(0, depth),
    };
  }
}

function isSafePositiveInt(n: unknown): n is number {
  return typeof n === "number" && Number.isSafeInteger(n) && n > 0 && n <= MAX_SAFE_INTEGER;
}

function isSafeNonNegativeInt(n: unknown): n is number {
  return typeof n === "number" && Number.isSafeInteger(n) && n >= 0 && n <= MAX_SAFE_INTEGER;
}

function isSide(s: unknown): s is Side {
  return s === "bid" || s === "ask";
}

/**
 * Validates wire messages at runtime: checks supported type, required fields,
 * valid side strings, tuple structure, and numeric bounds.
 */
export function isWireMessage(value: unknown): value is WireMessage {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.type !== "string" || !isSafeNonNegativeInt(obj.seq)) {
    return false;
  }

  switch (obj.type) {
    case "snapshot": {
      if (!Array.isArray(obj.bids) || !Array.isArray(obj.asks)) {
        return false;
      }
      for (const item of obj.bids) {
        if (
          !Array.isArray(item) ||
          item.length !== 2 ||
          !isSafePositiveInt(item[0]) ||
          !isSafePositiveInt(item[1])
        ) {
          return false;
        }
      }
      for (const item of obj.asks) {
        if (
          !Array.isArray(item) ||
          item.length !== 2 ||
          !isSafePositiveInt(item[0]) ||
          !isSafePositiveInt(item[1])
        ) {
          return false;
        }
      }
      return true;
    }
    case "delta": {
      return (
        isSide(obj.side) &&
        isSafePositiveInt(obj.price) &&
        isSafeNonNegativeInt(obj.qty)
      );
    }
    case "trade": {
      return (
        isSafePositiveInt(obj.price) &&
        isSafePositiveInt(obj.qty) &&
        isSide(obj.taker_side) &&
        isSafeNonNegativeInt(obj.ts)
      );
    }
    case "ack": {
      return isSafeNonNegativeInt(obj.id);
    }
    case "cancel": {
      return isSafeNonNegativeInt(obj.id);
    }
    case "reject": {
      return isSafeNonNegativeInt(obj.id) && typeof obj.reason === "string";
    }
    default:
      return false;
  }
}
