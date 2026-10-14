import { describe, expect, it } from "vitest";

import type { Snapshot, Trade } from "./protocol";
import { MarketStore, TAPE_LIMIT, withCumulative } from "./store";

const snapshot = (overrides: Partial<Snapshot> = {}): Snapshot => ({
  type: "snapshot",
  seq: 10,
  bids: [
    [100, 50],
    [99, 120],
  ],
  asks: [
    [101, 30],
    [102, 75],
  ],
  ...overrides,
});

const trade = (seq: number, price = 101, qty = 5): Trade => ({
  type: "trade",
  seq,
  price,
  qty,
  taker_side: "bid",
  ts: 1_730_000_000_000,
});

describe("MarketStore reconstruction", () => {
  it("applies the snapshot and derives spread/midpoint", () => {
    const store = new MarketStore();
    store.apply(snapshot());
    const view = store.render(10);
    expect(view.synced).toBe(true);
    expect(view.seq).toBe(10);
    expect(view.spread).toBe(1);
    expect(view.midpoint).toBe(100.5);
  });

  it("ignores messages before the first snapshot (but buffers them)", () => {
    const store = new MarketStore();
    store.apply({ type: "delta", seq: 11, side: "bid", price: 100, qty: 500 });
    expect(store.book.synced).toBe(false);
    expect(store.book.bids.size).toBe(0);
  });

  it("replays deltas buffered before the snapshot once it arrives", () => {
    const store = new MarketStore();
    // These arrive *before* the snapshot (out-of-order on the wire).
    store.apply({ type: "delta", seq: 11, side: "bid", price: 100, qty: 60 });
    store.apply({ type: "delta", seq: 12, side: "ask", price: 102, qty: 0 });
    // Now the snapshot replaces state and the buffer flushes in order.
    store.apply(snapshot());

    const view = store.render(10);
    expect(view.synced).toBe(true);
    expect(view.seq).toBe(12);
    expect(store.book.bids.get(100)).toBe(60);
    expect(store.book.asks.has(102)).toBe(false);
  });

  it("drops buffered messages already covered by the snapshot", () => {
    const store = new MarketStore();
    store.apply({ type: "delta", seq: 8, side: "bid", price: 98, qty: 1 });
    store.apply(snapshot());
    expect(store.render(10).seq).toBe(10);
    expect(store.book.bids.has(98)).toBe(false);
  });

  it("deletes a level when a delta has qty 0", () => {
    const store = new MarketStore();
    store.apply(snapshot());
    store.apply({ type: "delta", seq: 11, side: "ask", price: 102, qty: 0 });
    expect(store.book.asks.has(102)).toBe(false);
  });

  it("flags a sequence gap as desynced and applies nothing after it", () => {
    const store = new MarketStore();
    store.apply(snapshot());
    store.apply({ type: "delta", seq: 13, side: "bid", price: 98, qty: 1 });
    expect(store.book.desynced).toBe(true);
    expect(store.render(10).desynced).toBe(true);
    expect(store.book.bids.has(98)).toBe(false);
  });

  it("resyncs on a fresh snapshot after a gap", () => {
    const store = new MarketStore();
    store.apply(snapshot());
    store.apply({ type: "delta", seq: 13, side: "bid", price: 98, qty: 1 });
    expect(store.book.desynced).toBe(true);

    store.apply({ ...snapshot(), seq: 100, bids: [[200, 1]], asks: [[201, 1]] });
    const view = store.render(10);
    expect(view.desynced).toBe(false);
    expect(view.seq).toBe(100);
    expect(store.book.bids.get(200)).toBe(1);
  });

  it("counts messages and trades", () => {
    const store = new MarketStore();
    store.apply(snapshot());
    store.apply(trade(11));
    store.apply(trade(12));
    const view = store.render(10);
    expect(view.messages).toBe(3);
    expect(view.trades).toBe(2);
    expect(view.lastPrice).toBe(101);
  });
});

describe("withCumulative", () => {
  it("adds a running total in best-to-worst order", () => {
    expect(withCumulative([
      [100, 50],
      [99, 120],
      [98, 30],
    ])).toEqual([
      { price: 100, qty: 50, cumulative: 50 },
      { price: 99, qty: 120, cumulative: 170 },
      { price: 98, qty: 30, cumulative: 200 },
    ]);
  });
});

describe("MarketStore tape", () => {
  it("keeps newest first and caps the list at TAPE_LIMIT", () => {
    const store = new MarketStore();
    store.apply(snapshot());
    for (let seq = 11; seq <= 10 + TAPE_LIMIT + 10; seq += 1) {
      store.apply(trade(seq, 101 + (seq % 3)));
    }
    const view = store.render(10);
    expect(view.tape).toHaveLength(TAPE_LIMIT);
    // Newest first: the last applied seq is at the front.
    expect(view.tape[0].seq).toBe(10 + TAPE_LIMIT + 10);
    expect(view.tape[view.tape.length - 1].seq).toBe(11 + 10);
  });

  it("notifies trade listeners for every trade", () => {
    const store = new MarketStore();
    const seen: number[] = [];
    const off = store.onTrade((t) => seen.push(t.seq));
    store.apply(snapshot());
    store.apply(trade(11));
    store.apply(trade(12));
    off();
    store.apply(trade(13));
    expect(seen).toEqual([11, 12]);
  });
});
