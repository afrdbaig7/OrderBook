import { describe, expect, it } from "vitest";

import { isWireMessage, PureOrderBook, type Snapshot } from "./protocol";

const snapshot = (): Snapshot => ({
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
});

describe("PureOrderBook", () => {
  it("replaces all state on snapshot and syncs", () => {
    const book = new PureOrderBook();
    expect(book.synced).toBe(false);
    book.apply(snapshot());
    expect(book.synced).toBe(true);
    expect(book.desynced).toBe(false);
    expect(book.bids.get(100)).toBe(50);
    expect(book.asks.get(101)).toBe(30);
    expect(book.lastSeq).toBe(10);
  });

  it("ignores messages before the first snapshot", () => {
    const book = new PureOrderBook();
    book.apply({ type: "delta", seq: 1, side: "bid", price: 100, qty: 500 });
    expect(book.synced).toBe(false);
    expect(book.bids.size).toBe(0);
  });

  it("applies deltas only in strict seq order", () => {
    const book = new PureOrderBook();
    book.apply(snapshot());

    book.apply({ type: "delta", seq: 11, side: "bid", price: 100, qty: 60 });
    expect(book.bids.get(100)).toBe(60);
    expect(book.lastSeq).toBe(11);

    book.apply({ type: "trade", seq: 12, price: 100, qty: 5, taker_side: "ask", ts: 1 });
    expect(book.lastSeq).toBe(12);
    expect(book.bids.get(100)).toBe(60); // trades do not mutate levels
  });

  it("deletes a level when qty hits zero", () => {
    const book = new PureOrderBook();
    book.apply(snapshot());
    book.apply({ type: "delta", seq: 11, side: "ask", price: 102, qty: 0 });
    expect(book.asks.has(102)).toBe(false);
  });

  it("rejects stale messages (already covered by the snapshot)", () => {
    const book = new PureOrderBook();
    book.apply(snapshot());
    book.apply({ type: "delta", seq: 8, side: "bid", price: 98, qty: 1 });
    expect(book.lastSeq).toBe(10);
    expect(book.bids.has(98)).toBe(false);
  });

  it("flags a gap instead of applying out of order", () => {
    const book = new PureOrderBook();
    book.apply(snapshot());
    book.apply({ type: "delta", seq: 12, side: "bid", price: 98, qty: 1 });
    expect(book.desynced).toBe(true);
    expect(book.bids.has(98)).toBe(false);
  });

  it("resyncs on a fresh snapshot after a gap", () => {
    const book = new PureOrderBook();
    book.apply(snapshot());
    book.apply({ type: "delta", seq: 12, side: "bid", price: 98, qty: 1 });
    expect(book.desynced).toBe(true);

    book.apply({ ...snapshot(), seq: 100, bids: [[200, 1]], asks: [[201, 1]] });
    expect(book.desynced).toBe(false);
    expect(book.synced).toBe(true);
    expect(book.lastSeq).toBe(100);
    expect(book.bids.get(200)).toBe(1);
  });

  it("returns explicit ApplyResult status for every operation", () => {
    const book = new PureOrderBook();
    const res0 = book.apply({ type: "delta", seq: 1, side: "bid", price: 100, qty: 10 });
    expect(res0.status).toBe("ignored");

    const res1 = book.apply(snapshot());
    expect(res1.status).toBe("applied");

    const res2 = book.apply({ type: "delta", seq: 11, side: "bid", price: 100, qty: 60 });
    expect(res2.status).toBe("applied");

    // Stale message
    const res3 = book.apply({ type: "delta", seq: 11, side: "bid", price: 100, qty: 60 });
    expect(res3.status).toBe("ignored");

    // Sequence gap
    const res4 = book.apply({ type: "delta", seq: 15, side: "bid", price: 100, qty: 60 });
    expect(res4.status).toBe("gap");
  });
});

describe("isWireMessage runtime validation", () => {
  it("rejects incomplete snapshot objects", () => {
    // Specifically reproduces the bug: isWireMessage({type: "snapshot"}) returning true
    expect(isWireMessage({ type: "snapshot" })).toBe(false);
    expect(isWireMessage({ type: "snapshot", seq: 1 })).toBe(false);
    expect(isWireMessage({ type: "snapshot", seq: 1, bids: [] })).toBe(false);
  });

  it("validates valid snapshot messages", () => {
    expect(isWireMessage(snapshot())).toBe(true);
  });

  it("rejects invalid delta schemas and side values", () => {
    expect(isWireMessage({ type: "delta", seq: 1, side: "invalid", price: 100, qty: 10 })).toBe(false);
    expect(isWireMessage({ type: "delta", seq: 1, side: "bid", price: -10, qty: 10 })).toBe(false);
    expect(isWireMessage({ type: "delta", seq: 1, side: "bid", price: 100, qty: -1 })).toBe(false);
    expect(isWireMessage({ type: "delta", seq: 1, side: "bid", price: 100.5, qty: 10 })).toBe(false);
  });

  it("rejects numbers exceeding MAX_SAFE_INTEGER", () => {
    expect(
      isWireMessage({
        type: "delta",
        seq: 1,
        side: "bid",
        price: Number.MAX_SAFE_INTEGER + 10,
        qty: 10,
      })
    ).toBe(false);
  });

  it("validates trade messages strictly", () => {
    expect(
      isWireMessage({
        type: "trade",
        seq: 1,
        price: 100,
        qty: 10,
        taker_side: "bid",
        ts: 1_700_000_000_000,
      })
    ).toBe(true);
    expect(
      isWireMessage({
        type: "trade",
        seq: 1,
        price: 0,
        qty: 10,
        taker_side: "bid",
        ts: 1_700_000_000_000,
      })
    ).toBe(false);
  });
});

describe("PureOrderBook.ladder", () => {
  it("sorts bids descending and asks ascending", () => {
    const book = new PureOrderBook();
    book.apply(snapshot());
    const ladder = book.ladder(10);
    expect(ladder.bids.map(([p]) => p)).toEqual([100, 99]);
    expect(ladder.asks.map(([p]) => p)).toEqual([101, 102]);
  });

  it("caps depth", () => {
    const book = new PureOrderBook();
    book.apply(snapshot());
    expect(book.ladder(1).bids).toHaveLength(1);
  });
});