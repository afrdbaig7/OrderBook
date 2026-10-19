import { useEffect, useRef, useState } from "react";

import { isWireMessage, type WireMessage } from "../lib/protocol";
import {
  DEFAULT_DEPTH,
  EMPTY_SNAPSHOT,
  MarketStore,
  type BookSnapshot,
} from "../lib/store";

export type ConnectionStatus = "connecting" | "live" | "error";

export interface OrderBookState {
  snapshot: BookSnapshot;
  status: ConnectionStatus;
  messagesPerSecond: number;
  tradesPerSecond: number;
}

/**
 * Subscribes to the `/ws` market-data stream.
 *
 * Networking and book mutation happen **outside** React, in a
 * [`MarketStore`] held in a ref: incoming messages never trigger a render.
 * A single `requestAnimationFrame` loop publishes a fresh snapshot to React at
 * screen refresh rate, so a high-frequency stream costs at most one render per
 * frame. Sequence-gap detection lives in the store; on a gap
 * the socket is closed and a fresh snapshot is requested automatically.
 */
export function useOrderBook(
  url: string,
  depth: number = DEFAULT_DEPTH,
): { state: OrderBookState; store: MarketStore } {
  const storeRef = useRef<MarketStore | null>(null);
  if (storeRef.current === null) {
    storeRef.current = new MarketStore();
  }
  const store = storeRef.current;

  const [state, setState] = useState<OrderBookState>({
    snapshot: EMPTY_SNAPSHOT,
    status: "connecting",
    messagesPerSecond: 0,
    tradesPerSecond: 0,
  });

  useEffect(() => {
    let socket: WebSocket | null = null;
    let retryTimer: number | undefined;
    let rafId: number | undefined;
    let disposed = false;
    let sawSnapshot = false;

    // Rate accounting over a rolling ~1 s window, computed on frame publish.
    let windowStart = performance.now();
    let windowMessages = store.messages;
    let windowTrades = store.trades;
    let rates = { messagesPerSecond: 0, tradesPerSecond: 0 };

    const publish = () => {
      rafId = undefined;
      const now = performance.now();
      if (now - windowStart >= 1000) {
        const seconds = (now - windowStart) / 1000;
        rates = {
          messagesPerSecond: Math.round((store.messages - windowMessages) / seconds),
          tradesPerSecond: Math.round((store.trades - windowTrades) / seconds),
        };
        windowStart = now;
        windowMessages = store.messages;
        windowTrades = store.trades;
      }
      setState({
        snapshot: store.render(depth),
        status: sawSnapshot ? "live" : "connecting",
        ...rates,
      });
    };

    const schedule = () => {
      if (rafId === undefined && !disposed) {
        rafId = requestAnimationFrame(publish);
      }
    };

    const connect = () => {
      if (disposed) {
        return;
      }
      setState((current) => ({ ...current, status: "connecting" }));
      socket = new WebSocket(url);

      socket.onmessage = (event) => {
        let message: WireMessage;
        try {
          const value: unknown = JSON.parse(event.data as string);
          if (!isWireMessage(value)) {
            return;
          }
          message = value;
        } catch {
          return; // ignore malformed frames
        }

        store.apply(message);

        if (store.book.desynced) {
          // A gap makes the stream unusable; drop it and resync.
          setState((current) => ({ ...current, status: "error" }));
          socket?.close();
          return;
        }

        if (!sawSnapshot && store.book.synced) {
          sawSnapshot = true;
        }
        schedule();
      };

      socket.onerror = () => {
        socket?.close();
      };

      socket.onclose = () => {
        if (disposed) {
          return;
        }
        setState((current) => ({ ...current, status: "error" }));
        retryTimer = window.setTimeout(connect, 750);
      };
    };

    connect();

    return () => {
      disposed = true;
      if (retryTimer !== undefined) {
        window.clearTimeout(retryTimer);
      }
      if (rafId !== undefined) {
        cancelAnimationFrame(rafId);
      }
      socket?.close();
    };
  }, [url, depth, store]);

  return { state, store };
}
