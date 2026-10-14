/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// Dev proxy: the browser talks to relative paths, Vite forwards them to the
// Rust server (HTTP + WebSocket).
const API = "http://127.0.0.1:8080";
const WS = "ws://127.0.0.1:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/ws": { target: WS, ws: true },
      "/health": { target: API },
      "/candles": { target: API },
      "/feed-rate": { target: API },
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});