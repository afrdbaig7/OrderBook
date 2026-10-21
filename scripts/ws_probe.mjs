// scripts/ws_probe.mjs — Lightweight WebSocket probe for smoke tests.
// Runs with vanilla Node.js (v20+ has global WebSocket built-in).

const url = process.argv[2] || 'ws://127.0.0.1:15173/ws';
const targetCount = parseInt(process.argv[3] || '25', 10);

console.log(`[ws_probe] Connecting to ${url}...`);

const ws = new WebSocket(url);
let received = 0;
let lastSeq = -1;
let receivedSnapshot = false;

const timeout = setTimeout(() => {
  console.error('[ws_probe] Timeout waiting for WebSocket messages');
  ws.close();
  process.exit(1);
}, 10000);

ws.onopen = () => {
  console.log('[ws_probe] Connected successfully');
};

ws.onmessage = (event) => {
  try {
    const data = JSON.parse(event.data);
    received++;

    if (!receivedSnapshot) {
      if (data.type !== 'snapshot') {
        console.error(`[ws_probe] Expected first frame to be 'snapshot', got '${data.type}'`);
        process.exit(1);
      }
      if (typeof data.seq !== 'number' || !Array.isArray(data.bids) || !Array.isArray(data.asks)) {
        console.error('[ws_probe] Malformed snapshot frame:', data);
        process.exit(1);
      }
      receivedSnapshot = true;
      lastSeq = data.seq;
      console.log(`[ws_probe] Received bootstrap snapshot: seq=${data.seq}, bids=${data.bids.length}, asks=${data.asks.length}`);
    } else {
      if (typeof data.seq === 'number') {
        if (data.seq <= lastSeq) {
          // Event already covered by snapshot or out of order
          console.error(`[ws_probe] Non-monotonic sequence: got ${data.seq}, last was ${lastSeq}`);
          process.exit(1);
        }
        if (data.seq !== lastSeq + 1) {
          console.error(`[ws_probe] Sequence gap detected: expected ${lastSeq + 1}, got ${data.seq}`);
          process.exit(1);
        }
        lastSeq = data.seq;
      }
    }

    if (received >= targetCount) {
      clearTimeout(timeout);
      console.log(`[ws_probe] Successfully verified ${received} contiguous messages (latest seq: ${lastSeq})`);
      ws.close();
      process.exit(0);
    }
  } catch (err) {
    console.error('[ws_probe] Error processing message:', err);
    process.exit(1);
  }
};

ws.onerror = (err) => {
  console.error('[ws_probe] WebSocket error:', err);
  process.exit(1);
};
