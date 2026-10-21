//! WebSocket handler: snapshot bootstrap then live delta streaming.
//!
//! On connect: subscribe to the broadcast channel *first* (so no delta can
//! race past the snapshot), request a consistent snapshot from the engine
//! thread, send it, then stream live messages skipping those already covered
//! by the snapshot. Lagging clients are closed so they reconnect for a fresh
//! snapshot rather than silently receiving a corrupted book.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message as Ws, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info, warn};

use crate::engine_thread::{EngineCommand, SnapshotReply};
use crate::metrics::Metrics;
use crate::state::AppState;

/// `GET /ws` — upgrades the connection and starts streaming.
pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| run(socket, state))
}

/// Decrements the client gauge even if the task unwinds/returns early.
struct ClientGuard {
    metrics: Arc<Metrics>,
}

impl ClientGuard {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.client_connected();
        Self { metrics }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.metrics.client_disconnected();
    }
}

async fn run(mut socket: WebSocket, state: AppState) {
    let _guard = ClientGuard::new(state.metrics.clone());
    let mut shutdown = state.shutdown.clone();
    let send_timeout = Duration::from_secs(5);

    // 1. Subscribe first so the snapshot cannot race ahead of live deltas.
    let mut rx = state.broadcast.subscribe();

    // 2. Ask the engine thread for a complete bootstrap snapshot (depth 0 = full book within capacity).
    // Client-side display slicing manages presentation depth.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<SnapshotReply>();
    let command = EngineCommand::Snapshot {
        depth: 0,
        respond: reply_tx,
    };
    if !send_with_backoff(&state, command, &mut shutdown).await {
        warn!("engine channel full or shutting down; closing socket before snapshot");
        let _ = tokio::time::timeout(send_timeout, socket.send(Ws::Close(None))).await;
        return;
    }

    let SnapshotReply { seq, json } = tokio::select! {
        _ = shutdown.changed() => {
            if *shutdown.borrow() {
                info!("ws: shutdown signaled before snapshot; closed socket");
                let _ = tokio::time::timeout(send_timeout, socket.send(Ws::Close(None))).await;
            }
            return;
        }
        res = tokio::time::timeout(send_timeout, reply_rx) => match res {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => {
                warn!("engine dropped snapshot request; closing socket");
                return;
            }
            Err(_) => {
                warn!("engine snapshot request timed out; closing socket");
                return;
            }
        }
    };
    let mut last_seq = seq;

    let send_snapshot = tokio::select! {
        _ = shutdown.changed() => {
            let _ = tokio::time::timeout(send_timeout, socket.send(Ws::Close(None))).await;
            return;
        }
        res = tokio::time::timeout(send_timeout, socket.send(Ws::Text(json.to_string()))) => res,
    };
    if send_snapshot.is_err() || send_snapshot.unwrap().is_err() {
        return;
    }
    info!(last_seq, "ws: snapshot sent");

    // 3. Stream live messages.
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    let _ = tokio::time::timeout(send_timeout, socket.send(Ws::Close(None))).await;
                    info!("ws: shutdown signaled; closed socket");
                    return;
                }
            }
            received = rx.recv() => match received {
                Ok(event) => {
                    if event.seq <= last_seq {
                        // Already covered by the snapshot.
                        continue;
                    }
                    if tokio::time::timeout(send_timeout, socket.send(Ws::Text(event.json.to_string())))
                        .await
                        .map_err(|_| ())
                        .and_then(|r| r.map_err(|_| ()))
                        .is_err()
                    {
                        warn!("ws: send failed or timed out; closing socket");
                        return;
                    }
                    last_seq = event.seq;
                }
                Err(RecvError::Lagged(skipped)) => {
                    warn!(skipped, "ws: client lagged; closing so it can resync");
                    let _ = tokio::time::timeout(send_timeout, socket.send(Ws::Close(None))).await;
                    return;
                }
                Err(RecvError::Closed) => {
                    debug!("ws: broadcast channel closed");
                    return;
                }
            },
            inbound = socket.recv() => match inbound {
                None => return,
                Some(Ok(Ws::Close(_))) => return,
                Some(Ok(Ws::Ping(payload))) => {
                    let _ = tokio::time::timeout(send_timeout, socket.send(Ws::Pong(payload))).await;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) => return,
            },
        }
    }
}

/// Sends a command to the engine, retrying briefly if the channel is full.
/// Short backoff avoids blocking the async runtime on a synchronous channel,
/// and yields or exits if shutdown is signaled.
async fn send_with_backoff(
    state: &AppState,
    mut command: EngineCommand,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    for attempt in 0..10u32 {
        if *shutdown.borrow() {
            return false;
        }
        match state.cmd_tx.try_send(command) {
            Ok(()) => return true,
            Err(crossbeam_channel::TrySendError::Full(returned)) => {
                command = returned;
                if attempt < 9 {
                    tokio::select! {
                        _ = shutdown.changed() => return false,
                        _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                    }
                }
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => return false,
        }
    }
    false
}
