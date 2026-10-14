//! WebSocket handler: snapshot-then-deltas, with lag handling (spec §5, §28).
//!
//! # Handshake
//!
//! 1. Subscribe to the broadcast channel **before** requesting the snapshot,
//!    so no delta published after the snapshot can be missed.
//! 2. Send a `snapshot` request to the engine thread and wait for the reply.
//! 3. Send the snapshot, then stream live messages, dropping any with
//!    `seq <= snapshot.seq` (those are duplicates already reflected in the
//!    snapshot).
//!
//! # Backpressure
//!
//! A Tokio `broadcast` receiver that falls behind by more than its capacity
//! yields `RecvError::Lagged`. Rather than silently skipping deltas (which
//! would corrupt the client's book), we log the gap and **close the socket**.
//! A well-behaved client reconnects and gets a fresh snapshot — the same
//! recovery rule the `feed` crate's `ClientBook` implements.

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

    // 1. Subscribe first so the snapshot cannot race ahead of live deltas.
    let mut rx = state.broadcast.subscribe();

    // 2. Ask the engine thread for a consistent snapshot.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<SnapshotReply>();
    let command = EngineCommand::Snapshot {
        depth: state.snapshot_depth,
        respond: reply_tx,
    };
    if !send_with_backoff(&state, command).await {
        warn!("engine channel full; closing socket before snapshot");
        return;
    }

    let SnapshotReply { seq, json } = match reply_rx.await {
        Ok(reply) => reply,
        Err(_) => {
            warn!("engine dropped snapshot request; closing socket");
            return;
        }
    };
    let mut last_seq = seq;

    if socket.send(Ws::Text(json.to_string())).await.is_err() {
        return;
    }
    info!(last_seq, "ws: snapshot sent");

    // 3. Stream live messages.
    loop {
        tokio::select! {
            received = rx.recv() => match received {
                Ok(event) => {
                    if event.seq <= last_seq {
                        // Already covered by the snapshot.
                        continue;
                    }
                    if socket.send(Ws::Text(event.json.to_string())).await.is_err() {
                        return;
                    }
                    last_seq = event.seq;
                }
                Err(RecvError::Lagged(skipped)) => {
                    warn!(skipped, "ws: client lagged; closing so it can resync");
                    let _ = socket.send(Ws::Close(None)).await;
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
                    let _ = socket.send(Ws::Pong(payload)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) => return,
            },
        }
    }
}

/// Sends a command to the engine, retrying briefly if the channel is full.
///
/// Snapshot requests are rare; a short backoff avoids ever blocking the
/// async runtime on a synchronous channel. `try_send` returns the command
/// when the channel is full, so we re-own it and retry.
async fn send_with_backoff(state: &AppState, mut command: EngineCommand) -> bool {
    for attempt in 0..10u32 {
        match state.cmd_tx.try_send(command) {
            Ok(()) => return true,
            Err(crossbeam_channel::TrySendError::Full(returned)) => {
                command = returned;
                if attempt < 9 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => return false,
        }
    }
    false
}
