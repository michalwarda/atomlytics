use axum::{
    extract::{ws::WebSocketUpgrade, State},
    response::Response,
};

#[cfg(debug_assertions)]
use axum::extract::ws::{Message, WebSocket};
#[cfg(debug_assertions)]
use futures_util::{SinkExt, StreamExt};
#[cfg(debug_assertions)]
use std::sync::Arc;
#[cfg(debug_assertions)]
use tokio::sync::broadcast::{self, Sender};
#[cfg(debug_assertions)]
use tracing::info;

#[cfg(debug_assertions)]
pub struct LiveReload {
    reload_notifier: Sender<()>,
}

#[cfg(debug_assertions)]
impl LiveReload {
    pub fn new() -> Self {
        let (reload_notifier, _) = broadcast::channel(1);
        Self { reload_notifier }
    }

    pub fn get_sender(&self) -> Sender<()> {
        self.reload_notifier.clone()
    }

    pub async fn notify_reload(&self) {
        let _ = self.reload_notifier.send(());
    }
}

#[cfg(debug_assertions)]
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<crate::AppState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state.live_reload))
}

#[cfg(not(debug_assertions))]
pub async fn ws_handler(_: WebSocketUpgrade, _: State<crate::AppState>) -> Response {
    axum::response::Response::builder()
        .status(axum::http::StatusCode::NOT_FOUND)
        .body(axum::body::Body::empty())
        .unwrap()
}

#[cfg(debug_assertions)]
async fn handle_socket(socket: WebSocket, live_reload: Arc<LiveReload>) {
    let mut rx = live_reload.reload_notifier.subscribe();

    info!("New live reload connection established");

    let (mut sender, _receiver) = socket.split();

    while rx.recv().await.is_ok() {
        if let Err(_) = sender.send(Message::Text("reload".to_string())).await {
            break;
        }
    }

    info!("Live reload connection closed");
}
