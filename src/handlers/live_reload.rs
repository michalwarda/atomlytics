use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::broadcast::{self, Sender};
use tracing::info;

pub struct LiveReload {
    reload_notifier: Sender<()>,
}

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

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<crate::AppState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state.live_reload))
}

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
