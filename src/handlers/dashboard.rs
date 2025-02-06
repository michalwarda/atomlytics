use axum::response::IntoResponse;

const DASHBOARD_HTML: &str = include_str!("../dashboard.html");

#[cfg(debug_assertions)]
const LIVE_RELOAD_SCRIPT: &str = r#"
    <script>
        // Live reload script for development
        if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') {
            const ws = new WebSocket(`ws://${window.location.host}/live-reload`);
            ws.onmessage = () => window.location.reload();
            ws.onclose = () => {
                console.log('Live reload connection closed. Attempting to reconnect...');
                setInterval(() => {
                    const newWs = new WebSocket(`ws://${window.location.host}/live-reload`);
                    newWs.onopen = () => window.location.reload();
                }, 1000);
            };
        }
    </script>
"#;

#[cfg(not(debug_assertions))]
const LIVE_RELOAD_SCRIPT: &str = "";

pub async fn serve_dashboard() -> impl IntoResponse {
    let html = DASHBOARD_HTML.replace("<!-- LIVE_RELOAD_SCRIPT -->", LIVE_RELOAD_SCRIPT);

    axum::response::Response::builder()
        .header("Content-Type", "text/html")
        .body(html)
        .unwrap()
}
