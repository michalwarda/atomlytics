use axum::response::IntoResponse;

pub async fn serve_dashboard() -> impl IntoResponse {
    const DASHBOARD_HTML: &str = include_str!("../dashboard.html");

    axum::response::Response::builder()
        .header("Content-Type", "text/html")
        .body(DASHBOARD_HTML.to_string())
        .unwrap()
} 