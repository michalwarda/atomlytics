use axum::response::IntoResponse;

pub async fn serve_script() -> impl IntoResponse {
    const SCRIPT: &str = include_str!("../script.js");

    axum::response::Response::builder()
        .header("Content-Type", "application/javascript")
        .header("Cache-Control", "max-age=3600")
        .body(SCRIPT.to_string())
        .unwrap()
} 