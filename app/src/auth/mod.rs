use axum::{
    body::Body,
    http::{header, HeaderMap, Request, Response, StatusCode},
    middleware::Next,
};
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::AppState;

pub async fn basic_auth(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    request: Request<Body>,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    let unauthorized = || {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(
                header::WWW_AUTHENTICATE,
                "Basic realm=\"Atomlytics Dashboard\"",
            )
            .body(Body::empty())
            .unwrap()
    };

    let auth_header = match headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok())
    {
        Some(value) if value.starts_with("Basic ") => value,
        _ => return Ok(unauthorized()),
    };

    let encoded = auth_header.trim_start_matches("Basic ").trim();
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let credentials = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let mut parts = credentials.splitn(2, ':');
    let username = parts.next().unwrap_or_default();
    let password = parts.next().unwrap_or_default();

    if username == state.config.dashboard_username && password == state.config.dashboard_password {
        Ok(next.run(request).await)
    } else {
        Ok(unauthorized())
    }
}
