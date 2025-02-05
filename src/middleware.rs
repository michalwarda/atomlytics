use axum::{
    body::Body,
    http::{HeaderMap, Request, Response, StatusCode},
    middleware::Next,
};
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use std::env;

pub async fn basic_auth(
    headers: HeaderMap,
    request: Request<Body>,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .and_then(|header| header.to_str().ok());

    // If no auth header is present or it's invalid, return 401 with WWW-Authenticate header
    if auth_header.is_none() || !auth_header.unwrap().starts_with("Basic ") {
        return Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(
                "WWW-Authenticate",
                "Basic realm=\"Please enter your credentials\"",
            )
            .body(Body::empty())
            .unwrap());
    }

    let credentials = auth_header.unwrap()["Basic ".len()..].trim().to_string();

    let decoded = base64
        .decode(credentials)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let credentials = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let mut parts = credentials.splitn(2, ':');
    let username = parts.next().unwrap_or("");
    let password = parts.next().unwrap_or("");

    let expected_username = env::var("DASHBOARD_USERNAME").unwrap_or_else(|_| "admin".to_string());
    let expected_password = env::var("DASHBOARD_PASSWORD").unwrap_or_else(|_| "admin".to_string());

    if username == expected_username && password == expected_password {
        Ok(next.run(request).await)
    } else {
        // Return 401 with WWW-Authenticate header for invalid credentials
        Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(
                "WWW-Authenticate",
                "Basic realm=\"Please enter your credentials\"",
            )
            .body(Body::empty())
            .unwrap())
    }
}
