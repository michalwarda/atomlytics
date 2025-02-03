use axum::extract::ConnectInfo;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, Level};

#[derive(Clone)]
struct AppState {
    db: Arc<Connection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PageView {
    page_url: String,
    referrer: Option<String>,
    browser: String,
    operating_system: String,
    device_type: String,
    country: Option<String>,
    region: Option<String>,
    city: Option<String>,
    utm_source: Option<String>,
    utm_medium: Option<String>,
    utm_campaign: Option<String>,
    utm_content: Option<String>,
    utm_term: Option<String>,
    timestamp: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .with_max_level(Level::INFO)
        .init();

    info!("Starting Atomlytics server...");

    // Initialize SQLite database
    let db = Connection::open("analytics.db").await?;

    // Create tables if they don't exist
    db.call(|conn| {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS page_views (
                id INTEGER PRIMARY KEY,
                page_url TEXT NOT NULL,
                referrer TEXT,
                browser TEXT NOT NULL,
                operating_system TEXT NOT NULL,
                device_type TEXT NOT NULL,
                country TEXT,
                region TEXT,
                city TEXT,
                utm_source TEXT,
                utm_medium TEXT,
                utm_campaign TEXT,
                utm_content TEXT,
                utm_term TEXT,
                timestamp INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )
        .map_err(tokio_rusqlite::Error::from)
    })
    .await?;

    let app_state = AppState { db: Arc::new(db) };

    // Create router with routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/collect", post(track_pageview))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive()) // Allow CORS for browser tracking
        .with_state(app_state);

    // Start server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("Server listening on http://0.0.0.0:3000");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

async fn track_pageview(
    State(state): State<AppState>,
    Json(pageview): Json<PageView>,
) -> Result<StatusCode, StatusCode> {
    state
        .db
        .call(move |conn| {
            conn.execute(
                "INSERT INTO page_views (
                    page_url, referrer, browser, operating_system, 
                    device_type, country, region, city,
                    utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                    timestamp
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
                )",
                (
                    &pageview.page_url,
                    &pageview.referrer,
                    &pageview.browser,
                    &pageview.operating_system,
                    &pageview.device_type,
                    &pageview.country,
                    &pageview.region,
                    &pageview.city,
                    &pageview.utm_source,
                    &pageview.utm_medium,
                    &pageview.utm_campaign,
                    &pageview.utm_content,
                    &pageview.utm_term,
                    pageview.timestamp,
                ),
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::CREATED)
}
