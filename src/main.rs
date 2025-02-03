use axum::extract::ConnectInfo;
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use maxminddb::geoip2;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn, Level};

#[derive(Clone)]
struct AppState {
    db: Arc<Connection>,
    geoip: Arc<maxminddb::Reader<Vec<u8>>>,
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
    ip_address: Option<String>,
}

#[derive(Debug)]
struct GeoLocation {
    country: Option<String>,
    region: Option<String>,
    city: Option<String>,
}

impl AppState {
    fn get_location(&self, ip_str: &str) -> GeoLocation {
        match ip_str.parse() {
            Ok(ip) => match self.geoip.lookup::<geoip2::City>(ip) {
                Ok(city) => GeoLocation {
                    country: city
                        .country
                        .and_then(|c| c.names)
                        .and_then(|n| n.get("en").cloned())
                        .map(|s| s.to_string()),
                    region: city
                        .subdivisions
                        .and_then(|s| s.get(0).cloned())
                        .and_then(|sd| sd.names)
                        .and_then(|n| n.get("en").cloned())
                        .map(|s| s.to_string()),
                    city: city
                        .city
                        .and_then(|c| c.names)
                        .and_then(|n| n.get("en").cloned())
                        .map(|s| s.to_string()),
                },
                Err(e) => {
                    warn!("Failed to lookup IP location: {} {}", ip_str, e);
                    GeoLocation {
                        country: None,
                        region: None,
                        city: None,
                    }
                }
            },
            Err(e) => {
                warn!("Failed to parse IP address: {}", e);
                GeoLocation {
                    country: None,
                    region: None,
                    city: None,
                }
            }
        }
    }
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

    // Initialize MaxMind database
    let geoip_path = Path::new("GeoLite2-City.mmdb");
    if !geoip_path.exists() {
        println!("Please download the GeoLite2-City.mmdb file from MaxMind and place it in the project root");
        println!(
            "You can get it from: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data"
        );
        std::process::exit(1);
    }

    let geoip = maxminddb::Reader::open_readfile(geoip_path)?;

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
                ip_address TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )
        .map_err(tokio_rusqlite::Error::from)
    })
    .await?;

    let app_state = AppState {
        db: Arc::new(db),
        geoip: Arc::new(geoip),
    };

    // Create router with routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/collect", post(track_pageview))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(app_state);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Server listening on http://{}", addr);

    // Use into_make_service_with_connect_info to include the client's IP address
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

async fn track_pageview(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(mut pageview): Json<PageView>,
) -> Result<StatusCode, StatusCode> {
    // Get IP address from the connection
    let ip_str = addr.ip().to_string();
    pageview.ip_address = Some(ip_str.clone());

    // Get location from IP if not provided
    if pageview.country.is_none() || pageview.region.is_none() || pageview.city.is_none() {
        let location = state.get_location(&ip_str);
        pageview.country = location.country;
        pageview.region = location.region;
        pageview.city = location.city;
    }

    state
        .db
        .call(move |conn| {
            conn.execute(
                "INSERT INTO page_views (
                    page_url, referrer, browser, operating_system, 
                    device_type, country, region, city,
                    utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                    timestamp, ip_address
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
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
                    &pageview.ip_address,
                ),
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::CREATED)
}
