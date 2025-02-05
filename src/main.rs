mod event_handler;
mod handlers;
mod migrations;
mod remote_ip;

use axum::http::HeaderMap;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::{
    http::StatusCode,
    routing::{get, post},
    Router,
};
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use event_handler::EventHandler;
use handlers::*;
use maxminddb::geoip2;
use rusqlite::params;
use sha2::{Digest, Sha256};
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_rusqlite::Connection;
use tower_http::cors::CorsLayer;
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::{error, info, warn, Level, Span};
use uaparser::{Parser, UserAgentParser};

#[derive(Clone)]
struct AppState {
    db: Arc<Connection>,
    geoip: Arc<maxminddb::Reader<Vec<u8>>>,
    parser: Arc<UserAgentParser>,
}

#[derive(Debug)]
struct GeoLocation {
    country: Option<String>,
    region: Option<String>,
    city: Option<String>,
}

#[derive(Debug)]
struct UserAgentInfo {
    browser: String,
    operating_system: String,
    device_type: String,
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

    fn parse_user_agent(&self, user_agent: &str) -> UserAgentInfo {
        let ua = self.parser.parse(user_agent);

        // Determine device type based on device brand and model
        let device_type = if ua.device.family.to_lowercase().contains("mobile")
            || ua.device.family.to_lowercase().contains("phone")
            || ua.device.family.to_lowercase().contains("android")
            || ua.device.family.to_lowercase().contains("iphone")
        {
            "Mobile"
        } else if ua.device.family.to_lowercase().contains("tablet")
            || ua.device.family.to_lowercase().contains("ipad")
        {
            "Tablet"
        } else if ua.device.family.to_lowercase().contains("bot")
            || ua.device.family.to_lowercase().contains("crawler")
            || ua.device.family.to_lowercase().contains("spider")
        {
            "Bot"
        } else {
            "Desktop"
        };

        UserAgentInfo {
            browser: format!(
                "{} {}",
                ua.user_agent.family,
                ua.user_agent.major.unwrap_or_default()
            ),
            operating_system: format!("{} {}", ua.os.family, ua.os.major.unwrap_or_default()),
            device_type: device_type.to_string(),
        }
    }

    fn get_current_day() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            / 86400 // seconds in a day
    }

    async fn get_visitor_id(&self, ip: &str, user_agent: &str, domain: &str) -> String {
        let current_day = Self::get_current_day();
        let salt = self
            .db
            .call(move |conn| {
                conn.query_row(
                    "SELECT value FROM salt WHERE day = ?",
                    [current_day],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|e| e.into())
            })
            .await
            .unwrap_or_else(|_| "default_salt".to_string());

        let input = format!("{}{}{}{}", salt, domain, ip, user_agent);
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    async fn rotate_salt_if_needed(&self) {
        let current_day = Self::get_current_day();

        let needs_rotation = self
            .db
            .call(move |conn| {
                let count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM salt WHERE day = ?",
                    [current_day],
                    |row| row.get(0),
                )?;
                Ok::<_, tokio_rusqlite::Error>(count == 0)
            })
            .await
            .unwrap_or(true);

        if needs_rotation {
            use rand::Rng;
            let new_salt = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(16)
                .map(char::from)
                .collect::<String>();

            let current_day = current_day;
            let new_salt = new_salt;

            if let Err(e) = self
                .db
                .call(move |conn| {
                    Ok(conn.execute(
                        "INSERT INTO salt (day, value) VALUES (?, ?)",
                        params![current_day, new_salt],
                    ))
                })
                .await
            {
                error!("Failed to rotate salt: {}", e);
            } else {
                info!("Rotated daily salt");
            }

            // Clean up old salts (keep only yesterday and today)
            if let Err(e) = self
                .db
                .call(move |conn| {
                    Ok(conn.execute("DELETE FROM salt WHERE day < ?", params![current_day - 1]))
                })
                .await
            {
                error!("Failed to clean up old salts: {}", e);
            }
        }
    }
}

async fn basic_auth(
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
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
            .body(axum::body::Body::empty())
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
            .body(axum::body::Body::empty())
            .unwrap())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let env = std::env::var("RUST_LOG").unwrap_or("info".to_string());
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .with_max_level(env.parse().unwrap_or(Level::INFO))
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
    let db = Connection::open("db/analytics.db").await?;

    // Initialize database and run migrations
    migrations::initialize_database(&db).await?;

    // Initialize User Agent Parser
    let parser =
        UserAgentParser::from_yaml("regexes.yaml").expect("Failed to initialize user agent parser");

    let app_state = AppState {
        db: Arc::new(db),
        geoip: Arc::new(geoip),
        parser: Arc::new(parser),
    };

    // Ensure we have an initial salt
    app_state.rotate_salt_if_needed().await;

    // Start statistics aggregation task
    let db_clone = app_state.db.clone();
    tokio::spawn(async move {
        let aggregator = StatisticsAggregator::new(db_clone);
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(e) = aggregator.aggregate_statistics().await {
                error!("Failed to aggregate statistics: {}", e);
            }
        }
    });

    // Start active statistics aggregation task
    let db_clone = app_state.db.clone();
    tokio::spawn(async move {
        let aggregator = StatisticsAggregator::new(db_clone);
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            if let Err(e) = aggregator.aggregate_active_statistics().await {
                error!("Failed to aggregate active statistics: {}", e);
            }
        }
    });

    // Create router with routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/event", post(track_event))
        .route("/script.js", get(serve_script))
        .nest(
            "/",
            Router::new()
                .route("/dashboard", get(serve_dashboard))
                .route("/api/statistics", get(get_statistics))
                .layer(middleware::from_fn(basic_auth)),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<axum::body::Body>| {
                    let method = request.method();
                    let uri = request.uri();
                    let user_agent = request
                        .headers()
                        .get("user-agent")
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("unknown");

                    tracing::info_span!(
                        "request",
                        method = %method,
                        uri = %uri,
                        user_agent = %user_agent,
                    )
                })
                .on_request(|_request: &axum::http::Request<_>, _span: &Span| {
                    info!("Started processing request");
                })
                .on_response(
                    |response: &axum::http::Response<_>,
                     latency: std::time::Duration,
                     _span: &Span| {
                        info!(
                            status = %response.status(),
                            latency = %latency.as_secs_f64(),
                            "Finished processing request"
                        );
                    },
                )
                .on_failure(DefaultOnFailure::new().level(Level::WARN)),
        )
        .layer(CorsLayer::permissive())
        .with_state(app_state);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Server listening on http://{}", addr);

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
