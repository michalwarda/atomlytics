mod event_handler;

use axum::extract::ConnectInfo;
use axum::http::HeaderMap;
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use event_handler::EventHandler;
use maxminddb::geoip2;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json;
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_rusqlite::Connection;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn, Level};
use uaparser::{Parser, UserAgentParser};

#[derive(Clone)]
struct AppState {
    db: Arc<Connection>,
    geoip: Arc<maxminddb::Reader<Vec<u8>>>,
    salt: Arc<RwLock<String>>,
    parser: Arc<UserAgentParser>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PageView {
    page_url: String,
    event_type: String,
    #[serde(skip_deserializing)]
    referrer: Option<String>,
    #[serde(skip_deserializing)]
    browser: String,
    #[serde(skip_deserializing)]
    operating_system: String,
    #[serde(skip_deserializing)]
    device_type: String,
    country: Option<String>,
    region: Option<String>,
    city: Option<String>,
    utm_source: Option<String>,
    utm_medium: Option<String>,
    utm_campaign: Option<String>,
    utm_content: Option<String>,
    utm_term: Option<String>,
    #[serde(skip_deserializing)]
    timestamp: i64,
    #[serde(default)]
    visitor_id: Option<String>,
    #[serde(default)]
    custom_params: Option<serde_json::Value>,
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

    async fn get_visitor_id(&self, ip: &str, user_agent: &str, domain: &str) -> String {
        let salt = self.salt.read().await;
        let input = format!("{}{}{}{}", *salt, domain, ip, user_agent);
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn get_current_day() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            / 86400 // seconds in a day
    }

    async fn rotate_salt_if_needed(&self) {
        let mut salt = self.salt.write().await;
        let current_day = Self::get_current_day();
        let salt_day = salt
            .split('_')
            .next()
            .unwrap_or("0")
            .parse::<i64>()
            .unwrap_or(0);

        if current_day > salt_day {
            use rand::Rng;
            let new_salt = format!(
                "{}_{}",
                current_day,
                rand::thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(16)
                    .map(char::from)
                    .collect::<String>()
            );
            *salt = new_salt;
            info!("Rotated daily salt");
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
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
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
                visitor_id TEXT NOT NULL,
                custom_params TEXT
            )",
            [],
        )
        .map_err(tokio_rusqlite::Error::from)
    })
    .await?;

    // Initialize with a random salt
    use rand::Rng;
    let initial_salt = format!(
        "{}_{}",
        AppState::get_current_day(),
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(16)
            .map(char::from)
            .collect::<String>()
    );

    // Initialize User Agent Parser
    let parser =
        UserAgentParser::from_yaml("regexes.yaml").expect("Failed to initialize user agent parser");

    let app_state = AppState {
        db: Arc::new(db),
        geoip: Arc::new(geoip),
        salt: Arc::new(RwLock::new(initial_salt)),
        parser: Arc::new(parser),
    };

    // Start salt rotation task
    let state_clone = app_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600)); // Check every hour
        loop {
            interval.tick().await;
            state_clone.rotate_salt_if_needed().await;
        }
    });

    // Create router with routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/event", post(track_event))
        .layer(TraceLayer::new_for_http())
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

async fn health_check() -> StatusCode {
    StatusCode::OK
}

async fn track_event(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(pageview): Json<PageView>,
) -> Result<StatusCode, StatusCode> {
    let handler = EventHandler::new(state);
    handler.handle_event(addr, headers, pageview).await
}
