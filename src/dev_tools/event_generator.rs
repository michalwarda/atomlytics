use axum::http::HeaderMap;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use serde_json::json;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

use crate::{handlers::event::Event, AppState};

const SAMPLE_PAGES: &[&str] = &[
    "/",
    "/products",
    "/about",
    "/contact",
    "/blog",
    "/pricing",
    "/features",
];

const SAMPLE_BROWSERS: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
];

const SAMPLE_REFERRERS: &[&str] = &[
    "https://google.com",
    "https://facebook.com",
    "https://twitter.com",
    "https://linkedin.com",
    "", // direct traffic
];

const SAMPLE_IPS: &[&str] = &[
    "8.8.8.8",      // USA (Google DNS)
    "178.79.163.0", // UK
    "35.158.210.0", // Germany
    "52.69.0.0",    // Japan
    "13.54.0.0",    // Australia
    "35.182.0.0",   // Canada
    "35.198.0.0",   // Brazil
    "35.206.0.0",   // India
    "41.79.0.0",    // South Africa
    "156.146.0.0",  // France
];

pub struct EventGenerator {
    state: AppState,
    base_url: String,
}

impl EventGenerator {
    pub fn new(state: AppState, base_url: String) -> Self {
        Self { state, base_url }
    }

    fn generate_random_event(&self) -> (Event, String) {
        let mut rng = StdRng::from_entropy();
        let page = SAMPLE_PAGES.choose(&mut rng).unwrap();
        let page_url = format!("{}{}", self.base_url, page);
        let ip = *SAMPLE_IPS.choose(&mut rng).unwrap();

        let event = Event {
            page_url,
            event_type: "pageview".to_string(),
            custom_params: Some(json!({
                "dev_generated": true,
                "timestamp": chrono::Utc::now().timestamp(),
                "test_ip": ip,
            })),
            referrer: Some(SAMPLE_REFERRERS.choose(&mut rng).unwrap().to_string()),
            source: None,
            browser: String::new(),
            operating_system: String::new(),
            device_type: String::new(),
            country: None,
            region: None,
            city: None,
            utm_source: None,
            utm_medium: None,
            utm_campaign: None,
            utm_content: None,
            utm_term: None,
            timestamp: 0,
            visitor_id: None,
            visit_id: None,
            page_url_path: None,
        };

        (event, ip.to_string())
    }

    pub async fn start_generation(&self) {
        info!("Starting synthetic event generation for development...");

        let mut rng = StdRng::from_entropy();

        loop {
            // Generate a random event and IP
            let (event, ip) = self.generate_random_event();
            let addr: SocketAddr = format!("{}:12345", ip).parse().unwrap();

            // Create headers with random user agent
            let mut headers = HeaderMap::new();
            headers.insert(
                "user-agent",
                SAMPLE_BROWSERS.choose(&mut rng).unwrap().parse().unwrap(),
            );
            // Add X-Forwarded-For header to ensure IP is properly detected
            headers.insert("x-forwarded-for", ip.parse().unwrap());

            // Send the event through the normal event handling pipeline
            let handler = crate::handlers::event::EventHandler::new(self.state.clone());
            if let Err(e) = handler.handle_event(addr, headers, event).await {
                info!("Failed to process synthetic event: {:?}", e);
            }

            // Random delay between 2 and 10 seconds
            let delay = rng.gen_range(2..10);
            sleep(Duration::from_secs(delay)).await;
        }
    }
}
