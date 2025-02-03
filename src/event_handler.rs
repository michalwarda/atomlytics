use axum::{http::HeaderMap, http::StatusCode};
use rusqlite::params;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

use crate::AppState;
use crate::Event;

pub struct EventHandler {
    state: AppState,
}

impl EventHandler {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    fn extract_user_agent(&self, headers: &HeaderMap) -> String {
        headers
            .get("user-agent")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("unknown")
            .to_string()
    }

    fn extract_referrer(&self, headers: &HeaderMap) -> Option<String> {
        headers
            .get("referer")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string())
    }

    fn extract_domain(&self, page_url: &str) -> String {
        Url::parse(page_url)
            .map(|u| u.host_str().unwrap_or("unknown").to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    }

    fn get_current_timestamp(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn serialize_custom_params(&self, custom_params: &Option<serde_json::Value>) -> Option<String> {
        custom_params
            .as_ref()
            .map(|p| serde_json::to_string(p).unwrap_or_default())
    }

    async fn process_user_agent_info(&self, user_agent: &str, event: &mut Event) {
        let ua_info = self.state.parse_user_agent(user_agent);
        event.browser = ua_info.browser;
        event.operating_system = ua_info.operating_system;
        event.device_type = ua_info.device_type;
    }

    async fn process_location(&self, ip_str: &str, event: &mut Event) {
        if event.country.is_none() || event.region.is_none() || event.city.is_none() {
            let location = self.state.get_location(ip_str);
            event.country = location.country;
            event.region = location.region;
            event.city = location.city;
        }
    }

    async fn save_event(
        &self,
        event: &Event,
        custom_params: Option<String>,
    ) -> Result<(), tokio_rusqlite::Error> {
        let event_type = event.event_type.clone();
        let page_url = event.page_url.clone();
        let referrer = event.referrer.clone();
        let browser = event.browser.clone();
        let operating_system = event.operating_system.clone();
        let device_type = event.device_type.clone();
        let country = event.country.clone();
        let region = event.region.clone();
        let city = event.city.clone();
        let utm_source = event.utm_source.clone();
        let utm_medium = event.utm_medium.clone();
        let utm_campaign = event.utm_campaign.clone();
        let utm_content = event.utm_content.clone();
        let utm_term = event.utm_term.clone();
        let timestamp = event.timestamp;
        let visitor_id = event.visitor_id.clone();
        let custom_params = custom_params;

        self.state
            .db
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO events (
                        event_type, page_url, referrer, browser, operating_system, 
                        device_type, country, region, city,
                        utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                        timestamp, visitor_id, custom_params
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17
                    )",
                    params![
                        &event_type,
                        &page_url,
                        &referrer,
                        &browser,
                        &operating_system,
                        &device_type,
                        &country,
                        &region,
                        &city,
                        &utm_source,
                        &utm_medium,
                        &utm_campaign,
                        &utm_content,
                        &utm_term,
                        timestamp,
                        &visitor_id,
                        &custom_params,
                    ],
                )
                .map(|_| ())
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
    }

    pub async fn handle_event(
        &self,
        addr: SocketAddr,
        headers: HeaderMap,
        mut event: Event,
    ) -> Result<StatusCode, StatusCode> {
        // Extract basic information
        let ip_str = addr.ip().to_string();
        let user_agent = self.extract_user_agent(&headers);
        event.referrer = self.extract_referrer(&headers);

        // Process user agent information
        self.process_user_agent_info(&user_agent, &mut event).await;

        // Extract domain and generate visitor ID
        let domain = self.extract_domain(&event.page_url);
        let visitor_id = self
            .state
            .get_visitor_id(&ip_str, &user_agent, &domain)
            .await;
        event.visitor_id = Some(visitor_id);

        // Process location information
        self.process_location(&ip_str, &mut event).await;

        // Set timestamp
        event.timestamp = self.get_current_timestamp();

        // Prepare custom parameters
        let custom_params = self.serialize_custom_params(&event.custom_params);

        // Save event to database
        self.save_event(&event, custom_params)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(StatusCode::CREATED)
    }
}
