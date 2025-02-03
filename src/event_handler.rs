use axum::{http::HeaderMap, http::StatusCode};
use rusqlite::params;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

use crate::AppState;
use crate::PageView;

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

    async fn process_user_agent_info(&self, user_agent: &str, pageview: &mut PageView) {
        let ua_info = self.state.parse_user_agent(user_agent);
        pageview.browser = ua_info.browser;
        pageview.operating_system = ua_info.operating_system;
        pageview.device_type = ua_info.device_type;
    }

    async fn process_location(&self, ip_str: &str, pageview: &mut PageView) {
        if pageview.country.is_none() || pageview.region.is_none() || pageview.city.is_none() {
            let location = self.state.get_location(ip_str);
            pageview.country = location.country;
            pageview.region = location.region;
            pageview.city = location.city;
        }
    }

    async fn save_event(
        &self,
        pageview: &PageView,
        custom_params: Option<String>,
    ) -> Result<(), tokio_rusqlite::Error> {
        let event_type = pageview.event_type.clone();
        let page_url = pageview.page_url.clone();
        let referrer = pageview.referrer.clone();
        let browser = pageview.browser.clone();
        let operating_system = pageview.operating_system.clone();
        let device_type = pageview.device_type.clone();
        let country = pageview.country.clone();
        let region = pageview.region.clone();
        let city = pageview.city.clone();
        let utm_source = pageview.utm_source.clone();
        let utm_medium = pageview.utm_medium.clone();
        let utm_campaign = pageview.utm_campaign.clone();
        let utm_content = pageview.utm_content.clone();
        let utm_term = pageview.utm_term.clone();
        let timestamp = pageview.timestamp;
        let visitor_id = pageview.visitor_id.clone();
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
        mut pageview: PageView,
    ) -> Result<StatusCode, StatusCode> {
        // Extract basic information
        let ip_str = addr.ip().to_string();
        let user_agent = self.extract_user_agent(&headers);
        pageview.referrer = self.extract_referrer(&headers);

        // Process user agent information
        self.process_user_agent_info(&user_agent, &mut pageview)
            .await;

        // Extract domain and generate visitor ID
        let domain = self.extract_domain(&pageview.page_url);
        let visitor_id = self
            .state
            .get_visitor_id(&ip_str, &user_agent, &domain)
            .await;
        pageview.visitor_id = Some(visitor_id);

        // Process location information
        self.process_location(&ip_str, &mut pageview).await;

        // Set timestamp
        pageview.timestamp = self.get_current_timestamp();

        // Prepare custom parameters
        let custom_params = self.serialize_custom_params(&pageview.custom_params);

        // Save event to database
        self.save_event(&pageview, custom_params)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(StatusCode::CREATED)
    }
}
