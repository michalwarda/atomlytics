use axum::{http::HeaderMap, http::StatusCode};
use rusqlite::params;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, instrument, warn};
use url::Url;

use crate::remote_ip::RemoteIp;
use crate::AppState;
use crate::Event;

pub struct EventHandler {
    state: AppState,
}

impl EventHandler {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    #[instrument(skip(self, headers))]
    fn extract_user_agent(&self, headers: &HeaderMap) -> String {
        headers
            .get("user-agent")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("unknown")
            .to_string()
    }

    #[instrument(skip(self, headers))]
    fn extract_referrer(&self, headers: &HeaderMap) -> Option<String> {
        headers
            .get("referer")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string())
    }

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
    async fn process_user_agent_info(&self, user_agent: &str, event: &mut Event) {
        let ua_info = self.state.parse_user_agent(user_agent);
        event.browser = ua_info.browser;
        event.operating_system = ua_info.operating_system;
        event.device_type = ua_info.device_type;
        debug!(
            browser = %event.browser,
            os = %event.operating_system,
            device = %event.device_type,
            "Processed user agent info"
        );
    }

    #[instrument(skip(self))]
    async fn process_location(&self, ip_str: &str, event: &mut Event) {
        if event.country.is_none() || event.region.is_none() || event.city.is_none() {
            let location = self.state.get_location(ip_str);
            event.country = location.country;
            event.region = location.region;
            event.city = location.city;
            debug!(
                country = ?event.country,
                region = ?event.region,
                city = ?event.city,
                "Processed location info"
            );
        }
    }

    #[instrument(skip(self, event, custom_params))]
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

        debug!(
            event_type = %event_type,
            page_url = %page_url,
            visitor_id = ?visitor_id,
            "Saving event to database"
        );

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
                .map(|_| {
                    debug!(
                        event_type = %event_type,
                        page_url = %page_url,
                        "Successfully saved event"
                    );
                    ()
                })
                .map_err(|e| {
                    warn!(error = %e, "Failed to save event");
                    tokio_rusqlite::Error::from(e)
                })
            })
            .await
    }

    async fn check_and_create_visit(
        &self,
        visitor_id: &str,
        timestamp: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        let visitor_id_clone: String = visitor_id.to_string();
        let visitor_id_for_query = visitor_id_clone.clone();
        let result = self
            .state
            .db
            .call(move |conn| {
                let mut stmt =
                    conn.prepare("SELECT MAX(timestamp) FROM events WHERE visitor_id = ?1")?;
                let last_timestamp: Option<i64> = stmt
                    .query_row(params![visitor_id_for_query], |row| row.get(0))
                    .unwrap_or(None);

                if let Some(last_ts) = last_timestamp {
                    // If last event was more than 30 minutes ago (1800 seconds)
                    if timestamp - last_ts > 1800 {
                        return Ok(true);
                    }
                } else {
                    // No previous events found for this visitor
                    return Ok(true);
                }
                Ok(false)
            })
            .await?;

        if result {
            let visit_event = Event {
                event_type: "visit".to_string(),
                page_url: "".to_string(),
                referrer: None,
                browser: "".to_string(),
                operating_system: "".to_string(),
                device_type: "".to_string(),
                country: None,
                region: None,
                city: None,
                utm_source: None,
                utm_medium: None,
                utm_campaign: None,
                utm_content: None,
                utm_term: None,
                timestamp,
                visitor_id: Some(visitor_id_clone.to_string()),
                custom_params: None,
            };

            self.save_event(&visit_event, None).await?;
            debug!(visitor_id = %visitor_id_clone, "Created new visit event");
        }

        Ok(())
    }

    #[instrument(skip(self, headers), fields(ip = %addr.ip(), event_type = %event.event_type))]
    pub async fn handle_event(
        &self,
        addr: SocketAddr,
        headers: HeaderMap,
        mut event: Event,
    ) -> Result<StatusCode, StatusCode> {
        debug!("Processing new event");

        let ip_str = RemoteIp::get(&headers, &addr);
        let user_agent = self.extract_user_agent(&headers);
        event.referrer = self.extract_referrer(&headers);

        self.process_user_agent_info(&user_agent, &mut event).await;

        let domain = self.extract_domain(&event.page_url);
        let visitor_id = self
            .state
            .get_visitor_id(&ip_str, &user_agent, &domain)
            .await;
        event.visitor_id = Some(visitor_id.clone());

        debug!(visitor_id = %visitor_id, "Generated visitor ID");

        self.process_location(&ip_str, &mut event).await;

        event.timestamp = self.get_current_timestamp();

        // Check and create visit event if needed
        if let Err(e) = self
            .check_and_create_visit(&visitor_id, event.timestamp)
            .await
        {
            warn!(error = %e, "Failed to check and create visit event");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }

        let custom_params = self.serialize_custom_params(&event.custom_params);

        match self.save_event(&event, custom_params).await {
            Ok(_) => {
                debug!("Successfully processed event");
                Ok(StatusCode::CREATED)
            }
            Err(e) => {
                warn!(error = %e, "Failed to process event");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}
