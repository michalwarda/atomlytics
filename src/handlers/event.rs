use crate::AppState;
use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, instrument, warn};
use url::Url;

use crate::remote_ip::RemoteIp;

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "u")]
    pub page_url: String,
    #[serde(rename = "n")]
    pub event_type: String,
    #[serde(rename = "p", default)]
    pub custom_params: Option<serde_json::Value>,
    #[serde(rename = "r", default)]
    pub referrer: Option<String>,
    #[serde(skip_deserializing)]
    pub source: Option<String>,
    #[serde(skip_deserializing)]
    pub browser: String,
    #[serde(skip_deserializing)]
    pub operating_system: String,
    #[serde(skip_deserializing)]
    pub device_type: String,
    #[serde(skip_deserializing)]
    pub country: Option<String>,
    #[serde(skip_deserializing)]
    pub region: Option<String>,
    #[serde(skip_deserializing)]
    pub city: Option<String>,
    #[serde(skip_deserializing)]
    pub utm_source: Option<String>,
    #[serde(skip_deserializing)]
    pub utm_medium: Option<String>,
    #[serde(skip_deserializing)]
    pub utm_campaign: Option<String>,
    #[serde(skip_deserializing)]
    pub utm_content: Option<String>,
    #[serde(skip_deserializing)]
    pub utm_term: Option<String>,
    #[serde(skip_deserializing)]
    pub timestamp: i64,
    #[serde(skip_deserializing)]
    pub visitor_id: Option<String>,
    #[serde(skip_deserializing)]
    pub is_active: i64,
    #[serde(skip_deserializing)]
    pub last_activity_at: i64,
    #[serde(skip_deserializing)]
    pub last_visited_url: Option<String>,
    #[serde(skip_deserializing)]
    pub page_url_path: Option<String>,
    #[serde(skip_deserializing)]
    pub last_visited_url_path: Option<String>,
}

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
    fn calculate_source(&self, referrer: &Option<String>, utm_source: &Option<String>) -> String {
        if let Some(utm) = utm_source {
            if !utm.is_empty() {
                return utm.clone();
            }
        }

        if let Some(ref_url) = referrer {
            if let Ok(url) = Url::parse(ref_url) {
                if let Some(host) = url.host_str() {
                    // Extract domain and remove www. if present
                    let domain = if host.starts_with("www.") {
                        host[4..].to_string()
                    } else {
                        host.to_string()
                    };

                    // Special cases for common sources
                    return match domain.as_str() {
                        "google.com" | "google.co.uk" | "google.fr" => "Google".to_string(),
                        "facebook.com" => "Facebook".to_string(),
                        "twitter.com" => "Twitter".to_string(),
                        "linkedin.com" | "lnkd.in" => "LinkedIn".to_string(),
                        "instagram.com" => "Instagram".to_string(),
                        "t.co" => "Twitter".to_string(),
                        "bing.com" => "Bing".to_string(),
                        "yahoo.com" => "Yahoo".to_string(),
                        _ => domain,
                    };
                }
            }
        }

        "Direct".to_string()
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

    #[instrument(skip(self))]
    fn extract_path(&self, url: &str) -> Option<String> {
        Url::parse(url).ok().and_then(|u| {
            let path = u.path().to_string();
            if path.is_empty() || path == "/" {
                Some("/".to_string())
            } else {
                Some(path)
            }
        })
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
        let source = event.source.clone();
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
        let is_active = event.is_active;
        let last_activity_at = event.last_activity_at;
        let last_visited_url = event.last_visited_url.clone();
        let page_url_path = event.page_url_path.clone();
        let last_visited_url_path = event.last_visited_url_path.clone();
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
                        event_type, page_url, referrer, source, browser, operating_system, 
                        device_type, country, region, city,
                        utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                        timestamp, visitor_id, custom_params, is_active, last_activity_at, last_visited_url,
                        page_url_path, last_visited_url_path
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21,
                        ?22, ?23
                    )",
                    params![
                        &event_type,
                        &page_url,
                        &referrer,
                        &source,
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
                        &is_active,
                        &last_activity_at,
                        &last_visited_url,
                        &page_url_path,
                        &last_visited_url_path,
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
        event: &Event,
    ) -> Result<(), tokio_rusqlite::Error> {
        let visitor_id_clone: String = visitor_id.to_string();
        let visitor_id_for_query = visitor_id_clone.clone();
        let timestamp = event.timestamp;
        let page_url = event.page_url.clone();
        let result = self
            .state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, timestamp FROM events 
                     WHERE visitor_id = ?1 AND event_type = 'visit'
                     ORDER BY timestamp DESC LIMIT 1",
                )?;

                let result = stmt.query_row(params![visitor_id_for_query], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                });

                match result {
                    Ok((visit_id, last_ts)) => {
                        if timestamp - last_ts <= 1800 {
                            println!("Visit exists and is active, updating last_activity_at");
                            // Visit exists and is active, update last_activity_at and last_visited_url
                            conn.execute(
                                "UPDATE events SET last_activity_at = ?1, last_visited_url = ?2 WHERE id = ?3",
                                params![timestamp, page_url, visit_id],
                            )?;
                            Ok(false)
                        } else {
                            println!("Visit exists but expired");
                            // Visit exists but expired
                            Ok(true)
                        }
                    }
                    Err(_) => {
                        // No visit found
                        println!("No visit found");
                        Ok(true)
                    }
                }
            })
            .await?;

        if result {
            let visit_event = Event {
                event_type: "visit".to_string(),
                page_url: event.page_url.clone(),
                referrer: event.referrer.clone(),
                browser: event.browser.clone(),
                operating_system: event.operating_system.clone(),
                device_type: event.device_type.clone(),
                country: event.country.clone(),
                region: event.region.clone(),
                city: event.city.clone(),
                source: event.source.clone(),
                utm_source: event.utm_source.clone(),
                utm_medium: event.utm_medium.clone(),
                utm_campaign: event.utm_campaign.clone(),
                utm_content: event.utm_content.clone(),
                utm_term: event.utm_term.clone(),
                timestamp: event.timestamp,
                visitor_id: Some(visitor_id_clone.to_string()),
                custom_params: event.custom_params.clone(),
                is_active: 1,
                last_activity_at: event.timestamp,
                last_visited_url: Some(event.page_url.clone()),
                page_url_path: event.page_url_path.clone(),
                last_visited_url_path: event.page_url_path.clone(),
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

        // Extract path from page_url
        event.page_url_path = self.extract_path(&event.page_url);

        // Only set referrer from headers if it's not already set from client
        if event.referrer.is_none() {
            event.referrer = self.extract_referrer(&headers);
        }

        // Calculate source before saving the event
        let source = self.calculate_source(&event.referrer, &event.utm_source);
        event.source = Some(source);

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
        if let Err(e) = self.check_and_create_visit(&visitor_id, &event).await {
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

pub async fn track_event(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(event): Json<Event>,
) -> Result<StatusCode, StatusCode> {
    let handler = EventHandler::new(state);
    handler.handle_event(addr, headers, event).await
}
