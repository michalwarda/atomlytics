use crate::AppState;
use axum::http::StatusCode;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, instrument, warn};

#[derive(Debug, Serialize, Deserialize)]
pub struct Visit {
    pub id: Option<i64>,
    pub visitor_id: String,
    pub page_url: String,
    pub page_url_path: Option<String>,
    pub referrer: Option<String>,
    pub source: Option<String>,
    pub browser: String,
    pub operating_system: String,
    pub device_type: String,
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,
    pub utm_source: Option<String>,
    pub utm_medium: Option<String>,
    pub utm_campaign: Option<String>,
    pub utm_content: Option<String>,
    pub utm_term: Option<String>,
    pub timestamp: i64,
    pub is_active: i64,
    pub last_activity_at: i64,
    pub last_visited_url: Option<String>,
    pub last_visited_url_path: Option<String>,
}

pub struct VisitHandler {
    state: AppState,
}

impl VisitHandler {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    fn get_current_timestamp(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[instrument(skip(self))]
    pub async fn create_visit(&self, visit: &Visit) -> Result<i64, tokio_rusqlite::Error> {
        let visit_clone = Visit {
            id: visit.id,
            visitor_id: visit.visitor_id.clone(),
            page_url: visit.page_url.clone(),
            page_url_path: visit.page_url_path.clone(),
            referrer: visit.referrer.clone(),
            source: visit.source.clone(),
            browser: visit.browser.clone(),
            operating_system: visit.operating_system.clone(),
            device_type: visit.device_type.clone(),
            country: visit.country.clone(),
            region: visit.region.clone(),
            city: visit.city.clone(),
            utm_source: visit.utm_source.clone(),
            utm_medium: visit.utm_medium.clone(),
            utm_campaign: visit.utm_campaign.clone(),
            utm_content: visit.utm_content.clone(),
            utm_term: visit.utm_term.clone(),
            timestamp: visit.timestamp,
            is_active: visit.is_active,
            last_activity_at: visit.last_activity_at,
            last_visited_url: visit.last_visited_url.clone(),
            last_visited_url_path: visit.last_visited_url_path.clone(),
        };

        self.state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare_cached(
                    "INSERT INTO visits (
                        visitor_id, page_url, page_url_path, referrer, source,
                        browser, operating_system, device_type, country, region,
                        city, utm_source, utm_medium, utm_campaign, utm_content,
                        utm_term, timestamp, is_active, last_activity_at,
                        last_visited_url, last_visited_url_path
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                        ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19,
                        ?20, ?21
                    ) RETURNING id",
                )?;

                let result = stmt.query_row(
                    params![
                        &visit_clone.visitor_id,
                        &visit_clone.page_url,
                        &visit_clone.page_url_path,
                        &visit_clone.referrer,
                        &visit_clone.source,
                        &visit_clone.browser,
                        &visit_clone.operating_system,
                        &visit_clone.device_type,
                        &visit_clone.country,
                        &visit_clone.region,
                        &visit_clone.city,
                        &visit_clone.utm_source,
                        &visit_clone.utm_medium,
                        &visit_clone.utm_campaign,
                        &visit_clone.utm_content,
                        &visit_clone.utm_term,
                        &visit_clone.timestamp,
                        &visit_clone.is_active,
                        &visit_clone.last_activity_at,
                        &visit_clone.last_visited_url,
                        &visit_clone.last_visited_url_path,
                    ],
                    |row| row.get(0),
                );

                match result {
                    Ok(visit) => Ok(visit),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Err(tokio_rusqlite::Error::from(
                        rusqlite::Error::QueryReturnedNoRows,
                    )),
                    Err(e) => Err(tokio_rusqlite::Error::from(e)),
                }
            })
            .await
    }

    #[instrument(skip(self))]
    pub async fn get_active_visit(
        &self,
        visitor_id: &str,
        current_timestamp: i64,
    ) -> Result<Option<Visit>, tokio_rusqlite::Error> {
        let visitor_id = visitor_id.to_string();
        self.state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare_cached(
                    "SELECT 
                        id, visitor_id, page_url, page_url_path, referrer, source,
                        browser, operating_system, device_type, country, region,
                        city, utm_source, utm_medium, utm_campaign, utm_content,
                        utm_term, timestamp, is_active, last_activity_at,
                        last_visited_url, last_visited_url_path
                    FROM visits 
                    WHERE visitor_id = ?1 
                    AND timestamp >= ?2 - 1800
                    AND is_active = 1
                    ORDER BY timestamp DESC 
                    LIMIT 1",
                )?;

                let result = stmt.query_row(params![visitor_id, current_timestamp], |row| {
                    Ok(Visit {
                        id: row.get(0)?,
                        visitor_id: row.get(1)?,
                        page_url: row.get(2)?,
                        page_url_path: row.get(3)?,
                        referrer: row.get(4)?,
                        source: row.get(5)?,
                        browser: row.get(6)?,
                        operating_system: row.get(7)?,
                        device_type: row.get(8)?,
                        country: row.get(9)?,
                        region: row.get(10)?,
                        city: row.get(11)?,
                        utm_source: row.get(12)?,
                        utm_medium: row.get(13)?,
                        utm_campaign: row.get(14)?,
                        utm_content: row.get(15)?,
                        utm_term: row.get(16)?,
                        timestamp: row.get(17)?,
                        is_active: row.get(18)?,
                        last_activity_at: row.get(19)?,
                        last_visited_url: row.get(20)?,
                        last_visited_url_path: row.get(21)?,
                    })
                });

                match result {
                    Ok(visit) => Ok(Some(visit)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(tokio_rusqlite::Error::from(e)),
                }
            })
            .await
    }

    #[instrument(skip(self))]
    pub async fn update_visit_activity(
        &self,
        visit_id: i64,
        current_timestamp: i64,
        last_visited_url: &str,
        last_visited_url_path: Option<String>,
    ) -> Result<(), tokio_rusqlite::Error> {
        let last_visited_url = last_visited_url.to_string();
        let last_visited_url_path = last_visited_url_path;

        self.state
            .db
            .call(move |conn| {
                conn.execute(
                    "UPDATE visits 
                    SET last_activity_at = ?, 
                        last_visited_url = ?,
                        last_visited_url_path = ?
                    WHERE id = ?",
                    params![
                        current_timestamp,
                        last_visited_url,
                        last_visited_url_path,
                        visit_id
                    ],
                )
                .map(|_| ())
                .map_err(Into::into)
            })
            .await
    }
}
