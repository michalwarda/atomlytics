use crate::AppState;
use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use std::sync::Arc;
use tokio_rusqlite::Connection;

#[derive(Serialize)]
pub struct FilterValues {
    pub country: Vec<String>,
    pub region: Vec<String>,
    pub city: Vec<String>,
    pub browser: Vec<String>,
    pub operating_system: Vec<String>,
    pub device_type: Vec<String>,
    pub page_url_path: Vec<String>,
    pub source: Vec<String>,
    pub utm_campaign: Vec<String>,
}

pub async fn get_filter_values(
    State(state): State<AppState>,
) -> Result<Json<FilterValues>, StatusCode> {
    let filter_values = state.db.call(|conn| {
        // Query distinct values for each filter
        let mut stmt = conn.prepare("SELECT DISTINCT country FROM visits WHERE country IS NOT NULL AND country <> ''")?;
        let countries = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT region FROM visits WHERE region IS NOT NULL AND region <> ''")?;
        let regions = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT city FROM visits WHERE city IS NOT NULL AND city <> ''")?;
        let cities = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT browser FROM visits WHERE browser IS NOT NULL AND browser <> ''")?;
        let browsers = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT operating_system FROM visits WHERE operating_system IS NOT NULL AND operating_system <> ''")?;
        let operating_systems = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT device_type FROM visits WHERE device_type IS NOT NULL AND device_type <> ''")?;
        let device_types = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT page_url_path FROM visits WHERE page_url_path IS NOT NULL AND page_url_path <> ''")?;
        let page_url_paths = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT source FROM visits WHERE source IS NOT NULL AND source <> ''")?;
        let sources = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        let mut stmt = conn.prepare("SELECT DISTINCT utm_campaign FROM visits WHERE utm_campaign IS NOT NULL AND utm_campaign <> ''")?;
        let campaigns = stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        Ok(FilterValues {
            country: countries,
            region: regions,
            city: cities,
            browser: browsers,
            operating_system: operating_systems,
            device_type: device_types,
            page_url_path: page_url_paths,
            source: sources,
            utm_campaign: campaigns,
        })
    }).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(filter_values))
}
