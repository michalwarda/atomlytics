use crate::AppState;
use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use serde_json;
use tracing::error;

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
    let filter_values = state.db.call(|conn| -> Result<FilterValues, tokio_rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT country, region, city, browser, operating_system, device_type, page_url_path, source, utm_campaign FROM filter_values_cache WHERE id = 1").map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?;
        let filter_values = stmt.query_row([], |row| {
            let country_json: String = row.get(0)?;
            let region_json: String = row.get(1)?;
            let city_json: String = row.get(2)?;
            let browser_json: String = row.get(3)?;
            let operating_system_json: String = row.get(4)?;
            let device_type_json: String = row.get(5)?;
            let page_url_path_json: String = row.get(6)?;
            let source_json: String = row.get(7)?;
            let utm_campaign_json: String = row.get(8)?;

            let countries: Vec<String> = serde_json::from_str(&country_json).unwrap_or_default();
            let regions: Vec<String> = serde_json::from_str(&region_json).unwrap_or_default();
            let cities: Vec<String> = serde_json::from_str(&city_json).unwrap_or_default();
            let browsers: Vec<String> = serde_json::from_str(&browser_json).unwrap_or_default();
            let operating_systems: Vec<String> = serde_json::from_str(&operating_system_json).unwrap_or_default();
            let device_types: Vec<String> = serde_json::from_str(&device_type_json).unwrap_or_default();
            let page_url_paths: Vec<String> = serde_json::from_str(&page_url_path_json).unwrap_or_default();
            let sources: Vec<String> = serde_json::from_str(&source_json).unwrap_or_default();
            let campaigns: Vec<String> = serde_json::from_str(&utm_campaign_json).unwrap_or_default();

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
        }).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?;
        Ok(filter_values)
    }).await.map_err(|e| {
        error!("Error getting filter values: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(filter_values))
}
