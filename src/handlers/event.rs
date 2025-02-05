use crate::{AppState, EventHandler};
use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use std::net::SocketAddr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "u")]
    pub page_url: String,
    #[serde(rename = "n")]
    pub event_type: String,
    #[serde(rename = "r", default)]
    pub custom_params: Option<serde_json::Value>,
    #[serde(skip_deserializing)]
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