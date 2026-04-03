use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct EventPayload {
    #[serde(rename = "u")]
    pub page_url: String,
    #[serde(rename = "n")]
    pub event_type: String,
    #[serde(rename = "p", default)]
    pub custom_params: Option<serde_json::Value>,
    #[serde(rename = "r", default)]
    pub referrer: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnrichedEvent {
    pub payload: EventPayload,
    pub domain: String,
    pub page_url_path: String,
    pub source: String,
    pub browser: String,
    pub operating_system: String,
    pub device_type: String,
    pub country: String,
    pub region: String,
    pub city: String,
    pub utm_source: Option<String>,
    pub utm_medium: Option<String>,
    pub utm_campaign: Option<String>,
    pub utm_content: Option<String>,
    pub utm_term: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub visitor_id: String,
    pub visit_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
pub enum TimeFrame {
    Realtime,
    Today,
    Yesterday,
    Last7Days,
    Last30Days,
    AllTime,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
pub enum Granularity {
    Minutes,
    Hours,
    Days,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
pub enum Metric {
    UniqueVisitors,
    Visits,
    Pageviews,
    AvgVisitDuration,
    BounceRate,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
pub enum LocationGrouping {
    Country,
    Region,
    City,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
pub enum DeviceGrouping {
    Browser,
    OperatingSystem,
    DeviceType,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
pub enum SourceGrouping {
    Source,
    Referrer,
    Campaign,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
pub enum PageGrouping {
    Page,
    EntryPage,
    ExitPage,
}

#[derive(Debug, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[serde(default)]
pub struct DashboardQuery {
    pub timeframe: TimeFrame,
    pub granularity: Granularity,
    pub metric: Metric,
    pub location_grouping: LocationGrouping,
    pub device_grouping: DeviceGrouping,
    pub source_grouping: SourceGrouping,
    pub page_grouping: PageGrouping,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_country: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_country_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_region: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_region_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_city: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_city_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_browser: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_browser_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_os: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_os_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_device: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_device_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_page: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_page_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_source: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_source_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_referrer: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_referrer_not: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_campaign: Vec<String>,
    #[serde(deserialize_with = "deserialize_filter_values")]
    pub filter_campaign_not: Vec<String>,
}

impl Default for DashboardQuery {
    fn default() -> Self {
        Self {
            timeframe: TimeFrame::Today,
            granularity: Granularity::Hours,
            metric: Metric::UniqueVisitors,
            location_grouping: LocationGrouping::Country,
            device_grouping: DeviceGrouping::Browser,
            source_grouping: SourceGrouping::Source,
            page_grouping: PageGrouping::Page,
            filter_country: Vec::new(),
            filter_country_not: Vec::new(),
            filter_region: Vec::new(),
            filter_region_not: Vec::new(),
            filter_city: Vec::new(),
            filter_city_not: Vec::new(),
            filter_browser: Vec::new(),
            filter_browser_not: Vec::new(),
            filter_os: Vec::new(),
            filter_os_not: Vec::new(),
            filter_device: Vec::new(),
            filter_device_not: Vec::new(),
            filter_page: Vec::new(),
            filter_page_not: Vec::new(),
            filter_source: Vec::new(),
            filter_source_not: Vec::new(),
            filter_referrer: Vec::new(),
            filter_referrer_not: Vec::new(),
            filter_campaign: Vec::new(),
            filter_campaign_not: Vec::new(),
        }
    }
}

fn deserialize_filter_values<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    match Option::<OneOrMany>::deserialize(deserializer)? {
        Some(OneOrMany::One(value)) => Ok(vec![value]),
        Some(OneOrMany::Many(values)) => Ok(values),
        None => Ok(Vec::new()),
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AggregateMetrics {
    pub unique_visitors: i64,
    pub total_visits: i64,
    pub total_pageviews: i64,
    pub current_visits: Option<i64>,
    pub avg_visit_duration: i64,
    pub bounce_rate: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LocationMetrics {
    pub country: String,
    pub region: Option<String>,
    pub city: Option<String>,
    pub visitors: i64,
    pub visits: i64,
    pub pageviews: i64,
    pub avg_visit_duration: i64,
    pub bounce_rate: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DeviceMetrics {
    pub browser: String,
    pub operating_system: String,
    pub device_type: String,
    pub visitors: i64,
    pub visits: i64,
    pub pageviews: i64,
    pub avg_visit_duration: i64,
    pub bounce_rate: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SourceMetrics {
    pub source: String,
    pub referrer: Option<String>,
    pub utm_source: Option<String>,
    pub utm_medium: Option<String>,
    pub utm_campaign: Option<String>,
    pub visitors: i64,
    pub visits: i64,
    pub pageviews: i64,
    pub avg_visit_duration: i64,
    pub bounce_rate: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PageMetrics {
    pub page_path: String,
    pub entry_page_path: String,
    pub exit_page_path: String,
    pub visitors: i64,
    pub visits: i64,
    pub pageviews: i64,
    pub avg_visit_duration: i64,
    pub bounce_rate: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DashboardResponse {
    pub stats: Vec<(i64, i64)>,
    pub aggregates: AggregateMetrics,
    pub realtime_aggregates: AggregateMetrics,
    pub location_metrics: Vec<LocationMetrics>,
    pub device_metrics: Vec<DeviceMetrics>,
    pub source_metrics: Vec<SourceMetrics>,
    pub page_metrics: Vec<PageMetrics>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FilterValuesResponse {
    pub country: Vec<String>,
    pub region: Vec<String>,
    pub city: Vec<String>,
    pub browser: Vec<String>,
    pub operating_system: Vec<String>,
    pub device_type: Vec<String>,
    pub page_url_path: Vec<String>,
    pub source: Vec<String>,
    pub referrer: Vec<String>,
    pub utm_campaign: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DomainSummary {
    pub domain: String,
    pub total_visits: i64,
    pub visits_last_24h: i64,
    pub last_seen_at: Option<i64>,
}
