use serde::{Deserialize, Serialize};

pub mod base_metrics_aggregator;
pub mod device_metrics_aggregator;
pub mod location_metrics_aggregator;
pub mod source_metrics_aggregator;

#[derive(Deserialize)]
pub enum TimeFrame {
    Realtime,
    Today,
    Yesterday,
    Last7Days,
    Last30Days,
    AllTime,
}

#[derive(Deserialize)]
pub enum Granularity {
    Minutes,
    Hours,
    Days,
}

#[derive(Deserialize, Clone, Copy)]
pub enum Metric {
    UniqueVisitors,
    Visits,
    Pageviews,
    AvgVisitDuration,
    BounceRate,
}

#[derive(Deserialize, Clone, Copy)]
pub enum LocationGrouping {
    Country,
    Region,
    City,
}

#[derive(Deserialize, Clone, Copy)]
pub enum DeviceGrouping {
    Browser,
    OperatingSystem,
    DeviceType,
}

#[derive(Deserialize, Clone, Copy)]
pub enum SourceGrouping {
    Source,
    Referrer,
    Campaign,
}

#[derive(Serialize)]
pub struct AggregateMetrics {
    pub unique_visitors: i64,
    pub total_visits: i64,
    pub total_pageviews: i64,
    pub current_visits: Option<i64>,
    pub avg_visit_duration: i64,
    pub bounce_rate: i64,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
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

#[derive(Serialize)]
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
