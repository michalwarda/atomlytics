use serde::{Deserialize, Serialize};

pub mod base_metrics_aggregator;
pub mod device_metrics_aggregator;
pub mod location_metrics_aggregator;
pub mod page_metrics_aggregator;
pub mod source_metrics_aggregator;

#[derive(Deserialize, Debug)]
pub enum TimeFrame {
    Realtime,
    Today,
    Yesterday,
    Last7Days,
    Last30Days,
    AllTime,
}

#[derive(Deserialize, Debug)]
pub enum Granularity {
    Minutes,
    Hours,
    Days,
}

#[derive(Deserialize, Clone, Copy, Debug)]
pub enum Metric {
    UniqueVisitors,
    Visits,
    Pageviews,
    AvgVisitDuration,
    BounceRate,
}

#[derive(Deserialize, Clone, Copy, Debug)]
pub enum PageGrouping {
    Page,
    EntryPage,
    ExitPage,
}

#[derive(Deserialize, Clone, Copy, Debug)]
pub enum LocationGrouping {
    Country,
    Region,
    City,
}

#[derive(Deserialize, Clone, Copy, Debug)]
pub enum DeviceGrouping {
    Browser,
    OperatingSystem,
    DeviceType,
}

#[derive(Deserialize, Clone, Copy, Debug)]
pub enum SourceGrouping {
    Source,
    Referrer,
    Campaign,
}

#[derive(Serialize, Debug)]
pub struct AggregateMetrics {
    pub unique_visitors: i64,
    pub total_visits: i64,
    pub total_pageviews: i64,
    pub current_visits: Option<i64>,
    pub avg_visit_duration: i64,
    pub bounce_rate: i64,
}

#[derive(Serialize, Debug)]
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

#[derive(Serialize, Debug)]
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

#[derive(Serialize, Debug)]
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

#[derive(Serialize, Debug)]
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
