use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use chrono::{Timelike, Utc};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::error;

use crate::aggregators::{
    AggregateMetrics, DeviceGrouping, DeviceMetrics, Granularity, LocationGrouping,
    LocationMetrics, Metric, PageGrouping, PageMetrics, SourceGrouping, SourceMetrics, TimeFrame,
};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct FilteredStatisticsQuery {
    pub timeframe: TimeFrame,
    pub granularity: Granularity,
    pub metric: Metric,
    pub location_grouping: LocationGrouping,
    pub device_grouping: DeviceGrouping,
    pub source_grouping: SourceGrouping,
    pub page_grouping: PageGrouping,
    pub filter_country: Option<String>,
    pub filter_region: Option<String>,
    pub filter_city: Option<String>,
    pub filter_browser: Option<String>,
    pub filter_os: Option<String>,
    pub filter_device: Option<String>,
    pub filter_page: Option<String>,
    pub filter_source: Option<String>,
    pub filter_campaign: Option<String>,
}

#[derive(Serialize)]
pub struct FilteredStatistics {
    stats: Vec<(i64, i64)>,
    aggregates: AggregateMetrics,
    realtime_aggregates: AggregateMetrics,
    location_metrics: Vec<LocationMetrics>,
    device_metrics: Vec<DeviceMetrics>,
    source_metrics: Vec<SourceMetrics>,
    page_metrics: Vec<PageMetrics>,
}

pub struct FilteredStatisticsHandler {
    db: Arc<Connection>,
}

impl FilteredStatisticsHandler {
    pub fn new(state: AppState) -> Self {
        Self { db: state.db }
    }

    pub async fn get_filtered_statistics(
        &self,
        query: FilteredStatisticsQuery,
    ) -> Result<FilteredStatistics, StatusCode> {
        let now = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_start_ts = today_start.and_utc().timestamp();
        let today_end = today_start + chrono::Duration::days(1);
        let today_end_ts = today_end.and_utc().timestamp();

        let (start_ts, end_ts) = match query.timeframe {
            TimeFrame::Realtime => (now.timestamp() - 30 * 60, now.timestamp()),
            TimeFrame::Today => (today_start_ts, today_end_ts),
            TimeFrame::Yesterday => (today_start_ts - 86400, today_start_ts),
            TimeFrame::Last7Days => (today_start_ts - 7 * 86400, today_end_ts),
            TimeFrame::Last30Days => (today_start_ts - 30 * 86400, today_end_ts),
            TimeFrame::AllTime => (0, now.timestamp()),
        };

        let interval = match query.granularity {
            Granularity::Minutes => 60,
            Granularity::Hours => 3600,
            Granularity::Days => 86400,
        };

        let (filter_condition_str, filter_params) = self.build_filter_conditions(&query);

        let stats = self
            .get_time_series_data(
                start_ts,
                end_ts,
                interval,
                &query.metric,
                filter_condition_str.clone(),
                filter_params.clone(),
            )
            .await?;
        let aggregates = self
            .calculate_aggregates(
                start_ts,
                end_ts,
                filter_condition_str.clone(),
                filter_params.clone(),
            )
            .await?;
        let realtime_aggregates = self
            .calculate_realtime_aggregates(filter_condition_str.clone(), filter_params.clone())
            .await?;
        let location_metrics = self
            .get_location_metrics(
                start_ts,
                end_ts,
                &query.metric,
                query.location_grouping,
                filter_condition_str.clone(),
                filter_params.clone(),
            )
            .await?;
        let device_metrics = self
            .get_device_metrics(
                start_ts,
                end_ts,
                &query.metric,
                query.device_grouping,
                filter_condition_str.clone(),
                filter_params.clone(),
            )
            .await?;
        let source_metrics = self
            .get_source_metrics(
                start_ts,
                end_ts,
                &query.metric,
                query.source_grouping,
                filter_condition_str.clone(),
                filter_params.clone(),
            )
            .await?;
        let page_metrics = self
            .get_page_metrics(
                start_ts,
                end_ts,
                &query.metric,
                query.page_grouping,
                filter_condition_str,
                filter_params,
            )
            .await?;
        Ok(FilteredStatistics {
            stats,
            aggregates,
            realtime_aggregates,
            location_metrics,
            device_metrics,
            source_metrics,
            page_metrics,
        })
    }

    fn build_filter_conditions(&self, query: &FilteredStatisticsQuery) -> (String, Vec<String>) {
        let mut conditions = Vec::new();
        let mut params = Vec::new();
        if let Some(country) = &query.filter_country {
            conditions.push("v.country = ?".to_string());
            params.push(country.clone());
        }
        if let Some(region) = &query.filter_region {
            conditions.push("v.region = ?".to_string());
            params.push(region.clone());
        }
        if let Some(city) = &query.filter_city {
            conditions.push("v.city = ?".to_string());
            params.push(city.clone());
        }
        if let Some(browser) = &query.filter_browser {
            conditions.push("v.browser = ?".to_string());
            params.push(browser.clone());
        }
        if let Some(os) = &query.filter_os {
            conditions.push("v.operating_system = ?".to_string());
            params.push(os.clone());
        }
        if let Some(device) = &query.filter_device {
            conditions.push("v.device_type = ?".to_string());
            params.push(device.clone());
        }
        if let Some(page) = &query.filter_page {
            conditions.push("v.page_url_path = ?".to_string());
            params.push(page.clone());
        }
        if let Some(source) = &query.filter_source {
            conditions.push("v.source = ?".to_string());
            params.push(source.clone());
        }
        if let Some(campaign) = &query.filter_campaign {
            conditions.push("v.utm_campaign = ?".to_string());
            params.push(campaign.clone());
        }

        if conditions.is_empty() {
            (String::new(), params)
        } else {
            (format!("AND {}", conditions.join(" AND ")), params)
        }
    }

    async fn get_time_series_data(
        &self,
        start_ts: i64,
        end_ts: i64,
        interval: i64,
        metric: &Metric,
        filter_condition_str: String,
        filter_params: Vec<String>,
    ) -> Result<Vec<(i64, i64)>, StatusCode> {
        let metric_expr = match metric {
            Metric::UniqueVisitors => "COUNT(DISTINCT e.visitor_id)",
            Metric::Visits => "COUNT(DISTINCT v.id)",
            Metric::Pageviews => "COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id ELSE NULL END)",
            Metric::AvgVisitDuration => "CAST(AVG(CASE WHEN v.id IS NOT NULL AND v.last_activity_at > v.timestamp THEN COALESCE(v.last_activity_at - v.timestamp, 0) ELSE 0 END) AS INTEGER)",
            Metric::BounceRate => "COALESCE(CAST(CAST(COUNT(DISTINCT CASE WHEN v.id IS NOT NULL AND v.timestamp = v.last_activity_at THEN v.id ELSE NULL END) AS FLOAT) / NULLIF(CAST(COUNT(DISTINCT v.id) AS FLOAT), 0) * 100.0 AS INTEGER), 0)",
        };

        let query = format!(
            "SELECT 
                (e.timestamp / ?) * ? as period_start,
                {}
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.timestamp >= ? AND e.timestamp <= ? {}
            GROUP BY period_start
            ORDER BY period_start ASC",
            metric_expr, filter_condition_str
        );

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut params_vec: Vec<&(dyn rusqlite::types::ToSql)> = Vec::new();
                params_vec.push(&interval);
                params_vec.push(&interval);
                params_vec.push(&start_ts);
                params_vec.push(&end_ts);
                for p in &filter_params {
                    params_vec.push(p);
                }
                let rows =
                    stmt.query_map(params_vec.as_slice(), |row| Ok((row.get(0)?, row.get(1)?)))?;

                let mut data_map = std::collections::HashMap::new();
                let mut lower_bound = None;
                for row in rows {
                    let (timestamp, value) = row?;
                    if lower_bound.is_none() {
                        lower_bound = Some(timestamp);
                    }
                    data_map.insert(timestamp, value);
                }

                let mut complete_series = Vec::new();
                let mut normalized_start_ts = start_ts;
                if start_ts == 0 {
                    normalized_start_ts = lower_bound.unwrap_or(start_ts);
                }
                let mut current_ts = normalized_start_ts - (normalized_start_ts % interval);
                while current_ts <= end_ts {
                    let value = data_map.get(&current_ts).copied().unwrap_or(0);
                    complete_series.push((current_ts, value));
                    current_ts += interval;
                }

                Ok(complete_series)
            })
            .await
            .map_err(|e| {
                error!("error: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn calculate_aggregates(
        &self,
        start_ts: i64,
        end_ts: i64,
        filter_condition_str: String,
        filter_params: Vec<String>,
    ) -> Result<AggregateMetrics, StatusCode> {
        let query = format!(
            "SELECT 
                COUNT(DISTINCT e.visitor_id) as unique_visitors,
                COUNT(DISTINCT v.id) as total_visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id ELSE NULL END) as total_pageviews,
                CAST(AVG(CASE 
                    WHEN v.id IS NOT NULL AND v.last_activity_at > v.timestamp 
                    THEN COALESCE(v.last_activity_at - v.timestamp, 0)
                    ELSE 0 
                END) AS INTEGER) as avg_visit_duration,
                CAST(CAST(COUNT(DISTINCT CASE WHEN v.id IS NOT NULL AND v.timestamp = v.last_activity_at THEN v.id ELSE NULL END) AS FLOAT) / CAST(NULLIF(COUNT(DISTINCT v.id), 0) AS FLOAT) * 100.0 AS INTEGER) as bounce_rate
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.timestamp >= ? AND e.timestamp <= ? {}",
            filter_condition_str
        );

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut params_vec: Vec<&(dyn rusqlite::types::ToSql)> = Vec::new();
                params_vec.push(&start_ts);
                params_vec.push(&end_ts);
                for p in &filter_params {
                    params_vec.push(p);
                }
                stmt.query_row(params_vec.as_slice(), |row| {
                    Ok(AggregateMetrics {
                        unique_visitors: row.get(0)?,
                        total_visits: row.get(1)?,
                        total_pageviews: row.get(2)?,
                        current_visits: None,
                        avg_visit_duration: row.get(3).unwrap_or(0),
                        bounce_rate: row.get(4).unwrap_or(0),
                    })
                })
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|e| {
                error!("error: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn calculate_realtime_aggregates(
        &self,
        filter_condition_str: String,
        filter_params: Vec<String>,
    ) -> Result<AggregateMetrics, StatusCode> {
        let now = Utc::now().timestamp();
        let thirty_minutes_ago = now - 30 * 60;

        let query = format!(
            "SELECT 
                COUNT(DISTINCT e.visitor_id) as unique_visitors,
                COUNT(DISTINCT v.id) as total_visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id ELSE NULL END) as total_pageviews,
                COUNT(DISTINCT CASE WHEN v.is_active = 1 THEN v.id ELSE NULL END) as current_visits,
                CAST(AVG(CASE 
                    WHEN v.id IS NOT NULL AND v.last_activity_at > v.timestamp 
                    THEN COALESCE(v.last_activity_at - v.timestamp, 0)
                    ELSE 0 
                END) AS INTEGER) as avg_visit_duration,
                CAST(CAST(COUNT(DISTINCT CASE WHEN v.id IS NOT NULL AND v.timestamp = v.last_activity_at THEN v.id ELSE NULL END) AS FLOAT) / CAST(NULLIF(COUNT(DISTINCT v.id), 0) AS FLOAT) * 100.0 AS INTEGER) as bounce_rate
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.timestamp >= ? AND e.timestamp <= ? {}",
            filter_condition_str
        );

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut params_vec_rt: Vec<&(dyn rusqlite::types::ToSql)> = Vec::new();
                params_vec_rt.push(&thirty_minutes_ago);
                params_vec_rt.push(&now);
                for p in &filter_params {
                    params_vec_rt.push(p);
                }
                stmt.query_row(params_vec_rt.as_slice(), |row| {
                    Ok(AggregateMetrics {
                        unique_visitors: row.get(0)?,
                        total_visits: row.get(1)?,
                        total_pageviews: row.get(2)?,
                        current_visits: Some(row.get(3)?),
                        avg_visit_duration: row.get(4).unwrap_or(0),
                        bounce_rate: row.get(5).unwrap_or(0),
                    })
                })
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|e| {
                error!("error: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn get_location_metrics(
        &self,
        start_ts: i64,
        end_ts: i64,
        metric: &Metric,
        grouping: LocationGrouping,
        filter_condition_str: String,
        filter_params: Vec<String>,
    ) -> Result<Vec<LocationMetrics>, StatusCode> {
        let (group_fields, group_field, order_field) = match grouping {
            LocationGrouping::Country => (
                "v.country, 'Unknown' as region, 'Unknown' as city",
                "v.country",
                "visitors",
            ),
            LocationGrouping::Region => (
                "'Unknown' as country, v.region, 'Unknown' as city",
                "v.region",
                "visitors",
            ),
            LocationGrouping::City => (
                "'Unknown' as country, 'Unknown' as region, v.city",
                "v.city",
                "visitors",
            ),
        };

        let order_direction = match metric {
            Metric::BounceRate => "ASC",
            _ => "DESC",
        };

        let query = format!(
            "SELECT 
                {},
                COUNT(DISTINCT e.visitor_id) as visitors,
                COUNT(DISTINCT v.id) as visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id ELSE NULL END) as pageviews,
                CAST(AVG(CASE 
                    WHEN v.id IS NOT NULL AND v.last_activity_at > v.timestamp 
                    THEN COALESCE(v.last_activity_at - v.timestamp, 0)
                    ELSE 0 
                END) AS INTEGER) as avg_visit_duration,
                CAST(CAST(COUNT(DISTINCT CASE WHEN v.id IS NOT NULL AND v.timestamp = v.last_activity_at THEN v.id ELSE NULL END) AS FLOAT) / CAST(NULLIF(COUNT(DISTINCT v.id), 0) AS FLOAT) * 100.0 AS INTEGER) as bounce_rate
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.timestamp >= ? AND e.timestamp <= ? {}
            GROUP BY {}
            ORDER BY {} {}",
            group_fields, filter_condition_str, group_field, order_field, order_direction
        );

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut params_vec_loc: Vec<&(dyn rusqlite::types::ToSql)> = Vec::new();
                params_vec_loc.push(&start_ts);
                params_vec_loc.push(&end_ts);
                for p in &filter_params {
                    params_vec_loc.push(p);
                }
                let rows = stmt.query_map(params_vec_loc.as_slice(), |row| {
                    Ok(LocationMetrics {
                        country: row.get(0).unwrap_or("".to_string()),
                        region: row.get(1).unwrap_or(None),
                        city: row.get(2).unwrap_or(None),
                        visitors: row.get(3).unwrap_or(0),
                        visits: row.get(4).unwrap_or(0),
                        pageviews: row.get(5).unwrap_or(0),
                        avg_visit_duration: row.get(6).unwrap_or(0),
                        bounce_rate: row.get(7).unwrap_or(0),
                    })
                })?;

                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|e| {
                error!("error: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn get_device_metrics(
        &self,
        start_ts: i64,
        end_ts: i64,
        metric: &Metric,
        grouping: DeviceGrouping,
        filter_condition_str: String,
        filter_params: Vec<String>,
    ) -> Result<Vec<DeviceMetrics>, StatusCode> {
        let (group_fields, group_field, order_field) = match grouping {
            DeviceGrouping::Browser => (
                "v.browser, 'Unknown' as operating_system, 'Unknown' as device_type",
                "v.browser",
                "visitors",
            ),
            DeviceGrouping::OperatingSystem => (
                "'Unknown' as browser, v.operating_system, 'Unknown' as device_type",
                "v.operating_system",
                "visitors",
            ),
            DeviceGrouping::DeviceType => (
                "'Unknown' as browser, 'Unknown' as operating_system, v.device_type",
                "v.device_type",
                "visitors",
            ),
        };

        let order_direction = match metric {
            Metric::BounceRate => "ASC",
            _ => "DESC",
        };

        let query = format!(
            "SELECT 
                {},
                COUNT(DISTINCT e.visitor_id) as visitors,
                COUNT(DISTINCT v.id) as visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id ELSE NULL END) as pageviews,
                CAST(AVG(CASE 
                    WHEN v.id IS NOT NULL AND v.last_activity_at > v.timestamp 
                    THEN COALESCE(v.last_activity_at - v.timestamp, 0)
                    ELSE 0 
                END) AS INTEGER) as avg_visit_duration,
                CAST(CAST(COUNT(DISTINCT CASE WHEN v.id IS NOT NULL AND v.timestamp = v.last_activity_at THEN v.id ELSE NULL END) AS FLOAT) / CAST(NULLIF(COUNT(DISTINCT v.id), 0) AS FLOAT) * 100.0 AS INTEGER) as bounce_rate
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.timestamp >= ? AND e.timestamp <= ? {}
            GROUP BY {}
            ORDER BY {} {}",
            group_fields, filter_condition_str, group_field, order_field, order_direction
        );

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut params_vec_dev: Vec<&(dyn rusqlite::types::ToSql)> = Vec::new();
                params_vec_dev.push(&start_ts);
                params_vec_dev.push(&end_ts);
                for p in &filter_params {
                    params_vec_dev.push(p);
                }
                let rows = stmt.query_map(params_vec_dev.as_slice(), |row| {
                    Ok(DeviceMetrics {
                        browser: row.get(0).unwrap_or("".to_string()),
                        operating_system: row.get(1).unwrap_or("".to_string()),
                        device_type: row.get(2).unwrap_or("".to_string()),
                        visitors: row.get(3).unwrap_or(0),
                        visits: row.get(4).unwrap_or(0),
                        pageviews: row.get(5).unwrap_or(0),
                        avg_visit_duration: row.get(6).unwrap_or(0),
                        bounce_rate: row.get(7).unwrap_or(0),
                    })
                })?;

                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|e| {
                error!("error: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn get_source_metrics(
        &self,
        start_ts: i64,
        end_ts: i64,
        metric: &Metric,
        grouping: SourceGrouping,
        filter_condition_str: String,
        filter_params: Vec<String>,
    ) -> Result<Vec<SourceMetrics>, StatusCode> {
        let (group_fields, group_field, order_field) = match grouping {
            SourceGrouping::Source => (
                "v.source, NULL as referrer, NULL as campaign",
                "v.source",
                "visitors",
            ),
            SourceGrouping::Referrer => (
                "v.source, v.referrer, NULL as campaign",
                "v.referrer",
                "visitors",
            ),
            SourceGrouping::Campaign => (
                "v.source, v.referrer, v.utm_campaign",
                "v.utm_campaign",
                "visitors",
            ),
        };

        let order_direction = match metric {
            Metric::BounceRate => "ASC",
            _ => "DESC",
        };

        let query = format!(
            "SELECT 
                {},
                COUNT(DISTINCT e.visitor_id) as visitors,
                COUNT(DISTINCT v.id) as visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id ELSE NULL END) as pageviews,
                CAST(AVG(CASE 
                    WHEN v.id IS NOT NULL AND v.last_activity_at > v.timestamp 
                    THEN COALESCE(v.last_activity_at - v.timestamp, 0)
                    ELSE 0 
                END) AS INTEGER) as avg_visit_duration,
                CAST(CAST(COUNT(DISTINCT CASE WHEN v.id IS NOT NULL AND v.timestamp = v.last_activity_at THEN v.id ELSE NULL END) AS FLOAT) / CAST(NULLIF(COUNT(DISTINCT v.id), 0) AS FLOAT) * 100.0 AS INTEGER) as bounce_rate
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.timestamp >= ? AND e.timestamp <= ? {}
            GROUP BY {}
            ORDER BY {} {}",
            group_fields, filter_condition_str, group_field, order_field, order_direction
        );

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut params_vec_src: Vec<&(dyn rusqlite::types::ToSql)> = Vec::new();
                params_vec_src.push(&start_ts);
                params_vec_src.push(&end_ts);
                for p in &filter_params {
                    params_vec_src.push(p);
                }
                let rows = stmt.query_map(params_vec_src.as_slice(), |row| {
                    Ok(SourceMetrics {
                        source: row.get(0).unwrap_or("".to_string()),
                        referrer: row.get(1).unwrap_or(None),
                        utm_source: None,
                        utm_medium: None,
                        utm_campaign: row.get(2).unwrap_or(None),
                        visitors: row.get(3).unwrap_or(0),
                        visits: row.get(4).unwrap_or(0),
                        pageviews: row.get(5).unwrap_or(0),
                        avg_visit_duration: row.get(6).unwrap_or(0),
                        bounce_rate: row.get(7).unwrap_or(0),
                    })
                })?;

                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|e| {
                error!("error: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn get_page_metrics(
        &self,
        start_ts: i64,
        end_ts: i64,
        metric: &Metric,
        grouping: PageGrouping,
        filter_condition_str: String,
        filter_params: Vec<String>,
    ) -> Result<Vec<PageMetrics>, StatusCode> {
        let (group_fields, group_field, order_field) = match grouping {
            PageGrouping::Page => (
                "v.page_url_path, NULL as entry_page_path, NULL as exit_page_path",
                "v.page_url_path",
                "visitors"
            ),
            PageGrouping::EntryPage => (
                "NULL as page_url_path, v.page_url_path as entry_page_path, NULL as exit_page_path",
                "v.page_url_path",
                "visitors"
            ),
            PageGrouping::ExitPage => (
                "NULL as page_url_path, NULL as entry_page_path, v.last_visited_url_path as exit_page_path",
                "v.last_visited_url_path",
                "visitors"
            ),
        };

        let order_direction = match metric {
            Metric::BounceRate => "ASC",
            _ => "DESC",
        };

        let query = format!(
            "SELECT 
                {},
                COUNT(DISTINCT e.visitor_id) as visitors,
                COUNT(DISTINCT v.id) as visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id ELSE NULL END) as pageviews,
                CAST(AVG(CASE 
                    WHEN v.id IS NOT NULL AND v.last_activity_at > v.timestamp 
                    THEN COALESCE(v.last_activity_at - v.timestamp, 0)
                    ELSE 0 
                END) AS INTEGER) as avg_visit_duration,
                CAST(CAST(COUNT(DISTINCT CASE WHEN v.id IS NOT NULL AND v.timestamp = v.last_activity_at THEN v.id ELSE NULL END) AS FLOAT) / CAST(NULLIF(COUNT(DISTINCT v.id), 0) AS FLOAT) * 100.0 AS INTEGER) as bounce_rate
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.timestamp >= ? AND e.timestamp <= ? {}
            GROUP BY {}
            ORDER BY {} {}",
            group_fields, filter_condition_str, group_field, order_field, order_direction
        );

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut params_vec_page: Vec<&(dyn rusqlite::types::ToSql)> = Vec::new();
                params_vec_page.push(&start_ts);
                params_vec_page.push(&end_ts);
                for p in &filter_params {
                    params_vec_page.push(p);
                }
                let rows = stmt.query_map(params_vec_page.as_slice(), |row| {
                    Ok(PageMetrics {
                        page_path: row.get(0).unwrap_or("".to_string()),
                        entry_page_path: row.get(1).unwrap_or("".to_string()),
                        exit_page_path: row.get(2).unwrap_or("".to_string()),
                        visitors: row.get(3).unwrap_or(0),
                        visits: row.get(4).unwrap_or(0),
                        pageviews: row.get(5).unwrap_or(0),
                        avg_visit_duration: row.get(6).unwrap_or(0),
                        bounce_rate: row.get(7).unwrap_or(0),
                    })
                })?;

                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|e| {
                error!("error: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }
}

pub async fn get_filtered_statistics(
    State(state): State<AppState>,
    Query(query): Query<FilteredStatisticsQuery>,
) -> Result<Json<FilteredStatistics>, StatusCode> {
    let handler = FilteredStatisticsHandler::new(state);
    let stats = handler.get_filtered_statistics(query).await?;
    Ok(Json(stats))
}
