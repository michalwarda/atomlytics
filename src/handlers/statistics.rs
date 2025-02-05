use std::sync::Arc;
use chrono::DateTime;
use chrono::Timelike;
use chrono::Utc;
use rusqlite::params;
use tokio_rusqlite::Connection;
use tracing::error;

use crate::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

pub struct StatisticsAggregator {
    db: Arc<Connection>,
}

#[derive(Deserialize)]
pub struct StatisticsParams {
    pub timeframe: TimeFrame,
    pub granularity: Granularity,
    pub metric: Metric,
    pub location_grouping: LocationGrouping,
    pub device_grouping: DeviceGrouping,
    pub source_grouping: SourceGrouping,
}

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
pub struct Statistics {
    stats: Vec<(i64, i64)>,
    aggregates: AggregateMetrics,
    realtime_aggregates: AggregateMetrics,
    location_metrics: Vec<LocationMetrics>,
    device_metrics: Vec<DeviceMetrics>,
    source_metrics: Vec<SourceMetrics>,
}

#[derive(Serialize)]
pub struct AggregateMetrics {
    pub unique_visitors: i64,
    pub total_visits: i64,
    pub total_pageviews: i64,
    pub current_visits: Option<i64>,
    pub avg_visit_duration: i64,
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
}

pub async fn get_statistics(
    State(state): State<AppState>,
    Query(params): Query<StatisticsParams>,
) -> Result<Json<Statistics>, StatusCode> {
    let aggregator = StatisticsAggregator::new(state.db);
    aggregator
        .get_filtered_statistics(
            params.timeframe,
            params.granularity,
            params.metric,
            params.location_grouping,
            params.device_grouping,
            params.source_grouping,
        )
        .await
        .map(Json)
        .map_err(|e| {
            error!("Failed to get statistics: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

impl StatisticsAggregator {
    pub fn new(db: Arc<Connection>) -> Self {
        Self { db }
    }

    pub async fn aggregate_active_statistics(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        
        self.mark_inactive_visits(now).await?;
        self.aggregate_active_stats(now).await?;
        self.aggregate_active_period_metrics(now).await?;
        self.remove_unused_active_aggregated_metrics(now).await?;
        Ok(())
    }

    pub async fn aggregate_statistics(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
            
        self.aggregate_minute_stats(now).await?;
        self.aggregate_hourly_stats(now).await?;
        self.aggregate_daily_stats(now).await?;
        self.aggregate_period_metrics(now).await?;
        Ok(())
    }

    async fn remove_unused_active_aggregated_metrics(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let thirty_minutes_ago = now - chrono::Duration::minutes(30);
        let thirty_minutes_ago_ts = thirty_minutes_ago.timestamp();

        self.db.call(move |conn| {
            conn.execute("DELETE FROM aggregated_metrics WHERE period_name = 'realtime' AND start_ts < ?", params![thirty_minutes_ago_ts])?;
            conn.execute("DELETE FROM location_aggregated_metrics WHERE period_name = 'realtime' AND start_ts < ?", params![thirty_minutes_ago_ts])?;
            conn.execute("DELETE FROM device_aggregated_metrics WHERE period_name = 'realtime' AND start_ts < ?", params![thirty_minutes_ago_ts])?;
            conn.execute("DELETE FROM source_aggregated_metrics WHERE period_name = 'realtime' AND start_ts < ?", params![thirty_minutes_ago_ts])?;
            Ok(())
        }).await?;
        Ok(())
    }

    async fn mark_inactive_visits(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let active_threshold = now - chrono::Duration::minutes(30);

        self.db
            .call(move |conn| {
                conn.execute("UPDATE events SET is_active = 0 WHERE event_type = 'visit' AND last_activity_at < ?", [active_threshold.timestamp()])?;
                Ok(())
            })
            .await
    }

    async fn aggregate_stats_for_period(
        &self,
        period_type: &str,
        time_division: i64,
        start_timestamp: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        let period_type = period_type.to_string();
        self.db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO statistics (
                        period_type, period_start, unique_visitors, total_visits, 
                        total_pageviews, avg_visit_duration, created_at
                    )
                    SELECT 
                        ?1 as period_type,
                        (timestamp / ?2) * ?2 as period_start,
                        COUNT(DISTINCT visitor_id) as unique_visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as total_visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as total_pageviews,
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now') as created_at
                    FROM events 
                    WHERE timestamp >= ?3
                    GROUP BY period_start",
                    params![period_type.to_string(), time_division, start_timestamp],
                )?;

                conn.execute(
                    "INSERT OR REPLACE INTO location_statistics (
                        period_type, period_start, country, region, city, visitors, visits, pageviews, avg_visit_duration, created_at
                    )
                    SELECT 
                        ?1 as period_type,
                        (timestamp / ?2) * ?2 as period_start,
                        COALESCE(country, 'Unknown') as country,
                        COALESCE(region, 'Unknown') as region,
                        COALESCE(city, 'Unknown') as city,
                        COUNT(DISTINCT visitor_id) as visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as pageviews,
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now') as created_at
                    FROM events
                    WHERE timestamp >= ?3 AND country IS NOT NULL
                    GROUP BY period_start, country, region, city",
                    params![period_type.to_string(), time_division, start_timestamp],
                )?;

                // Add device statistics
                conn.execute(
                    "INSERT OR REPLACE INTO device_statistics (
                        period_type, period_start, browser, operating_system, device_type,
                        visitors, visits, pageviews, avg_visit_duration, created_at
                    )
                    SELECT 
                        ?1 as period_type,
                        (timestamp / ?2) * ?2 as period_start,
                        COALESCE(browser, 'Unknown') as browser,
                        COALESCE(operating_system, 'Unknown') as operating_system,
                        COALESCE(device_type, 'Unknown') as device_type,
                        COUNT(DISTINCT visitor_id) as visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as pageviews,
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now') as created_at
                    FROM events
                    WHERE timestamp >= ?3 AND browser IS NOT NULL
                    GROUP BY period_start, browser, operating_system, device_type",
                    params![period_type.to_string(), time_division, start_timestamp],
                )?;

                // Add source statistics
                conn.execute(
                    "INSERT OR REPLACE INTO source_statistics (
                        period_type, period_start, source, referrer, utm_source, utm_medium, utm_campaign,
                        visitors, visits, pageviews, avg_visit_duration, created_at
                    )
                    SELECT 
                        ?1 as period_type,
                        (timestamp / ?2) * ?2 as period_start,
                        COALESCE(source, 'Direct') as source,
                        COALESCE(referrer, 'Unknown') as referrer,
                        COALESCE(utm_source, 'Unknown') as utm_source,
                        COALESCE(utm_medium, 'Unknown') as utm_medium,
                        COALESCE(utm_campaign, 'Unknown') as utm_campaign,
                        COUNT(DISTINCT visitor_id) as visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as pageviews,
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now') as created_at
                    FROM events
                    WHERE timestamp >= ?3
                    GROUP BY period_start, source, referrer, utm_source, utm_medium, utm_campaign",
                    params![period_type.to_string(), time_division, start_timestamp],
                )?;

                Ok(())
            })
            .await
    }

    async fn aggregate_active_stats(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let thirty_minutes_ago = now - chrono::Duration::minutes(30);
        self.aggregate_stats_for_period("realtime", 60, thirty_minutes_ago.timestamp()).await
    }

    async fn aggregate_minute_stats(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let five_minutes_ago = now - chrono::Duration::days(5);
        self.aggregate_stats_for_period("minute", 60, five_minutes_ago.timestamp()).await
    }

    async fn aggregate_hourly_stats(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let day_ago = now - chrono::Duration::days(5);
        self.aggregate_stats_for_period("hour", 3600, day_ago.timestamp()).await
    }

    async fn aggregate_daily_stats(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let month_ago = now - chrono::Duration::days(30);
        self.aggregate_stats_for_period("day", 86400, month_ago.timestamp()).await
    }

    async fn aggregate_period_metrics(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_start_ts = today_start.and_utc().timestamp();
        let today_end = today_start + chrono::Duration::days(1);
        let today_end_ts = today_end.and_utc().timestamp();

        let periods = vec![
            ("today", today_start_ts, today_end_ts),
            ("yesterday", today_start_ts - 86400, today_start_ts),
            ("last_7_days", today_start_ts - 7 * 86400, today_start_ts),
            ("last_30_days", today_start_ts - 30 * 86400, today_start_ts),
        ];

        for (period_name, start_ts, end_ts) in periods {
            self.aggregate_metrics_for_period(period_name, start_ts, end_ts, None).await?;
        }

        Ok(())
    }

    async fn aggregate_active_period_metrics(&self, now: DateTime<Utc>) -> Result<(), tokio_rusqlite::Error> {
        let thirty_minutes_ago = now.timestamp() - 30 * 60;
        self.aggregate_metrics_for_period("realtime", thirty_minutes_ago, now.timestamp(), Some(true)).await
    }

    async fn aggregate_metrics_for_period(
        &self,
        period_name: &str,
        start_ts: i64,
        end_ts: i64,
        include_current_visits: Option<bool>,
    ) -> Result<(), tokio_rusqlite::Error> {
        let period_name = period_name.to_string();
        self.db
            .call(move |conn| {
                // Base metrics query
                let metrics_query = if include_current_visits == Some(true) {
                    "INSERT OR REPLACE INTO aggregated_metrics 
                    (period_name, start_ts, end_ts, unique_visitors, total_visits, total_pageviews, current_visits, created_at)
                    SELECT 
                        ?,
                        ?,
                        ?,
                        COUNT(DISTINCT visitor_id),
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END),
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END),
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' AND is_active = 1 THEN id ELSE NULL END),
                        strftime('%s', 'now')
                    FROM events 
                    WHERE timestamp >= ? AND timestamp <= ?"
                } else {
                    "INSERT OR REPLACE INTO aggregated_metrics 
                    (period_name, start_ts, end_ts, unique_visitors, total_visits, total_pageviews, avg_visit_duration, created_at)
                    SELECT 
                        ?,
                        ?,
                        ?,
                        COUNT(DISTINCT visitor_id),
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END),
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END),
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now')
                    FROM events 
                    WHERE timestamp >= ? AND timestamp <= ?"
                };

                conn.execute(
                    metrics_query,
                    params![period_name, start_ts, end_ts, start_ts, end_ts],
                )?;

                // Location metrics
                conn.execute(
                    "INSERT OR REPLACE INTO location_aggregated_metrics 
                    (period_name, start_ts, end_ts, country, region, city, visitors, visits, pageviews, avg_visit_duration, created_at)
                    SELECT 
                        ?,
                        ?,
                        ?,
                        COALESCE(country, 'Unknown') as country,
                        COALESCE(region, 'Unknown') as region,
                        COALESCE(city, 'Unknown') as city,
                        COUNT(DISTINCT visitor_id),
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END),
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END),
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now') as created_at
                    FROM events 
                    WHERE timestamp >= ? AND timestamp <= ? AND country IS NOT NULL
                    GROUP BY country, region, city",
                    params![period_name, start_ts, end_ts, start_ts, end_ts],
                )?;

                // Device metrics
                conn.execute(
                    "INSERT OR REPLACE INTO device_aggregated_metrics 
                    (period_name, start_ts, end_ts, browser, operating_system, device_type,
                     visitors, visits, pageviews, avg_visit_duration, created_at)
                    SELECT 
                        ?,
                        ?,
                        ?,
                        COALESCE(browser, 'Unknown') as browser,
                        COALESCE(operating_system, 'Unknown') as operating_system,
                        COALESCE(device_type, 'Unknown') as device_type,
                        COUNT(DISTINCT visitor_id),
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END),
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END),
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now') as created_at
                    FROM events 
                    WHERE timestamp >= ? AND timestamp <= ? AND browser IS NOT NULL
                    GROUP BY browser, operating_system, device_type",
                    params![period_name, start_ts, end_ts, start_ts, end_ts],
                )?;

                // Source metrics
                conn.execute(
                    "INSERT OR REPLACE INTO source_aggregated_metrics 
                    (period_name, start_ts, end_ts, source, referrer, utm_source, utm_medium, utm_campaign,
                     visitors, visits, pageviews, avg_visit_duration, created_at)
                    SELECT 
                        ?,
                        ?,
                        ?,
                        COALESCE(source, 'Direct') as source,
                        COALESCE(referrer, 'Unknown') as referrer,
                        COALESCE(utm_source, 'Unknown') as utm_source,
                        COALESCE(utm_medium, 'Unknown') as utm_medium,
                        COALESCE(utm_campaign, 'Unknown') as utm_campaign,
                        COUNT(DISTINCT visitor_id),
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END),
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END),
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration,
                        strftime('%s', 'now') as created_at
                    FROM events 
                    WHERE timestamp >= ? AND timestamp <= ?
                    GROUP BY source, referrer, utm_source, utm_medium, utm_campaign",
                    params![period_name, start_ts, end_ts, start_ts, end_ts],
                )?;

                Ok(())
            })
            .await
    }

    async fn calculate_aggregates(&self, start_ts: i64, end_ts: i64) -> Result<AggregateMetrics, tokio_rusqlite::Error> {
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT 
                        COUNT(DISTINCT visitor_id) as unique_visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as total_visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as total_pageviews,
                        CAST(AVG(CASE 
                            WHEN event_type = 'visit' AND last_activity_at > timestamp 
                            THEN last_activity_at - timestamp 
                            ELSE NULL 
                        END) AS INTEGER) as avg_visit_duration
                     FROM events 
                     WHERE timestamp >= ? AND timestamp <= ?"
                )?;
                
                let row = stmt.query_row([start_ts, end_ts], |row| {
                    Ok(AggregateMetrics {
                        unique_visitors: row.get(0)?,
                        total_visits: row.get(1)?,
                        total_pageviews: row.get(2)?,
                        current_visits: None,
                        avg_visit_duration: row.get(3).unwrap_or(0),
                    })
                })?;
                
                Ok(row)
            })
            .await
    }

    pub async fn get_filtered_statistics(
        &self,
        timeframe: TimeFrame,
        granularity: Granularity,
        metric: Metric,
        location_grouping: LocationGrouping,
        device_grouping: DeviceGrouping,
        source_grouping: SourceGrouping,
    ) -> Result<Statistics, tokio_rusqlite::Error> {
        let now = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_start_ts = today_start.and_utc().timestamp();
        let today_end = today_start + chrono::Duration::days(1);
        let today_end_ts = today_end.and_utc().timestamp();

        let (start_ts, end_ts, period_name) = match timeframe {
            TimeFrame::Realtime => (now.timestamp() - 30 * 60, now.timestamp(), Some("realtime")),
            TimeFrame::Today => (today_start_ts, today_end_ts, Some("today")),
            TimeFrame::Yesterday => (today_start_ts - 86400, today_start_ts, Some("yesterday")),
            TimeFrame::Last7Days => (now.timestamp() - 7 * 86400, now.timestamp(), Some("last_7_days")),
            TimeFrame::Last30Days => (now.timestamp() - 30 * 86400, now.timestamp(), Some("last_30_days")),
            TimeFrame::AllTime => (0, now.timestamp(), None),
        };

        let stats = self.get_time_series_data(start_ts, end_ts, granularity, metric).await?;


        let aggregates = match period_name {
            Some(period) => {
                self.db
                    .call(move |conn| {
                        let mut stmt = conn.prepare(
                            "SELECT unique_visitors, total_visits, total_pageviews, avg_visit_duration 
                             FROM aggregated_metrics 
                             WHERE period_name = ?
                             "
                        )?;
                        
                        Ok(stmt.query_row(params![period], |row| {
                            Ok(AggregateMetrics {
                                unique_visitors: row.get(0)?,
                                total_visits: row.get(1)?,
                                total_pageviews: row.get(2)?,
                                current_visits: None,
                                avg_visit_duration: row.get(3).unwrap_or(0),
                            })
                        })?)
                    })
                    .await
            }
            None => self.calculate_aggregates(start_ts, end_ts).await,
        }?;


        let realtime_aggregates = self.get_realtime_aggregates().await?;

        let location_metrics = self.get_location_metrics(&timeframe, &metric, location_grouping).await?;

        let device_metrics = self.get_device_metrics(&timeframe, &metric, device_grouping).await?;

        let source_metrics = self.get_source_metrics(&timeframe, &metric, source_grouping).await?;

        Ok(Statistics {
            stats,
            aggregates,
            realtime_aggregates,
            location_metrics,
            device_metrics,
            source_metrics,
        })
    }

    async fn get_time_series_data(
        &self,
        start_ts: i64,
        end_ts: i64,
        granularity: Granularity,
        metric: Metric,
    ) -> Result<Vec<(i64, i64)>, tokio_rusqlite::Error> {
        let period_type = match granularity {
            Granularity::Minutes => "minute",
            Granularity::Hours => "hour",
            Granularity::Days => "day",
        };

        let interval = match granularity {
            Granularity::Minutes => 60,
            Granularity::Hours => 3600,
            Granularity::Days => 86400,
        };

        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT period_start, unique_visitors, total_visits, total_pageviews, avg_visit_duration
                     FROM statistics 
                     WHERE period_type = ? 
                     AND period_start >= ? 
                     AND period_start <= ?
                     ORDER BY period_start ASC",
                )?;

                let rows = stmt.query_map(
                    [period_type, &start_ts.to_string(), &end_ts.to_string()],
                    |row| {
                        match metric {
                            Metric::UniqueVisitors => Ok((row.get(0)?, row.get(1)?)),
                            Metric::Visits => Ok((row.get(0)?, row.get(2)?)),
                            Metric::Pageviews => Ok((row.get(0)?, row.get(3)?)),
                            Metric::AvgVisitDuration => Ok((row.get(0)?, row.get(4).unwrap_or(0))),
                        }
                    },
                )
                .map_err(|_| {
                    error!("Failed to get time series data");
                    rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some("Failed to get time series data".to_string()),
                    )
                })?;


                // Convert to HashMap for easy lookup
                let mut data_map: std::collections::HashMap<i64, i64> =
                    std::collections::HashMap::new();
                let mut lower_bound = None;
                for row in rows {
                    let (timestamp, visitors) = row?;
                    if lower_bound.is_none() {
                        lower_bound = Some(timestamp);
                    }
                    data_map.insert(timestamp, visitors);
                }

                // Generate complete series
                let mut complete_series = Vec::new();
                let mut normalized_start_ts = start_ts;
                if start_ts == 0 {
                    normalized_start_ts = lower_bound.unwrap();
                }
                let mut current_ts = normalized_start_ts - (normalized_start_ts % interval);
                while current_ts <= end_ts {
                    let visitors = data_map.get(&current_ts).copied().unwrap_or(0);
                    complete_series.push((current_ts, visitors));
                    current_ts += interval;
                }

                Ok(complete_series)
            })
            .await
    }

    async fn get_realtime_aggregates(&self) -> Result<AggregateMetrics, tokio_rusqlite::Error> {
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT unique_visitors, total_visits, total_pageviews, current_visits, avg_visit_duration
                     FROM aggregated_metrics
                     WHERE period_name = 'realtime'"
                )?;

                Ok(stmt.query_row([], |row| {
                    Ok(AggregateMetrics {
                        unique_visitors: row.get(0)?,
                        total_visits: row.get(1)?,
                        total_pageviews: row.get(2)?,
                        current_visits: row.get(3)?,
                        avg_visit_duration: row.get(4).unwrap_or(0),
                    })
                })?)
            })
        .await
    }

    async fn get_location_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: LocationGrouping,
    ) -> Result<Vec<LocationMetrics>, tokio_rusqlite::Error> {
        let now = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_start_ts = today_start.and_utc().timestamp();
        let today_end = today_start + chrono::Duration::days(1);
        let today_end_ts = today_end.and_utc().timestamp();

        let metric_str = match metric {
            Metric::UniqueVisitors => "visitors",
            Metric::Visits => "visits",
            Metric::Pageviews => "pageviews",
            Metric::AvgVisitDuration => "avg_visit_duration",
        };

        let (start_ts, end_ts, period_name) = match timeframe {
            TimeFrame::Realtime => (now.timestamp() - 30 * 60, now.timestamp(), Some("realtime")),
            TimeFrame::Today => (today_start_ts, today_end_ts, Some("today")),
            TimeFrame::Yesterday => (today_start_ts - 86400, today_start_ts, Some("yesterday")),
            TimeFrame::Last7Days => (now.timestamp() - 7 * 86400, now.timestamp(), Some("last_7_days")),
            TimeFrame::Last30Days => (now.timestamp() - 30 * 86400, now.timestamp(), Some("last_30_days")),
            TimeFrame::AllTime => (0, now.timestamp(), None),
        };

        let group_by_clause = match grouping {
            LocationGrouping::Country => "country",
            LocationGrouping::Region => "country, region",
            LocationGrouping::City => "country, region, city",
        };

        self.db
            .call(move |conn| {
                let query = format!(
                    "SELECT 
                        country,
                        region,
                        city,
                        SUM(visitors) as visitors,
                        SUM(visits) as visits,
                        SUM(pageviews) as pageviews,
                        CAST(AVG(avg_visit_duration) AS INTEGER) as avg_visit_duration
                     FROM location_aggregated_metrics
                     WHERE period_name = ?
                     AND start_ts >= ?
                     AND end_ts <= ?
                     GROUP BY {}
                     ORDER BY {} DESC",
                    group_by_clause, metric_str
                );

                let mut stmt = conn.prepare(&query)?;

                // TODO: Handle AllTime
                let metrics = stmt.query_map(params![period_name.unwrap_or(""), start_ts, end_ts], |row| {
                    let visitors: i64 = row.get(3)?;
                    let visits: i64 = row.get(4)?;
                    let pageviews: i64 = row.get(5)?;
                    let avg_visit_duration: i64 = row.get(6).unwrap_or(0);
                    
                    let value = match metric_str {
                        "visitors" => visitors,
                        "visits" => visits,
                        "pageviews" => pageviews,
                        "avg_visit_duration" => avg_visit_duration,
                        _ => 0,
                    };
                    
                    if value > 0 {
                        Ok(Some(LocationMetrics {
                            country: row.get(0)?,
                            region: row.get(1)?,
                            city: row.get(2)?,
                            visitors,
                            visits,
                            pageviews,
                            avg_visit_duration,
                        }))
                    } else {
                        Ok(None)
                    }
                })?.filter_map(|r| r.transpose()).collect::<Result<Vec<_>, _>>()?;

                Ok(metrics)
            })
            .await
    }

    async fn get_device_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: DeviceGrouping,
    ) -> Result<Vec<DeviceMetrics>, tokio_rusqlite::Error> {
        let now = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_start_ts = today_start.and_utc().timestamp();
        let today_end = today_start + chrono::Duration::days(1);
        let today_end_ts = today_end.and_utc().timestamp();

        let metric_str = match metric {
            Metric::UniqueVisitors => "visitors",
            Metric::Visits => "visits",
            Metric::Pageviews => "pageviews",
            Metric::AvgVisitDuration => "avg_visit_duration",
        };

        let (start_ts, end_ts, period_name) = match timeframe {
            TimeFrame::Realtime => (now.timestamp() - 30 * 60, now.timestamp(), Some("realtime")),
            TimeFrame::Today => (today_start_ts, today_end_ts, Some("today")),
            TimeFrame::Yesterday => (today_start_ts - 86400, today_start_ts, Some("yesterday")),
            TimeFrame::Last7Days => (now.timestamp() - 7 * 86400, now.timestamp(), Some("last_7_days")),
            TimeFrame::Last30Days => (now.timestamp() - 30 * 86400, now.timestamp(), Some("last_30_days")),
            TimeFrame::AllTime => (0, now.timestamp(), None),
        };

        let group_by_clause = match grouping {
            DeviceGrouping::Browser => "browser",
            DeviceGrouping::OperatingSystem => "operating_system",
            DeviceGrouping::DeviceType => "device_type",
        };

        self.db
            .call(move |conn| {
                let query = format!(
                    "SELECT 
                        browser,
                        operating_system,
                        device_type,
                        SUM(visitors) as visitors,
                        SUM(visits) as visits,
                        SUM(pageviews) as pageviews,
                        CAST(AVG(avg_visit_duration) AS INTEGER) as avg_visit_duration
                     FROM device_aggregated_metrics
                     WHERE period_name = ?
                     AND start_ts >= ?
                     AND end_ts <= ?
                     GROUP BY {}
                     ORDER BY {} DESC",
                    group_by_clause, metric_str
                );

                let mut stmt = conn.prepare(&query)?;

                let metrics = stmt.query_map(params![period_name.unwrap_or(""), start_ts, end_ts], |row| {
                    let visitors: i64 = row.get(3)?;
                    let visits: i64 = row.get(4)?;
                    let pageviews: i64 = row.get(5)?;
                    let avg_visit_duration: i64 = row.get(6).unwrap_or(0);
                    
                    let value = match metric_str {
                        "visitors" => visitors,
                        "visits" => visits,
                        "pageviews" => pageviews,
                        "avg_visit_duration" => avg_visit_duration,
                        _ => 0,
                    };
                    
                    if value > 0 {
                        Ok(Some(DeviceMetrics {
                            browser: row.get(0)?,
                            operating_system: row.get(1)?,
                            device_type: row.get(2)?,
                            visitors,
                            visits,
                            pageviews,
                            avg_visit_duration,
                        }))
                    } else {
                        Ok(None)
                    }
                })?.filter_map(|r| r.transpose()).collect::<Result<Vec<_>, _>>()?;

                Ok(metrics)
            })
            .await
    }

    async fn get_source_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: SourceGrouping,
    ) -> Result<Vec<SourceMetrics>, tokio_rusqlite::Error> {
        let now = Utc::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_start_ts = today_start.and_utc().timestamp();
        let today_end = today_start + chrono::Duration::days(1);
        let today_end_ts = today_end.and_utc().timestamp();

        let metric_str = match metric {
            Metric::UniqueVisitors => "visitors",
            Metric::Visits => "visits",
            Metric::Pageviews => "pageviews",
            Metric::AvgVisitDuration => "avg_visit_duration",
        };

        let (start_ts, end_ts, period_name) = match timeframe {
            TimeFrame::Realtime => (now.timestamp() - 30 * 60, now.timestamp(), Some("realtime")),
            TimeFrame::Today => (today_start_ts, today_end_ts, Some("today")),
            TimeFrame::Yesterday => (today_start_ts - 86400, today_start_ts, Some("yesterday")),
            TimeFrame::Last7Days => (now.timestamp() - 7 * 86400, now.timestamp(), Some("last_7_days")),
            TimeFrame::Last30Days => (now.timestamp() - 30 * 86400, now.timestamp(), Some("last_30_days")),
            TimeFrame::AllTime => (0, now.timestamp(), None),
        };

        let group_by_clause = match grouping {
            SourceGrouping::Source => "source",
            SourceGrouping::Referrer => "referrer",
            SourceGrouping::Campaign => "utm_source, utm_medium, utm_campaign",
        };

        self.db
            .call(move |conn| {
                let query = format!(
                    "SELECT 
                        source,
                        referrer,
                        utm_source,
                        utm_medium,
                        utm_campaign,
                        SUM(visitors) as visitors,
                        SUM(visits) as visits,
                        SUM(pageviews) as pageviews,
                        CAST(AVG(avg_visit_duration) AS INTEGER) as avg_visit_duration
                     FROM source_aggregated_metrics
                     WHERE period_name = ?
                     AND start_ts >= ?
                     AND end_ts <= ?
                     GROUP BY {}
                     ORDER BY {} DESC",
                    group_by_clause, metric_str
                );

                let mut stmt = conn.prepare(&query)?;

                let metrics = stmt.query_map(params![period_name.unwrap_or(""), start_ts, end_ts], |row| {
                    let visitors: i64 = row.get(5)?;
                    let visits: i64 = row.get(6)?;
                    let pageviews: i64 = row.get(7)?;
                    let avg_visit_duration: i64 = row.get(8).unwrap_or(0);

                    println!("avg_visit_duration: {}", avg_visit_duration);
                    
                    let value = match metric_str {
                        "visitors" => visitors,
                        "visits" => visits,
                        "pageviews" => pageviews,
                        "avg_visit_duration" => avg_visit_duration,
                        _ => 0,
                    };
                    
                    if value > 0 {
                        Ok(Some(SourceMetrics {
                            source: row.get(0)?,
                            referrer: row.get(1)?,
                            utm_source: row.get(2)?,
                            utm_medium: row.get(3)?,
                            utm_campaign: row.get(4)?,
                            visitors,
                            visits,
                            pageviews,
                            avg_visit_duration,
                        }))
                    } else {
                        Ok(None)
                    }
                })?.filter_map(|r| r.transpose()).collect::<Result<Vec<_>, _>>()?;

                Ok(metrics)
            })
            .await
    }
}
