use chrono::Utc;
use rusqlite::params;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::error;

pub struct StatisticsAggregator {
    db: Arc<Connection>,
}

#[derive(serde::Deserialize)]
pub enum Metric {
    UniqueVisitors,
    Visits,
    Pageviews,
}

impl StatisticsAggregator {
    pub fn new(db: Arc<Connection>) -> Self {
        Self { db }
    }

    pub async fn aggregate_active_statistics(&self) -> Result<(), tokio_rusqlite::Error> {
        self.mark_inactive_visits().await?;
        self.aggregate_active_stats().await?;
        self.aggregate_active_period_metrics().await?;
        Ok(())
    }

    pub async fn aggregate_statistics(&self) -> Result<(), tokio_rusqlite::Error> {
        self.aggregate_minute_stats().await?;
        self.aggregate_hourly_stats().await?;
        self.aggregate_daily_stats().await?;
        self.aggregate_period_metrics().await?;
        Ok(())
    }

    async fn mark_inactive_visits(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = chrono::Utc::now();
        let active_threshold = now - chrono::Duration::minutes(30);

        self.db
            .call(move |conn| {
                conn.execute("UPDATE events SET is_active = 1 WHERE event_type = 'visit' AND last_activity_at >= ?", [active_threshold.timestamp()])?;
                Ok(())
            })
            .await
    }

    async fn aggregate_active_stats(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = chrono::Utc::now();
        let thirty_minutes_ago = now - chrono::Duration::minutes(30);
        self.db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO statistics (period_type, period_start, unique_visitors, total_visits, total_pageviews, created_at)
                        SELECT 
                            'realtime' as period_type,
                            (timestamp / 60) * 60 as period_start,
                            COUNT(DISTINCT visitor_id) as unique_visitors,
                            COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as total_visits,
                            COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as total_pageviews,
                            strftime('%s', 'now') as created_at
                        FROM events
                        WHERE timestamp >= ?
                        GROUP BY period_start",
                    [thirty_minutes_ago.timestamp()],
                )?;
                // Insert a zero record if no data exists
                conn.execute(
                    "INSERT OR IGNORE INTO statistics (period_type, period_start, unique_visitors, total_visits, total_pageviews, created_at)
                    SELECT 
                        'realtime' as period_type,
                        0 as period_start,
                        0 as unique_visitors, 
                        0 as total_visits,
                        0 as total_pageviews,
                        strftime('%s', 'now') as created_at
                    WHERE NOT EXISTS (
                        SELECT 1 FROM statistics WHERE period_type = 'realtime'
                    )",
                    [],
                )?;
                Ok(())
            })
            .await
    }

    async fn aggregate_minute_stats(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = chrono::Utc::now();
        let five_minutes_ago = now - chrono::Duration::minutes(5);

        self.db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO statistics (period_type, period_start, unique_visitors, total_visits, total_pageviews, created_at)
                     SELECT 
                        'minute' as period_type,
                        (timestamp / 60) * 60 as period_start,
                        COUNT(DISTINCT visitor_id) as unique_visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as total_visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as total_pageviews,
                        strftime('%s', 'now') as created_at
                     FROM events 
                     WHERE timestamp >= ?
                     GROUP BY period_start",
                    [five_minutes_ago.timestamp()],
                )?;
                Ok(())
            })
            .await
    }

    async fn aggregate_hourly_stats(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = chrono::Utc::now();
        let day_ago = now - chrono::Duration::days(1);

        self.db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO statistics (period_type, period_start, unique_visitors, total_visits, total_pageviews, created_at)
                     SELECT 
                        'hour' as period_type,
                        (timestamp / 3600) * 3600 as period_start,
                        COUNT(DISTINCT visitor_id) as unique_visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as total_visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as total_pageviews,
                        strftime('%s', 'now') as created_at
                     FROM events 
                     WHERE timestamp >= ?
                     GROUP BY period_start",
                    [day_ago.timestamp()],
                )?;
                Ok(())
            })
            .await
    }

    async fn aggregate_daily_stats(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = chrono::Utc::now();
        let month_ago = now - chrono::Duration::days(30);

        self.db
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO statistics (period_type, period_start, unique_visitors, total_visits, total_pageviews, created_at)
                     SELECT 
                        'day' as period_type,
                        (timestamp / 86400) * 86400 as period_start,
                        COUNT(DISTINCT visitor_id) as unique_visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as total_visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as total_pageviews,
                        strftime('%s', 'now') as created_at
                     FROM events 
                     WHERE timestamp >= ?
                     GROUP BY period_start",
                    [month_ago.timestamp()],
                )?;
                Ok(())
            })
            .await
    }

    async fn aggregate_period_metrics(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = chrono::Utc::now();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_start_ts = today_start.and_utc().timestamp();

        let periods = vec![
            ("today", today_start_ts, now.timestamp()),
            ("yesterday", today_start_ts - 86400, today_start_ts),
            ("last_7_days", now.timestamp() - 7 * 86400, now.timestamp()),
            ("last_30_days", now.timestamp() - 30 * 86400, now.timestamp()),
        ];

        self.db
            .call(move |conn| {
                for (period_name, start_ts, end_ts) in periods {
                    conn.execute(
                        "INSERT OR REPLACE INTO aggregated_metrics 
                        (period_name, start_ts, end_ts, unique_visitors, total_visits, total_pageviews, created_at)
                        SELECT 
                            ?,
                            ?,
                            ?,
                            COUNT(DISTINCT visitor_id),
                            COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END),
                            COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END),
                            strftime('%s', 'now')
                        FROM events 
                        WHERE timestamp >= ? AND timestamp <= ?",
                        [
                            period_name,
                            &start_ts.to_string(),
                            &end_ts.to_string(),
                            &start_ts.to_string(),
                            &end_ts.to_string(),
                        ],
                    )?;
                }

                Ok(())
            })
            .await
    }

    async fn aggregate_active_period_metrics(&self) -> Result<(), tokio_rusqlite::Error> {
        let now = chrono::Utc::now();
        let thirty_minutes_ago = now - chrono::Duration::minutes(30);

        self.db
            .call(move |conn| {
                conn.execute(
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
                WHERE timestamp >= ? AND timestamp <= ?",
                params![
                    "realtime",
                    &thirty_minutes_ago.timestamp(),
                    &now.timestamp(),
                    &thirty_minutes_ago.timestamp(),
                    &now.timestamp(),
                ],
            )?;
            Ok(())
        }).await
    }

    async fn calculate_aggregates(&self, start_ts: i64, end_ts: i64) -> Result<AggregateMetrics, tokio_rusqlite::Error> {
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT 
                        COUNT(DISTINCT visitor_id) as unique_visitors,
                        COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END) as total_visits,
                        COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN id ELSE NULL END) as total_pageviews
                     FROM events 
                     WHERE timestamp >= ? AND timestamp <= ?"
                )?;
                
                let row = stmt.query_row([start_ts, end_ts], |row| {
                    Ok(AggregateMetrics {
                        unique_visitors: row.get(0)?,
                        total_visits: row.get(1)?,
                        total_pageviews: row.get(2)?,
                        current_visits: None,
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
    ) -> Result<Statistics, tokio_rusqlite::Error> {
        let now = Utc::now();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
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
                            "SELECT unique_visitors, total_visits, total_pageviews 
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
                            })
                        })?)
                    })
                    .await
            }
            None => self.calculate_aggregates(start_ts, end_ts).await,
        }?;


        let realtime_aggregates = self.get_realtime_aggregates().await?;

        Ok(Statistics { stats, aggregates, realtime_aggregates })
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
                    "SELECT period_start, unique_visitors, total_visits, total_pageviews
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
                    "SELECT unique_visitors, total_visits, total_pageviews, current_visits
                     FROM aggregated_metrics
                     WHERE period_name = 'realtime'"
                )?;

                Ok(stmt.query_row([], |row| {
                    Ok(AggregateMetrics {
                        unique_visitors: row.get(0)?,
                        total_visits: row.get(1)?,
                        total_pageviews: row.get(2)?,
                        current_visits: row.get(3)?,
                    })
                })?)
            })
        .await
    }
}

#[derive(serde::Deserialize)]
pub enum TimeFrame {
    Realtime,
    Today,
    Yesterday,
    Last7Days,
    Last30Days,
    AllTime,
}

#[derive(serde::Deserialize)]
pub enum Granularity {
    Minutes,
    Hours,
    Days,
}

#[derive(serde::Serialize)]
pub struct AggregateMetrics {
    pub unique_visitors: i64,
    pub total_visits: i64,
    pub total_pageviews: i64,
    pub current_visits: Option<i64>,
}

#[derive(serde::Serialize)]
pub struct Statistics {
    stats: Vec<(i64, i64)>,
    aggregates: AggregateMetrics,
    realtime_aggregates: AggregateMetrics,
}
