use std::sync::Arc;

use chrono::{Timelike, Utc};
use rusqlite::params;
use tokio_rusqlite::Connection;

use super::{LocationGrouping, LocationMetrics, Metric, TimeFrame};

pub struct LocationMetricsAggregator {
    db: Arc<Connection>,
}

impl LocationMetricsAggregator {
    pub fn new(db: Arc<Connection>) -> Self {
        Self { db }
    }

    pub async fn remove_unused_active_aggregated_metrics(
        &self,
        thirty_minutes_ago_ts: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        self.db.call(move |conn| {
            conn.execute("DELETE FROM location_aggregated_metrics WHERE period_name = 'realtime' AND start_ts < ?", params![thirty_minutes_ago_ts])?;
            Ok(())
        }).await
    }

    pub async fn aggregate_stats_for_period(
        &self,
        period_type: &str,
        time_division: i64,
        start_timestamp: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        let period_type = period_type.to_string();

        self.db.call(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO location_statistics (
                    period_type, period_start, country, region, city, 
                    visitors, visits, pageviews, avg_visit_duration, bounce_rate, created_at
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
                    CAST(
                        CAST(COUNT(DISTINCT CASE WHEN event_type = 'visit' AND timestamp = last_activity_at THEN id ELSE NULL END) AS FLOAT) /
                        CAST(NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END), 0) AS FLOAT) * 100.0
                    AS FLOAT) as bounce_rate,
                    strftime('%s', 'now') as created_at
                FROM events
                WHERE timestamp >= ?3 AND country IS NOT NULL
                GROUP BY period_start, country, region, city",
                params![period_type, time_division, start_timestamp],
            )?;
            Ok(())
        }).await
    }

    pub async fn aggregate_metrics_for_period(
        &self,
        period_name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        let period_name = period_name.to_string();
        self.db.call(move |conn| {
                // Location metrics
                conn.execute(
                    "INSERT OR REPLACE INTO location_aggregated_metrics 
                    (period_name, start_ts, end_ts, country, region, city, visitors, visits, pageviews, avg_visit_duration, bounce_rate, created_at)
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
                        CAST(
                            CAST(COUNT(DISTINCT CASE WHEN event_type = 'visit' AND timestamp = last_activity_at THEN id ELSE NULL END) AS FLOAT) /
                            CAST(NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'visit' THEN id ELSE NULL END), 0) AS FLOAT) * 100.0
                        AS FLOAT) as bounce_rate,
                        strftime('%s', 'now') as created_at
                    FROM events 
                    WHERE timestamp >= ? AND timestamp <= ? AND country IS NOT NULL
                    GROUP BY country, region, city",
                    params![period_name, start_ts, end_ts, start_ts, end_ts],
                )?;
                Ok(())
        }).await
    }

    pub async fn get_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: LocationGrouping,
    ) -> Result<Vec<LocationMetrics>, tokio_rusqlite::Error> {
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
            Metric::BounceRate => "bounce_rate",
        };
        let order_direction = match metric {
            Metric::BounceRate => "ASC",
            _ => "DESC",
        };

        let (start_ts, end_ts, period_name) = match timeframe {
            TimeFrame::Realtime => (now.timestamp() - 30 * 60, now.timestamp(), Some("realtime")),
            TimeFrame::Today => (today_start_ts, today_end_ts, Some("today")),
            TimeFrame::Yesterday => (today_start_ts - 86400, today_start_ts, Some("yesterday")),
            TimeFrame::Last7Days => (
                now.timestamp() - 7 * 86400,
                now.timestamp(),
                Some("last_7_days"),
            ),
            TimeFrame::Last30Days => (
                now.timestamp() - 30 * 86400,
                now.timestamp(),
                Some("last_30_days"),
            ),
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
                        CAST(AVG(avg_visit_duration) AS INTEGER) as avg_visit_duration,
                        CAST(AVG(bounce_rate) AS INTEGER) as bounce_rate
                     FROM location_aggregated_metrics
                     WHERE period_name = ?
                     AND start_ts >= ?
                     AND end_ts <= ?
                     GROUP BY {}
                     ORDER BY {} {}",
                    group_by_clause, metric_str, order_direction
                );

                let mut stmt = conn.prepare(&query)?;

                // TODO: Handle AllTime
                let metrics = stmt
                    .query_map(
                        params![period_name.unwrap_or(""), start_ts, end_ts],
                        |row| {
                            let visitors: i64 = row.get(3)?;
                            let visits: i64 = row.get(4)?;
                            let pageviews: i64 = row.get(5)?;
                            let avg_visit_duration: i64 =
                                row.get::<_, Option<i64>>(6)?.unwrap_or(0);
                            let bounce_rate: i64 = row.get::<_, Option<i64>>(7)?.unwrap_or(0);

                            let value = match metric_str {
                                "visitors" => visitors,
                                "visits" => visits,
                                "pageviews" => pageviews,
                                "avg_visit_duration" => avg_visit_duration,
                                "bounce_rate" => bounce_rate,
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
                                    bounce_rate,
                                }))
                            } else {
                                Ok(None)
                            }
                        },
                    )?
                    .filter_map(|r| r.transpose())
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(metrics)
            })
            .await
    }
}
