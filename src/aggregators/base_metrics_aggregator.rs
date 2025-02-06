use chrono::{Timelike, Utc};
use rusqlite::params;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::{debug, error};

use super::{Metric, TimeFrame};

pub trait MetricsOutput: Send {
    fn from_row(row: &rusqlite::Row) -> Result<Option<Self>, rusqlite::Error>
    where
        Self: Sized;
}

pub struct BaseMetricsAggregator {
    db: Arc<Connection>,
    aggregation_table_name: String,
    statistics_table_name: String,
    gathered_fields: Vec<(String, String)>,
    group_by_fields: Vec<String>,
}

impl BaseMetricsAggregator {
    pub fn new(
        db: Arc<Connection>,
        aggregation_table_name: String,
        statistics_table_name: String,
        gathered_fields: Vec<(String, String)>,
        group_by_fields: Vec<String>,
    ) -> Self {
        Self {
            db,
            aggregation_table_name,
            statistics_table_name,
            gathered_fields,
            group_by_fields,
        }
    }

    pub async fn remove_unused_active_aggregated_metrics(
        &self,
        thirty_minutes_ago_ts: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        let table_name = self.aggregation_table_name.clone();
        let query = format!(
            "DELETE FROM {} WHERE period_name = 'realtime' AND start_ts < ?",
            table_name
        );

        let query_clone = query.clone();
        let result = self
            .db
            .call(move |conn| {
                conn.execute(&query, params![thirty_minutes_ago_ts])
                    .map_err(tokio_rusqlite::Error::from)
            })
            .await;

        let query_string = query_clone.replace("?", &thirty_minutes_ago_ts.to_string());

        debug!(
            "Removing unused active aggregated metrics: {}",
            query_string
        );
        if let Err(ref _e) = result {
            error!("Failed SQL: {}", query_string);
        }
        result.map(|_| ())
    }

    pub async fn aggregate_stats_for_period(
        &self,
        period_type: &str,
        time_division: i64,
        start_timestamp: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        let period_type = period_type.to_string();
        let table_name = self.statistics_table_name.clone();
        let fields = self.gathered_fields.clone();
        let group_by = self.group_by_fields.join(", ");

        let fields_str = fields
            .iter()
            .map(|(_, expr)| expr.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let field_names = fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "INSERT OR REPLACE INTO {} (
                period_type, period_start, {}, 
                visitors, visits, pageviews, avg_visit_duration, bounce_rate, created_at
            )
            SELECT 
                ?1 as period_type,
                (timestamp / ?2) * ?2 as period_start,
                {},
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
            WHERE timestamp >= ?3 AND event_type = 'visit'
            GROUP BY period_start, {}",
            table_name, field_names, fields_str, group_by
        );

        let query_clone = query.clone();
        let period_type_clone = period_type.clone();

        let result = self
            .db
            .call(move |conn| {
                conn.execute(&query, params![period_type, time_division, start_timestamp])
                    .map_err(tokio_rusqlite::Error::from)
            })
            .await;

        let query_string = query_clone
            .replace("?1", &format!("'{}'", period_type_clone))
            .replace("?2", &time_division.to_string())
            .replace("?3", &start_timestamp.to_string());

        debug!("Aggregating stats for period: {}", query_string);

        if let Err(ref _e) = result {
            error!("Failed SQL: {}", query_string);
        }
        result.map(|_| ())
    }

    pub async fn aggregate_metrics_for_period(
        &self,
        period_name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<(), tokio_rusqlite::Error> {
        let period_name = period_name.to_string();
        let table_name = self.aggregation_table_name.clone();
        let fields = self.gathered_fields.clone();
        let group_by = self.group_by_fields.join(", ");

        let fields_str = fields
            .iter()
            .map(|(_, expr)| expr.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let field_names = fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "INSERT OR REPLACE INTO {} 
            (period_name, start_ts, end_ts, {}, visitors, visits, pageviews, avg_visit_duration, bounce_rate, created_at)
            SELECT 
                ?,
                ?,
                ?,
                {},
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
            WHERE timestamp >= ? AND timestamp <= ? AND event_type = 'visit'
            GROUP BY {}",
            table_name, field_names, fields_str, group_by
        );

        let query_clone = query.clone();
        let period_name_clone = period_name.clone();

        let result = self
            .db
            .call(move |conn| {
                conn.execute(
                    &query,
                    params![period_name, start_ts, end_ts, start_ts, end_ts],
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await;

        let query_string = query_clone
            .replacen("?", &format!("'{}'", period_name_clone), 1)
            .replacen("?", &start_ts.to_string(), 1)
            .replacen("?", &end_ts.to_string(), 1)
            .replacen("?", &start_ts.to_string(), 1)
            .replacen("?", &end_ts.to_string(), 1);

        debug!("Aggregating metrics for period: {}", query_string);

        if let Err(ref _e) = result {
            error!("Failed SQL: {}", query_string);
        }
        result.map(|_| ())
    }

    pub async fn get_metrics<T: MetricsOutput + 'static>(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        group_by_field: &str,
    ) -> Result<Vec<T>, tokio_rusqlite::Error> {
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
                today_start_ts - 7 * 86400,
                today_end_ts,
                Some("last_7_days"),
            ),
            TimeFrame::Last30Days => (
                today_start_ts - 30 * 86400,
                today_end_ts,
                Some("last_30_days"),
            ),
            TimeFrame::AllTime => (0, now.timestamp(), None),
        };

        let table_name = self.aggregation_table_name.clone();
        let fields = self.gathered_fields.clone();
        let group_by_field = group_by_field.to_string();

        let field_names = fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "SELECT 
                {},
                SUM(visitors) as visitors,
                SUM(visits) as visits,
                SUM(pageviews) as pageviews,
                CAST(AVG(avg_visit_duration) AS INTEGER) as avg_visit_duration,
                CAST(AVG(bounce_rate) AS INTEGER) as bounce_rate
             FROM {}
             WHERE period_name = ?
             AND start_ts >= ?
             AND end_ts <= ?
             GROUP BY {}
             ORDER BY {} {}",
            field_names, table_name, group_by_field, metric_str, order_direction
        );
        let query_clone = query.clone();

        let result = self
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;

                let result = stmt
                    .query_map(
                        params![period_name.unwrap_or(""), start_ts, end_ts],
                        |row| T::from_row(row),
                    )?
                    .filter_map(|r| r.transpose())
                    .collect::<Result<Vec<_>, rusqlite::Error>>()
                    .map_err(tokio_rusqlite::Error::from)?;
                Ok(result)
            })
            .await;

        let query_string = query_clone
            .replacen("?", &format!("'{}'", period_name.unwrap_or("")), 1)
            .replacen("?", &start_ts.to_string(), 1)
            .replacen("?", &end_ts.to_string(), 1);

        debug!("Getting metrics: {}", query_string);

        if let Err(ref _e) = result {
            error!("Failed SQL: {}", query_string);
        }
        result
    }
}
