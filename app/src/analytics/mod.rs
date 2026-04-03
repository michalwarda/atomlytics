use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use rand::RngExt;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr,
    EntityTrait, QueryFilter, QueryOrder, Set, Statement, Value,
};
use sha2::{Digest, Sha256};
use tracing::error;
use url::Url;

use crate::{
    domain::{
        AggregateMetrics, DashboardQuery, DashboardResponse, DeviceGrouping, DeviceMetrics,
        DomainSummary, EnrichedEvent, FilterValuesResponse, LocationGrouping, LocationMetrics,
        Metric, PageGrouping, PageMetrics, SourceGrouping, SourceMetrics, TimeFrame,
    },
    entities::{daily_salts, events, visits},
};

#[derive(Clone)]
pub struct AnalyticsService {
    db: DatabaseConnection,
}

impl AnalyticsService {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn ensure_daily_salt(&self, day: NaiveDate) -> Result<String, DbErr> {
        if let Some(existing) = daily_salts::Entity::find_by_id(day).one(&self.db).await? {
            return Ok(existing.value);
        }

        let salt: String = rand::rng()
            .sample_iter(rand::distr::Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        daily_salts::ActiveModel {
            day: Set(day),
            value: Set(salt.clone()),
        }
        .insert(&self.db)
        .await?;

        let yesterday = day - chrono::Days::new(1);
        daily_salts::Entity::delete_many()
            .filter(daily_salts::Column::Day.lt(yesterday))
            .exec(&self.db)
            .await?;

        Ok(salt)
    }

    pub fn visitor_id(&self, salt: &str, domain: &str, ip: &str, user_agent: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{salt}{domain}{ip}{user_agent}"));
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub async fn find_active_visit(
        &self,
        domain: &str,
        visitor_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<visits::Model>, DbErr> {
        visits::Entity::find()
            .filter(visits::Column::Domain.eq(domain))
            .filter(visits::Column::VisitorId.eq(visitor_id))
            .filter(visits::Column::LastActivityAt.gte(now - Duration::minutes(30)))
            .order_by_desc(visits::Column::StartedAt)
            .one(&self.db)
            .await
    }

    pub async fn create_visit(&self, event: &EnrichedEvent) -> Result<i64, DbErr> {
        let visit = visits::ActiveModel {
            id: Default::default(),
            domain: Set(event.domain.clone()),
            visitor_id: Set(event.visitor_id.clone()),
            page_url: Set(event.payload.page_url.clone()),
            page_url_path: Set(event.page_url_path.clone()),
            referrer: Set(event.payload.referrer.clone()),
            source: Set(event.source.clone()),
            browser: Set(event.browser.clone()),
            operating_system: Set(event.operating_system.clone()),
            device_type: Set(event.device_type.clone()),
            country: Set(event.country.clone()),
            region: Set(event.region.clone()),
            city: Set(event.city.clone()),
            utm_source: Set(event.utm_source.clone()),
            utm_medium: Set(event.utm_medium.clone()),
            utm_campaign: Set(event.utm_campaign.clone()),
            utm_content: Set(event.utm_content.clone()),
            utm_term: Set(event.utm_term.clone()),
            started_at: Set(event.timestamp),
            last_activity_at: Set(event.timestamp),
            last_visited_url: Set(Some(event.payload.page_url.clone())),
            last_visited_url_path: Set(Some(event.page_url_path.clone())),
        }
        .insert(&self.db)
        .await?;

        Ok(visit.id)
    }

    pub async fn update_visit(&self, visit: &visits::Model, event: &EnrichedEvent) -> Result<(), DbErr> {
        let mut active_model: visits::ActiveModel = visit.clone().into();
        active_model.last_activity_at = Set(event.timestamp);
        active_model.last_visited_url = Set(Some(event.payload.page_url.clone()));
        active_model.last_visited_url_path = Set(Some(event.page_url_path.clone()));
        active_model.update(&self.db).await?;
        Ok(())
    }

    pub async fn save_event(&self, event: &EnrichedEvent) -> Result<(), DbErr> {
        events::ActiveModel {
            id: Default::default(),
            domain: Set(event.domain.clone()),
            event_type: Set(event.payload.event_type.clone()),
            page_url: Set(event.payload.page_url.clone()),
            page_url_path: Set(event.page_url_path.clone()),
            referrer: Set(event.payload.referrer.clone()),
            source: Set(event.source.clone()),
            browser: Set(event.browser.clone()),
            operating_system: Set(event.operating_system.clone()),
            device_type: Set(event.device_type.clone()),
            country: Set(event.country.clone()),
            region: Set(event.region.clone()),
            city: Set(event.city.clone()),
            utm_source: Set(event.utm_source.clone()),
            utm_medium: Set(event.utm_medium.clone()),
            utm_campaign: Set(event.utm_campaign.clone()),
            utm_content: Set(event.utm_content.clone()),
            utm_term: Set(event.utm_term.clone()),
            timestamp: Set(event.timestamp),
            visitor_id: Set(event.visitor_id.clone()),
            custom_params: Set(event.payload.custom_params.clone()),
            visit_id: Set(event.visit_id),
        }
        .insert(&self.db)
        .await?;
        Ok(())
    }

    pub async fn list_domains(&self) -> Result<Vec<DomainSummary>, DbErr> {
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                r#"
                SELECT
                    domain,
                    COUNT(*)::BIGINT AS total_visits,
                    COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '24 hours')::BIGINT AS visits_last_24h,
                    MAX(EXTRACT(EPOCH FROM last_activity_at))::BIGINT AS last_seen_at
                FROM visits
                GROUP BY domain
                ORDER BY MAX(last_activity_at) DESC
                "#,
                [],
            ))
            .await?;

        rows.into_iter()
            .map(|row| {
                Ok(DomainSummary {
                    domain: row.try_get("", "domain")?,
                    total_visits: row.try_get("", "total_visits")?,
                    visits_last_24h: row.try_get("", "visits_last_24h")?,
                    last_seen_at: row.try_get("", "last_seen_at").ok(),
                })
            })
            .collect()
    }

    pub async fn get_filter_values(
        &self,
        domain: &str,
        query: &DashboardQuery,
    ) -> Result<FilterValuesResponse, DbErr> {
        Ok(FilterValuesResponse {
            country: self
                .distinct_values(domain, query, "country", "filter_country")
                .await?,
            region: self
                .distinct_values(domain, query, "region", "filter_region")
                .await?,
            city: self
                .distinct_values(domain, query, "city", "filter_city")
                .await?,
            browser: self
                .distinct_values(domain, query, "browser", "filter_browser")
                .await?,
            operating_system: self
                .distinct_values(domain, query, "operating_system", "filter_os")
                .await?,
            device_type: self
                .distinct_values(domain, query, "device_type", "filter_device")
                .await?,
            page_url_path: self
                .distinct_values(domain, query, "page_url_path", "filter_page")
                .await?,
            source: self
                .distinct_values(domain, query, "source", "filter_source")
                .await?,
            referrer: self
                .distinct_values(domain, query, "referrer", "filter_referrer")
                .await?,
            utm_campaign: self
                .distinct_values(domain, query, "utm_campaign", "filter_campaign")
                .await?,
        })
    }

    pub async fn get_dashboard_data(
        &self,
        domain: &str,
        query: &DashboardQuery,
    ) -> Result<DashboardResponse, DbErr> {
        let (start_ts, end_ts) = resolve_timeframe(query.timeframe);
        let interval = resolve_interval(query.granularity);

        let stats = self
            .get_time_series(domain, query, start_ts, end_ts, interval)
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, "failed to load time series");
                error
            })?;
        let aggregates = self
            .aggregate_metrics(domain, query, start_ts, end_ts)
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, "failed to load aggregate metrics");
                error
            })?;
        let realtime_aggregates = self.realtime_metrics(domain, query).await.map_err(|error| {
            error!(?error, %domain, ?query, "failed to load realtime metrics");
            error
        })?;
        let location_metrics = self
            .location_metrics(domain, query, start_ts, end_ts)
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, "failed to load location metrics");
                error
            })?;
        let device_metrics = self
            .device_metrics(domain, query, start_ts, end_ts)
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, "failed to load device metrics");
                error
            })?;
        let source_metrics = self
            .source_metrics(domain, query, start_ts, end_ts)
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, "failed to load source metrics");
                error
            })?;
        let page_metrics = self
            .page_metrics(domain, query, start_ts, end_ts)
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, "failed to load page metrics");
                error
            })?;

        Ok(DashboardResponse {
            stats,
            aggregates,
            realtime_aggregates,
            location_metrics,
            device_metrics,
            source_metrics,
            page_metrics,
        })
    }

    async fn distinct_values(
        &self,
        domain: &str,
        query: &DashboardQuery,
        column: &str,
        skipped_filter: &str,
    ) -> Result<Vec<String>, DbErr> {
        let (start_ts, end_ts) = resolve_timeframe(query.timeframe);
        let (filter_sql, values) = build_visit_filters(query, 4, Some(skipped_filter));
        let sql = format!(
            r#"
            SELECT DISTINCT {column}
            FROM visits
            WHERE domain = $1
              AND EXTRACT(EPOCH FROM started_at) >= $2
              AND EXTRACT(EPOCH FROM started_at) <= $3
              AND {column} IS NOT NULL
              AND {column} <> ''
              AND {column} <> 'Unknown'
              {filter_sql}
            ORDER BY {column} ASC
            "#
        );

        let mut bind_values = vec![domain.into(), start_ts.into(), end_ts.into()];
        bind_values.extend(values);
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql,
                bind_values,
            ))
            .await?;

        rows.into_iter()
            .map(|row| row.try_get("", column))
            .collect::<Result<Vec<String>, _>>()
    }

    async fn get_time_series(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
        interval: i64,
    ) -> Result<Vec<(i64, i64)>, DbErr> {
        let metric_expr = metric_expression(query.metric);
        let (filter_sql, values) = build_visit_filters(query, 5, None);
        let sql = format!(
            r#"
            SELECT
                CAST(FLOOR(EXTRACT(EPOCH FROM e.timestamp) / $4) * $4 AS BIGINT) AS period_start,
                {metric_expr} AS metric_value
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.domain = $1
              AND EXTRACT(EPOCH FROM e.timestamp) >= $2
              AND EXTRACT(EPOCH FROM e.timestamp) <= $3
              {filter_sql}
            GROUP BY 1
            ORDER BY 1 ASC
            "#
        );

        let mut bind_values = vec![domain.into(), start_ts.into(), end_ts.into(), interval.into()];
        bind_values.extend(values);
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql.clone(),
                bind_values.clone(),
            ))
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, statement = %sql, bindings = ?bind_values, "time series query failed");
                error
            })?;

        let mut map = BTreeMap::new();
        for row in rows {
            let timestamp: i64 = row.try_get("", "period_start")?;
            let value: i64 = row.try_get("", "metric_value")?;
            map.insert(timestamp, value);
        }

        let mut buckets = Vec::new();
        let mut current = start_ts - (start_ts % interval);
        while current <= end_ts {
            buckets.push((current, map.get(&current).copied().unwrap_or_default()));
            current += interval;
        }
        Ok(buckets)
    }

    async fn aggregate_metrics(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<AggregateMetrics, DbErr> {
        self.aggregate_metrics_for_range(domain, query, start_ts, end_ts, None)
            .await
    }

    async fn realtime_metrics(
        &self,
        domain: &str,
        query: &DashboardQuery,
    ) -> Result<AggregateMetrics, DbErr> {
        let now = Utc::now().timestamp();
        let start = now - 30 * 60;
        let current_visits = self.current_visits(domain, query).await?;
        let mut metrics = self
            .aggregate_metrics_for_range(domain, query, start, now, Some(current_visits))
            .await?;
        metrics.current_visits = Some(current_visits);
        Ok(metrics)
    }

    async fn current_visits(&self, domain: &str, query: &DashboardQuery) -> Result<i64, DbErr> {
        let (filter_sql, values) = build_visit_filters(query, 2, None);
        let sql = format!(
            r#"
            SELECT COUNT(DISTINCT v.id)::BIGINT AS current_visits
            FROM visits v
            WHERE v.domain = $1
              AND v.last_activity_at >= NOW() - INTERVAL '30 minutes'
              {filter_sql}
            "#
        );
        let mut bind_values = vec![domain.into()];
        bind_values.extend(values);
        let row = self
            .db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql.clone(),
                bind_values.clone(),
            ))
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, statement = %sql, bindings = ?bind_values, "current visits query failed");
                error
            })?;
        Ok(row
            .and_then(|row| row.try_get("", "current_visits").ok())
            .unwrap_or_default())
    }

    async fn aggregate_metrics_for_range(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
        current_visits: Option<i64>,
    ) -> Result<AggregateMetrics, DbErr> {
        let (filter_sql, values) = build_visit_filters(query, 4, None);
        let sql = format!(
            r#"
            SELECT
                COUNT(DISTINCT e.visitor_id)::BIGINT AS unique_visitors,
                COUNT(DISTINCT v.id)::BIGINT AS total_visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id END)::BIGINT AS total_pageviews,
                COALESCE(CAST(AVG(EXTRACT(EPOCH FROM (v.last_activity_at - v.started_at))) AS BIGINT), 0) AS avg_visit_duration,
                COALESCE(
                    CAST(
                        COUNT(DISTINCT CASE WHEN v.started_at = v.last_activity_at THEN v.id END) * 100.0 /
                        NULLIF(COUNT(DISTINCT v.id), 0)
                        AS BIGINT
                    ),
                    0
                ) AS bounce_rate
            FROM events e
            LEFT JOIN visits v ON e.visit_id = v.id
            WHERE e.domain = $1
              AND EXTRACT(EPOCH FROM e.timestamp) >= $2
              AND EXTRACT(EPOCH FROM e.timestamp) <= $3
              {filter_sql}
            "#
        );
        let mut bind_values = vec![domain.into(), start_ts.into(), end_ts.into()];
        bind_values.extend(values);
        let row = self
            .db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql.clone(),
                bind_values.clone(),
            ))
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, statement = %sql, bindings = ?bind_values, "aggregate metrics query failed");
                error
            })?;

        if let Some(row) = row {
            Ok(AggregateMetrics {
                unique_visitors: row.try_get("", "unique_visitors")?,
                total_visits: row.try_get("", "total_visits")?,
                total_pageviews: row.try_get("", "total_pageviews")?,
                current_visits,
                avg_visit_duration: row.try_get("", "avg_visit_duration")?,
                bounce_rate: row.try_get("", "bounce_rate")?,
            })
        } else {
            Ok(AggregateMetrics {
                unique_visitors: 0,
                total_visits: 0,
                total_pageviews: 0,
                current_visits,
                avg_visit_duration: 0,
                bounce_rate: 0,
            })
        }
    }

    async fn location_metrics(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<LocationMetrics>, DbErr> {
        let select_value = match query.location_grouping {
            LocationGrouping::Country => ("COALESCE(v.country, 'Unknown')", "country"),
            LocationGrouping::Region => ("COALESCE(v.region, 'Unknown')", "region"),
            LocationGrouping::City => ("COALESCE(v.city, 'Unknown')", "city"),
        };
        let rows = self
            .breakdown_rows(
                domain,
                query,
                start_ts,
                end_ts,
                &format!("{} AS grouping_value", select_value.0),
                "visits v LEFT JOIN events e ON e.visit_id = v.id",
            )
            .await?;

        rows.into_iter()
            .map(|row| {
                let value: String = row.try_get("", "grouping_value")?;
                Ok(LocationMetrics {
                    country: if matches!(query.location_grouping, LocationGrouping::Country) {
                        value.clone()
                    } else {
                        String::new()
                    },
                    region: if matches!(query.location_grouping, LocationGrouping::Region) {
                        Some(value.clone())
                    } else {
                        None
                    },
                    city: if matches!(query.location_grouping, LocationGrouping::City) {
                        Some(value)
                    } else {
                        None
                    },
                    visitors: row.try_get("", "visitors")?,
                    visits: row.try_get("", "visits")?,
                    pageviews: row.try_get("", "pageviews")?,
                    avg_visit_duration: row.try_get("", "avg_visit_duration")?,
                    bounce_rate: row.try_get("", "bounce_rate")?,
                })
            })
            .collect()
    }

    async fn device_metrics(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<DeviceMetrics>, DbErr> {
        let select_value = match query.device_grouping {
            DeviceGrouping::Browser => "COALESCE(v.browser, 'Unknown')",
            DeviceGrouping::OperatingSystem => "COALESCE(v.operating_system, 'Unknown')",
            DeviceGrouping::DeviceType => "COALESCE(v.device_type, 'Unknown')",
        };
        let rows = self
            .breakdown_rows(
                domain,
                query,
                start_ts,
                end_ts,
                &format!("{select_value} AS grouping_value"),
                "visits v LEFT JOIN events e ON e.visit_id = v.id",
            )
            .await?;

        rows.into_iter()
            .map(|row| {
                let value: String = row.try_get("", "grouping_value")?;
                Ok(DeviceMetrics {
                    browser: if matches!(query.device_grouping, DeviceGrouping::Browser) {
                        value.clone()
                    } else {
                        String::new()
                    },
                    operating_system: if matches!(
                        query.device_grouping,
                        DeviceGrouping::OperatingSystem
                    ) {
                        value.clone()
                    } else {
                        String::new()
                    },
                    device_type: if matches!(query.device_grouping, DeviceGrouping::DeviceType) {
                        value
                    } else {
                        String::new()
                    },
                    visitors: row.try_get("", "visitors")?,
                    visits: row.try_get("", "visits")?,
                    pageviews: row.try_get("", "pageviews")?,
                    avg_visit_duration: row.try_get("", "avg_visit_duration")?,
                    bounce_rate: row.try_get("", "bounce_rate")?,
                })
            })
            .collect()
    }

    async fn source_metrics(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<SourceMetrics>, DbErr> {
        let select_value = match query.source_grouping {
            SourceGrouping::Source => (
                "COALESCE(v.source, 'Direct') AS grouping_value",
                "source",
                false,
            ),
            SourceGrouping::Referrer => (
                "COALESCE(v.referrer, 'Direct') AS grouping_value",
                "referrer",
                false,
            ),
            SourceGrouping::Campaign => (
                "COALESCE(v.utm_source, '-') || '/' || COALESCE(v.utm_medium, '-') || '/' || COALESCE(v.utm_campaign, '-') AS grouping_value",
                "campaign",
                true,
            ),
        };
        let rows = self
            .breakdown_rows(
                domain,
                query,
                start_ts,
                end_ts,
                select_value.0,
                "visits v LEFT JOIN events e ON e.visit_id = v.id",
            )
            .await?;

        rows.into_iter()
            .map(|row| {
                let value: String = row.try_get("", "grouping_value")?;
                Ok(SourceMetrics {
                    source: if matches!(query.source_grouping, SourceGrouping::Source) || select_value.2 {
                        value.clone()
                    } else {
                        String::new()
                    },
                    referrer: if matches!(query.source_grouping, SourceGrouping::Referrer) {
                        Some(value.clone())
                    } else {
                        None
                    },
                    utm_source: if select_value.2 {
                        Some(value.split('/').next().unwrap_or_default().to_string())
                    } else {
                        None
                    },
                    utm_medium: if select_value.2 {
                        Some(value.split('/').nth(1).unwrap_or_default().to_string())
                    } else {
                        None
                    },
                    utm_campaign: if select_value.2 {
                        Some(value.split('/').nth(2).unwrap_or_default().to_string())
                    } else {
                        None
                    },
                    visitors: row.try_get("", "visitors")?,
                    visits: row.try_get("", "visits")?,
                    pageviews: row.try_get("", "pageviews")?,
                    avg_visit_duration: row.try_get("", "avg_visit_duration")?,
                    bounce_rate: row.try_get("", "bounce_rate")?,
                })
            })
            .collect()
    }

    async fn page_metrics(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<PageMetrics>, DbErr> {
        let (group_expr, from_clause) = match query.page_grouping {
            PageGrouping::Page => (
                "COALESCE(e.page_url_path, '/') AS grouping_value",
                "events e LEFT JOIN visits v ON e.visit_id = v.id",
            ),
            PageGrouping::EntryPage => (
                "COALESCE(v.page_url_path, '/') AS grouping_value",
                "visits v LEFT JOIN events e ON e.visit_id = v.id",
            ),
            PageGrouping::ExitPage => (
                "COALESCE(v.last_visited_url_path, '/') AS grouping_value",
                "visits v LEFT JOIN events e ON e.visit_id = v.id",
            ),
        };
        let rows = self
            .breakdown_rows(domain, query, start_ts, end_ts, group_expr, from_clause)
            .await?;

        rows.into_iter()
            .map(|row| {
                let value: String = row.try_get("", "grouping_value")?;
                Ok(PageMetrics {
                    page_path: if matches!(query.page_grouping, PageGrouping::Page) {
                        value.clone()
                    } else {
                        String::new()
                    },
                    entry_page_path: if matches!(query.page_grouping, PageGrouping::EntryPage) {
                        value.clone()
                    } else {
                        String::new()
                    },
                    exit_page_path: if matches!(query.page_grouping, PageGrouping::ExitPage) {
                        value
                    } else {
                        String::new()
                    },
                    visitors: row.try_get("", "visitors")?,
                    visits: row.try_get("", "visits")?,
                    pageviews: row.try_get("", "pageviews")?,
                    avg_visit_duration: row.try_get("", "avg_visit_duration")?,
                    bounce_rate: row.try_get("", "bounce_rate")?,
                })
            })
            .collect()
    }

    async fn breakdown_rows(
        &self,
        domain: &str,
        query: &DashboardQuery,
        start_ts: i64,
        end_ts: i64,
        grouping_expression: &str,
        from_clause: &str,
    ) -> Result<Vec<sea_orm::QueryResult>, DbErr> {
        let sort_column = metric_sort_column(query.metric);
        let (filter_sql, values) = build_visit_filters(query, 4, None);
        let sql = format!(
            r#"
            SELECT
                {grouping_expression},
                COUNT(DISTINCT COALESCE(v.visitor_id, e.visitor_id))::BIGINT AS visitors,
                COUNT(DISTINCT v.id)::BIGINT AS visits,
                COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id END)::BIGINT AS pageviews,
                COALESCE(CAST(AVG(EXTRACT(EPOCH FROM (v.last_activity_at - v.started_at))) AS BIGINT), 0) AS avg_visit_duration,
                COALESCE(
                    CAST(
                        COUNT(DISTINCT CASE WHEN v.started_at = v.last_activity_at THEN v.id END) * 100.0 /
                        NULLIF(COUNT(DISTINCT v.id), 0)
                        AS BIGINT
                    ),
                    0
                ) AS bounce_rate
            FROM {from_clause}
            WHERE COALESCE(v.domain, e.domain) = $1
              AND EXTRACT(EPOCH FROM COALESCE(e.timestamp, v.started_at)) >= $2
              AND EXTRACT(EPOCH FROM COALESCE(e.timestamp, v.started_at)) <= $3
              {filter_sql}
            GROUP BY 1
            ORDER BY {sort_column} DESC, grouping_value ASC
            LIMIT 50
            "#
        );

        let mut bind_values = vec![domain.into(), start_ts.into(), end_ts.into()];
        bind_values.extend(values);

        self.db
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                sql.clone(),
                bind_values.clone(),
            ))
            .await
            .map_err(|error| {
                error!(?error, %domain, ?query, statement = %sql, bindings = ?bind_values, "breakdown query failed");
                error
            })
    }
}

pub fn extract_domain(page_url: &str) -> Option<String> {
    Url::parse(page_url)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
}

pub fn extract_path(page_url: &str) -> String {
    Url::parse(page_url)
        .ok()
        .map(|url| {
            let path = url.path().to_string();
            if path.is_empty() {
                "/".to_string()
            } else {
                path
            }
        })
        .unwrap_or_else(|| "/".to_string())
}

pub fn extract_utm_params(page_url: &str) -> (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>) {
    let Ok(url) = Url::parse(page_url) else {
        return (None, None, None, None, None);
    };
    let params = url.query_pairs().collect::<BTreeMap<_, _>>();
    (
        params.get("utm_source").map(ToString::to_string),
        params.get("utm_medium").map(ToString::to_string),
        params.get("utm_campaign").map(ToString::to_string),
        params.get("utm_content").map(ToString::to_string),
        params.get("utm_term").map(ToString::to_string),
    )
}

pub fn calculate_source(referrer: &Option<String>, utm_source: &Option<String>) -> String {
    if let Some(utm_source) = utm_source.as_ref().filter(|value| !value.is_empty()) {
        return utm_source.clone();
    }

    let Some(referrer) = referrer else {
        return "Direct".to_string();
    };

    let Ok(url) = Url::parse(referrer) else {
        return "Direct".to_string();
    };

    let Some(host) = url.host_str() else {
        return "Direct".to_string();
    };

    let domain = host.trim_start_matches("www.");
    match domain {
        "google.com" | "google.co.uk" | "google.fr" => "Google".to_string(),
        "facebook.com" => "Facebook".to_string(),
        "twitter.com" | "t.co" => "Twitter".to_string(),
        "linkedin.com" | "lnkd.in" => "LinkedIn".to_string(),
        "instagram.com" => "Instagram".to_string(),
        "bing.com" => "Bing".to_string(),
        "yahoo.com" => "Yahoo".to_string(),
        _ => domain.to_string(),
    }
}

fn resolve_timeframe(timeframe: TimeFrame) -> (i64, i64) {
    let now = Utc::now();
    let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc();
    let today_end = today_start + Duration::days(1);

    match timeframe {
        TimeFrame::Realtime => (now.timestamp() - 30 * 60, now.timestamp()),
        TimeFrame::Today => (today_start.timestamp(), today_end.timestamp()),
        TimeFrame::Yesterday => (
            (today_start - Duration::days(1)).timestamp(),
            today_start.timestamp(),
        ),
        TimeFrame::Last7Days => ((today_start - Duration::days(7)).timestamp(), today_end.timestamp()),
        TimeFrame::Last30Days => ((today_start - Duration::days(30)).timestamp(), today_end.timestamp()),
        TimeFrame::AllTime => (0, now.timestamp()),
    }
}

fn resolve_interval(granularity: crate::domain::Granularity) -> i64 {
    match granularity {
        crate::domain::Granularity::Minutes => 60,
        crate::domain::Granularity::Hours => 3600,
        crate::domain::Granularity::Days => 86_400,
    }
}

fn metric_expression(metric: Metric) -> &'static str {
    match metric {
        Metric::UniqueVisitors => "COUNT(DISTINCT e.visitor_id)::BIGINT",
        Metric::Visits => "COUNT(DISTINCT v.id)::BIGINT",
        Metric::Pageviews => "COUNT(DISTINCT CASE WHEN e.event_type = 'pageview' THEN e.id END)::BIGINT",
        Metric::AvgVisitDuration => "COALESCE(CAST(AVG(EXTRACT(EPOCH FROM (v.last_activity_at - v.started_at))) AS BIGINT), 0)",
        Metric::BounceRate => "COALESCE(CAST(COUNT(DISTINCT CASE WHEN v.started_at = v.last_activity_at THEN v.id END) * 100.0 / NULLIF(COUNT(DISTINCT v.id), 0) AS BIGINT), 0)",
    }
}

fn metric_sort_column(metric: Metric) -> &'static str {
    match metric {
        Metric::UniqueVisitors => "visitors",
        Metric::Visits => "visits",
        Metric::Pageviews => "pageviews",
        Metric::AvgVisitDuration => "avg_visit_duration",
        Metric::BounceRate => "bounce_rate",
    }
}

fn build_visit_filters(
    query: &DashboardQuery,
    starting_index: usize,
    skipped_filter: Option<&str>,
) -> (String, Vec<Value>) {
    let mut sql = String::new();
    let mut values = Vec::new();

    let mut push = |filter_key: &str, column: &str, include: &[String], exclude: &[String]| {
        if skipped_filter == Some(filter_key) {
            return;
        }

        let include = include
            .iter()
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .collect::<Vec<_>>();
        let exclude = exclude
            .iter()
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .collect::<Vec<_>>();
        let value_expr = format!("COALESCE(v.{column}, 'Unknown')");

        if !include.is_empty() {
            let placeholders = include
                .into_iter()
                .map(|value| {
                    let idx = starting_index + values.len();
                    values.push(value.into());
                    format!("${idx}")
                })
                .collect::<Vec<_>>();
            sql.push_str(&format!(
                " AND {value_expr} IN ({})",
                placeholders.join(", ")
            ));
        }

        if !exclude.is_empty() {
            let placeholders = exclude
                .into_iter()
                .map(|value| {
                    let idx = starting_index + values.len();
                    values.push(value.into());
                    format!("${idx}")
                })
                .collect::<Vec<_>>();
            sql.push_str(&format!(
                " AND {value_expr} NOT IN ({})",
                placeholders.join(", ")
            ));
        }
    };

    push(
        "filter_country",
        "country",
        &query.filter_country,
        &query.filter_country_not,
    );
    push(
        "filter_region",
        "region",
        &query.filter_region,
        &query.filter_region_not,
    );
    push("filter_city", "city", &query.filter_city, &query.filter_city_not);
    push(
        "filter_browser",
        "browser",
        &query.filter_browser,
        &query.filter_browser_not,
    );
    push(
        "filter_os",
        "operating_system",
        &query.filter_os,
        &query.filter_os_not,
    );
    push(
        "filter_device",
        "device_type",
        &query.filter_device,
        &query.filter_device_not,
    );
    push(
        "filter_page",
        "page_url_path",
        &query.filter_page,
        &query.filter_page_not,
    );
    push(
        "filter_source",
        "source",
        &query.filter_source,
        &query.filter_source_not,
    );
    push(
        "filter_referrer",
        "referrer",
        &query.filter_referrer,
        &query.filter_referrer_not,
    );
    push(
        "filter_campaign",
        "utm_campaign",
        &query.filter_campaign,
        &query.filter_campaign_not,
    );

    (sql, values)
}

#[cfg(test)]
mod tests {
    use super::{calculate_source, extract_domain, extract_path, extract_utm_params};

    #[test]
    fn extracts_domain_and_path() {
        assert_eq!(extract_domain("https://example.com/foo?bar=baz").as_deref(), Some("example.com"));
        assert_eq!(extract_path("https://example.com/foo?bar=baz"), "/foo");
    }

    #[test]
    fn extracts_utm_values() {
        let (source, medium, campaign, content, term) = extract_utm_params(
            "https://example.com/?utm_source=news&utm_medium=email&utm_campaign=launch&utm_content=hero&utm_term=rust",
        );
        assert_eq!(source.as_deref(), Some("news"));
        assert_eq!(medium.as_deref(), Some("email"));
        assert_eq!(campaign.as_deref(), Some("launch"));
        assert_eq!(content.as_deref(), Some("hero"));
        assert_eq!(term.as_deref(), Some("rust"));
    }

    #[test]
    fn prefers_utm_source_when_present() {
        assert_eq!(
            calculate_source(&Some("https://google.com".to_string()), &Some("newsletter".to_string())),
            "newsletter"
        );
        assert_eq!(
            calculate_source(&Some("https://www.google.com/search?q=atom".to_string()), &None),
            "Google"
        );
    }
}
