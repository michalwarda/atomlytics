use super::base_metrics_aggregator::{BaseMetricsAggregator, MetricsOutput};
use super::{LocationGrouping, LocationMetrics, Metric, TimeFrame};
use std::ops::Deref;
use std::sync::Arc;
use tokio_rusqlite::Connection;

pub struct LocationMetricsAggregator {
    base: BaseMetricsAggregator,
}

impl Deref for LocationMetricsAggregator {
    type Target = BaseMetricsAggregator;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl LocationMetricsAggregator {
    pub fn new(db: Arc<Connection>) -> Self {
        let gathered_fields = vec![
            (
                "country".to_string(),
                "COALESCE(country, 'Unknown') as country".to_string(),
            ),
            (
                "region".to_string(),
                "COALESCE(region, 'Unknown') as region".to_string(),
            ),
            (
                "city".to_string(),
                "COALESCE(city, 'Unknown') as city".to_string(),
            ),
        ];
        let group_by_fields = vec![
            "country".to_string(),
            "region".to_string(),
            "city".to_string(),
        ];

        Self {
            base: BaseMetricsAggregator::new(
                db,
                "location_aggregated_metrics".to_string(),
                "location_statistics".to_string(),
                gathered_fields,
                group_by_fields,
            ),
        }
    }

    pub async fn get_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: LocationGrouping,
    ) -> Result<Vec<LocationMetrics>, tokio_rusqlite::Error> {
        let group_by_field = match grouping {
            LocationGrouping::Country => "country",
            LocationGrouping::Region => "region",
            LocationGrouping::City => "city",
        };

        self.base
            .get_metrics(timeframe, metric, group_by_field)
            .await
    }
}

impl MetricsOutput for LocationMetrics {
    fn from_row(row: &rusqlite::Row) -> Result<Option<Self>, rusqlite::Error> {
        let visitors: i64 = row.get(3)?;
        let visits: i64 = row.get(4)?;
        let pageviews: i64 = row.get(5)?;
        let avg_visit_duration: i64 = row.get::<_, Option<i64>>(6)?.unwrap_or(0);
        let bounce_rate: i64 = row.get::<_, Option<i64>>(7)?.unwrap_or(0);

        if visitors > 0 {
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
    }
}
