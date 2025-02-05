use std::ops::Deref;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use super::base_metrics_aggregator::{BaseMetricsAggregator, MetricsOutput};
use super::{Metric, SourceGrouping, SourceMetrics, TimeFrame};

pub struct SourceMetricsAggregator {
    base: BaseMetricsAggregator,
}

impl Deref for SourceMetricsAggregator {
    type Target = BaseMetricsAggregator;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl SourceMetricsAggregator {
    pub fn new(db: Arc<Connection>) -> Self {
        let gathered_fields = vec![
            (
                "source".to_string(),
                "COALESCE(source, 'Direct') as source".to_string(),
            ),
            (
                "referrer".to_string(),
                "COALESCE(referrer, 'Unknown') as referrer".to_string(),
            ),
            (
                "utm_source".to_string(),
                "COALESCE(utm_source, 'Unknown') as utm_source".to_string(),
            ),
            (
                "utm_medium".to_string(),
                "COALESCE(utm_medium, 'Unknown') as utm_medium".to_string(),
            ),
            (
                "utm_campaign".to_string(),
                "COALESCE(utm_campaign, 'Unknown') as utm_campaign".to_string(),
            ),
        ];
        let group_by_fields = vec![
            "source".to_string(),
            "referrer".to_string(),
            "utm_source".to_string(),
            "utm_medium".to_string(),
            "utm_campaign".to_string(),
        ];

        Self {
            base: BaseMetricsAggregator::new(
                db,
                "source_aggregated_metrics".to_string(),
                "source_statistics".to_string(),
                gathered_fields,
                group_by_fields,
            ),
        }
    }

    pub async fn get_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: SourceGrouping,
    ) -> Result<Vec<SourceMetrics>, tokio_rusqlite::Error> {
        let group_by_field = match grouping {
            SourceGrouping::Source => "source",
            SourceGrouping::Referrer => "referrer",
            SourceGrouping::Campaign => "utm_source, utm_medium, utm_campaign",
        };

        self.base
            .get_metrics(timeframe, metric, group_by_field)
            .await
    }
}

impl MetricsOutput for SourceMetrics {
    fn from_row(row: &rusqlite::Row) -> Result<Option<Self>, rusqlite::Error> {
        let visitors: i64 = row.get(5)?;
        let visits: i64 = row.get(6)?;
        let pageviews: i64 = row.get(7)?;
        let avg_visit_duration: i64 = row.get::<_, Option<i64>>(8)?.unwrap_or(0);
        let bounce_rate: i64 = row.get::<_, Option<i64>>(9)?.unwrap_or(0);

        if visitors > 0 {
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
                bounce_rate,
            }))
        } else {
            Ok(None)
        }
    }
}
