use std::ops::Deref;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use super::base_metrics_aggregator::{BaseMetricsAggregator, MetricsOutput};
use super::{Metric, PageGrouping, PageMetrics, TimeFrame};

pub struct PageMetricsAggregator {
    base: BaseMetricsAggregator,
}

impl Deref for PageMetricsAggregator {
    type Target = BaseMetricsAggregator;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl PageMetricsAggregator {
    pub fn new(db: Arc<Connection>) -> Self {
        let gathered_fields = vec![
            (
                "page_path".to_string(),
                "CASE WHEN event_type = 'visit' THEN page_url_path END as page_path".to_string(),
            ),
            (
                "entry_page_path".to_string(),
                "CASE WHEN event_type = 'visit' THEN page_url_path END as entry_page_path"
                    .to_string(),
            ),
            (
                "exit_page_path".to_string(),
                "CASE WHEN event_type = 'visit' THEN last_visited_url_path END as exit_page_path"
                    .to_string(),
            ),
        ];
        let group_by_fields = vec![
            "page_path".to_string(),
            "entry_page_path".to_string(),
            "exit_page_path".to_string(),
        ];

        Self {
            base: BaseMetricsAggregator::new(
                db,
                "page_aggregated_metrics".to_string(),
                "page_statistics".to_string(),
                gathered_fields,
                group_by_fields,
            ),
        }
    }

    pub async fn get_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: PageGrouping,
    ) -> Result<Vec<PageMetrics>, tokio_rusqlite::Error> {
        let group_by_field = match grouping {
            PageGrouping::Page => "page_path",
            PageGrouping::EntryPage => "entry_page_path",
            PageGrouping::ExitPage => "exit_page_path",
        };

        self.base
            .get_metrics(timeframe, metric, group_by_field)
            .await
    }
}

impl MetricsOutput for PageMetrics {
    fn from_row(row: &rusqlite::Row) -> Result<Option<Self>, rusqlite::Error> {
        let visitors: i64 = row.get(3)?;
        let visits: i64 = row.get(4)?;
        let pageviews: i64 = row.get(5)?;
        let avg_visit_duration: i64 = row.get::<_, Option<i64>>(6)?.unwrap_or(0);
        let bounce_rate: i64 = row.get::<_, Option<i64>>(7)?.unwrap_or(0);

        if visitors > 0 {
            Ok(Some(PageMetrics {
                page_path: row.get(0)?,
                entry_page_path: row.get(1)?,
                exit_page_path: row.get(2)?,
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
