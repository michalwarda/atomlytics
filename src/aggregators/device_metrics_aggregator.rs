use std::ops::Deref;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use super::base_metrics_aggregator::{BaseMetricsAggregator, MetricsOutput};
use super::{DeviceGrouping, DeviceMetrics, Metric, TimeFrame};

pub struct DeviceMetricsAggregator {
    base: BaseMetricsAggregator,
}

impl Deref for DeviceMetricsAggregator {
    type Target = BaseMetricsAggregator;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DeviceMetricsAggregator {
    pub fn new(db: Arc<Connection>) -> Self {
        let gathered_fields = vec![
            (
                "browser".to_string(),
                "COALESCE(browser, 'Unknown') as browser".to_string(),
            ),
            (
                "operating_system".to_string(),
                "COALESCE(operating_system, 'Unknown') as operating_system".to_string(),
            ),
            (
                "device_type".to_string(),
                "COALESCE(device_type, 'Unknown') as device_type".to_string(),
            ),
        ];
        let group_by_fields = vec![
            "browser".to_string(),
            "operating_system".to_string(),
            "device_type".to_string(),
        ];

        Self {
            base: BaseMetricsAggregator::new(
                db,
                "device_aggregated_metrics".to_string(),
                "device_statistics".to_string(),
                gathered_fields,
                group_by_fields,
            ),
        }
    }

    pub async fn get_metrics(
        &self,
        timeframe: &TimeFrame,
        metric: &Metric,
        grouping: DeviceGrouping,
    ) -> Result<Vec<DeviceMetrics>, tokio_rusqlite::Error> {
        let group_by_field = match grouping {
            DeviceGrouping::Browser => "browser",
            DeviceGrouping::OperatingSystem => "operating_system",
            DeviceGrouping::DeviceType => "device_type",
        };

        self.base
            .get_metrics(timeframe, metric, group_by_field)
            .await
    }
}

impl MetricsOutput for DeviceMetrics {
    fn from_row(row: &rusqlite::Row) -> Result<Option<Self>, rusqlite::Error> {
        let visitors: i64 = row.get(3)?;
        let visits: i64 = row.get(4)?;
        let pageviews: i64 = row.get(5)?;
        let avg_visit_duration: i64 = row.get::<_, Option<i64>>(6)?.unwrap_or(0);
        let bounce_rate: i64 = row.get::<_, Option<i64>>(7)?.unwrap_or(0);

        if visitors > 0 {
            Ok(Some(DeviceMetrics {
                browser: row.get(0)?,
                operating_system: row.get(1)?,
                device_type: row.get(2)?,
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
