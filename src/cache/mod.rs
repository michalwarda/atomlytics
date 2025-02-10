use std::sync::Arc;
use tokio_rusqlite::Connection;

pub async fn refresh_filter_values_cache(db: Arc<Connection>) {
    if let Err(e) = db.call(|conn| -> Result<_, tokio_rusqlite::Error> {
        conn.execute_batch(
            "INSERT OR REPLACE INTO filter_values_cache (id, country, region, city, browser, operating_system, device_type, page_url_path, source, referrer, utm_campaign)
             SELECT 1,
                    json_group_array(DISTINCT country) FILTER (WHERE country IS NOT NULL),
                    json_group_array(DISTINCT region) FILTER (WHERE region IS NOT NULL),
                    json_group_array(DISTINCT city) FILTER (WHERE city IS NOT NULL),
                    json_group_array(DISTINCT browser) FILTER (WHERE browser IS NOT NULL),
                    json_group_array(DISTINCT operating_system) FILTER (WHERE operating_system IS NOT NULL),
                    json_group_array(DISTINCT device_type) FILTER (WHERE device_type IS NOT NULL),
                    json_group_array(DISTINCT page_url_path) FILTER (WHERE page_url_path IS NOT NULL),
                    json_group_array(DISTINCT source) FILTER (WHERE source IS NOT NULL),
                    json_group_array(DISTINCT referrer) FILTER (WHERE referrer IS NOT NULL),
                    json_group_array(DISTINCT utm_campaign) FILTER (WHERE utm_campaign IS NOT NULL)
             FROM events"
        ).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?;
        Ok(())
    }).await {
        tracing::error!("Failed to refresh filter values cache: {}", e);
    }
}
