use anyhow::Result;
use rusqlite::params;
use tokio_rusqlite::Connection;
use tracing::info;

// First, let's define a Migration struct and related types
#[derive(Debug)]
struct Migration {
    name: &'static str,
    version: i32,
    up: fn(&rusqlite::Connection) -> rusqlite::Result<()>,
}

impl Migration {
    fn new(
        name: &'static str,
        version: i32,
        up: fn(&rusqlite::Connection) -> rusqlite::Result<()>,
    ) -> Self {
        Self { name, version, up }
    }
}

pub async fn initialize_database(db: &Connection) -> Result<()> {
    db.call(|conn| {
        // Create events table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                page_url TEXT NOT NULL,
                referrer TEXT,
                browser TEXT NOT NULL,
                operating_system TEXT NOT NULL,
                device_type TEXT NOT NULL,
                country TEXT,
                region TEXT,
                city TEXT,
                utm_source TEXT,
                utm_medium TEXT,
                utm_campaign TEXT,
                utm_content TEXT,
                utm_term TEXT,
                timestamp INTEGER NOT NULL,
                visitor_id TEXT NOT NULL,
                custom_params TEXT
            )",
            [],
        )?;

        // Create statistics table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY,
                period_type TEXT NOT NULL,  -- 'minute' or 'hour'
                period_start INTEGER NOT NULL,  -- timestamp of period start
                unique_visitors INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(period_type, period_start)
            )",
            [],
        )?;

        // Create aggregated metrics table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS aggregated_metrics (
                period_name TEXT PRIMARY KEY,
                start_ts INTEGER,
                end_ts INTEGER,
                unique_visitors INTEGER,
                total_visits INTEGER,
                total_pageviews INTEGER,
                created_at INTEGER
            )",
            [],
        )?;

        Ok(())
    })
    .await?;

    run_migrations(db).await?;

    Ok(())
}

// Create a vector of all migrations
fn get_migrations() -> Vec<Migration> {
    vec![
        Migration::new("Add bounce rate to aggregated metrics", 1, |conn| {
            conn.execute(
                "ALTER TABLE statistics ADD COLUMN total_visits INTEGER NOT NULL DEFAULT 0;",
                [],
            )?;
            conn.execute(
                "ALTER TABLE statistics ADD COLUMN total_pageviews INTEGER NOT NULL DEFAULT 0;",
                [],
            )?;
            Ok(())
        }),
        Migration::new("Add optimal indices", 2, |conn| {
            // Index for events table - frequently used in aggregation queries
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_visitor_event ON events(visitor_id, event_type)",
                [],
            )?;

            // Index for statistics table - used in time series queries
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_statistics_period ON statistics(period_type, period_start)",
                [],
            )?;

            // Index for aggregated_metrics table - used in dashboard queries
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_period ON aggregated_metrics(period_name, start_ts, end_ts)",
                [],
            )?;

            Ok(())
        }),
        Migration::new("Add salt table", 3, |conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS salt (
                    day INTEGER PRIMARY KEY,
                    value TEXT NOT NULL
                )",
                [],
            )?;
            Ok(())
        }),
        Migration::new("Add last_activity_at to events", 4, |conn| {
            conn.execute(
                "ALTER TABLE events ADD COLUMN last_activity_at INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
            Ok(())
        }),
        Migration::new("Add is_active to events", 5, |conn| {
            conn.execute("ALTER TABLE events ADD COLUMN is_active INTEGER", [])?;
            Ok(())
        }),
        Migration::new("Add realtime stats", 6, |conn| {
            conn.execute(
                "ALTER TABLE aggregated_metrics ADD COLUMN current_visits INTEGER",
                [],
            )?;
            Ok(())
        }),
        Migration::new("Add country statistics tables", 7, |conn| {
            // Create country statistics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS country_statistics (
                    id INTEGER PRIMARY KEY,
                    period_type TEXT NOT NULL,
                    period_start INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_type, period_start, country)
                )",
                [],
            )?;

            // Create country aggregated metrics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS country_aggregated_metrics (
                    id INTEGER PRIMARY KEY,
                    period_name TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_name, country)
                )",
                [],
            )?;

            // Add indices for better query performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_country_stats_period 
                 ON country_statistics(period_type, period_start)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_country_metrics_period 
                 ON country_aggregated_metrics(period_name)",
                [],
            )?;

            Ok(())
        }),
        Migration::new(
            "Add default values to events country, region, city",
            8,
            |conn| {
                conn.execute(
                    "ALTER TABLE events RENAME COLUMN country TO country_old",
                    [],
                )?;
                conn.execute("ALTER TABLE events RENAME COLUMN region TO region_old", [])?;
                conn.execute("ALTER TABLE events RENAME COLUMN city TO city_old", [])?;
                conn.execute(
                    "ALTER TABLE events ADD COLUMN country TEXT DEFAULT 'Unknown'",
                    [],
                )?;
                conn.execute(
                    "ALTER TABLE events ADD COLUMN region TEXT DEFAULT 'Unknown'",
                    [],
                )?;
                conn.execute(
                    "ALTER TABLE events ADD COLUMN city TEXT DEFAULT 'Unknown'",
                    [],
                )?;
                conn.execute(
                    "UPDATE events SET country = COALESCE(country_old, 'Unknown'), region = COALESCE(region_old, 'Unknown'), city = COALESCE(city_old, 'Unknown')",
                    [],
                )?;
                conn.execute("ALTER TABLE events DROP COLUMN country_old", [])?;
                conn.execute("ALTER TABLE events DROP COLUMN region_old", [])?;
                conn.execute("ALTER TABLE events DROP COLUMN city_old", [])?;
                Ok(())
            },
        ),
        Migration::new("Add location statistics tables", 9, |conn| {
            // Create location statistics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS location_statistics (
                    id INTEGER PRIMARY KEY,
                    period_type TEXT NOT NULL,
                    period_start INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    region TEXT,
                    city TEXT,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_type, period_start, country, region, city)
                )",
                [],
            )?;

            // Create location aggregated metrics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS location_aggregated_metrics (
                    id INTEGER PRIMARY KEY,
                    period_name TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    region TEXT,
                    city TEXT,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_name, country, region, city)
                )",
                [],
            )?;

            // Add indices for better query performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_location_stats_period 
                 ON location_statistics(period_type, period_start)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_location_metrics_period 
                 ON location_aggregated_metrics(period_name)",
                [],
            )?;

            // Migrate existing data from country tables to location tables
            conn.execute(
                "INSERT INTO location_statistics 
                 SELECT 
                    id,
                    period_type,
                    period_start,
                    country,
                    NULL as region,
                    NULL as city,
                    visitors,
                    visits,
                    pageviews,
                    created_at
                 FROM country_statistics",
                [],
            )?;

            // Drop old country tables
            conn.execute("DROP TABLE IF EXISTS country_statistics", [])?;
            conn.execute("DROP TABLE IF EXISTS country_aggregated_metrics", [])?;

            Ok(())
        }),
        Migration::new("Add device statistics tables", 10, |conn| {
            // Create device statistics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS device_statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_type TEXT NOT NULL,
                    period_start INTEGER NOT NULL,
                    browser TEXT NOT NULL,
                    operating_system TEXT NOT NULL, 
                    device_type TEXT NOT NULL,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_type, period_start, browser, operating_system, device_type)
                )",
                [],
            )?;

            // Create device aggregated metrics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS device_aggregated_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_name TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL,
                    browser TEXT NOT NULL,
                    operating_system TEXT NOT NULL,
                    device_type TEXT NOT NULL,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_name, start_ts, end_ts, browser, operating_system, device_type)
                )",
                [],
            )?;

            // Add indices for better query performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_device_stats_period 
                 ON device_statistics(period_type, period_start)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_device_metrics_period 
                 ON device_aggregated_metrics(period_name)",
                [],
            )?;

            Ok(())
        }),
        Migration::new("Add source statistics tables", 11, |conn| {
            // Create source statistics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS source_statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_type TEXT NOT NULL,
                    period_start INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    referrer TEXT,
                    utm_source TEXT,
                    utm_medium TEXT,
                    utm_campaign TEXT,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_type, period_start, source, referrer, utm_source, utm_medium, utm_campaign)
                )",
                [],
            )?;

            // Create source aggregated metrics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS source_aggregated_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_name TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    referrer TEXT,
                    utm_source TEXT,
                    utm_medium TEXT,
                    utm_campaign TEXT,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_name, start_ts, end_ts, source, referrer, utm_source, utm_medium, utm_campaign)
                )",
                [],
            )?;

            // Add indices for better query performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_statistics_period 
                 ON source_statistics(period_type, period_start)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_aggregated_metrics_period 
                 ON source_aggregated_metrics(period_name, start_ts, end_ts)",
                [],
            )?;

            Ok(())
        }),
        Migration::new("Add source column to events table", 12, |conn| {
            conn.execute(
                "ALTER TABLE events ADD COLUMN source TEXT DEFAULT 'Direct'",
                [],
            )?;
            Ok(())
        }),
        Migration::new("Add avg_visit_duration to statistics tables", 13, |conn| {
            // Add avg_visit_duration to statistics table
            conn.execute(
                "ALTER TABLE statistics ADD COLUMN avg_visit_duration INTEGER",
                [],
            )?;

            // Add avg_visit_duration to aggregated_metrics table
            conn.execute(
                "ALTER TABLE aggregated_metrics ADD COLUMN avg_visit_duration INTEGER",
                [],
            )?;

            Ok(())
        }),
        Migration::new(
            "Add avg_visit_duration to other statistics and metrics tables",
            14,
            |conn| {
                conn.execute(
                    "ALTER TABLE location_statistics ADD COLUMN avg_visit_duration INTEGER",
                    [],
                )?;
                conn.execute(
                    "ALTER TABLE location_aggregated_metrics ADD COLUMN avg_visit_duration INTEGER",
                    [],
                )?;

                conn.execute(
                    "ALTER TABLE device_statistics ADD COLUMN avg_visit_duration INTEGER",
                    [],
                )?;
                conn.execute(
                    "ALTER TABLE device_aggregated_metrics ADD COLUMN avg_visit_duration INTEGER",
                    [],
                )?;

                conn.execute(
                    "ALTER TABLE source_statistics ADD COLUMN avg_visit_duration INTEGER",
                    [],
                )?;
                conn.execute(
                    "ALTER TABLE source_aggregated_metrics ADD COLUMN avg_visit_duration INTEGER",
                    [],
                )?;

                Ok(())
            },
        ),
        Migration::new("Add bounce_rate to statistics tables", 15, |conn| {
            // Add bounce_rate to statistics table
            conn.execute("ALTER TABLE statistics ADD COLUMN bounce_rate INTEGER", [])?;

            // Add bounce_rate to aggregated_metrics table
            conn.execute(
                "ALTER TABLE aggregated_metrics ADD COLUMN bounce_rate INTEGER",
                [],
            )?;

            // Add bounce_rate to location_statistics table
            conn.execute(
                "ALTER TABLE location_statistics ADD COLUMN bounce_rate INTEGER",
                [],
            )?;

            // Add bounce_rate to location_aggregated_metrics table
            conn.execute(
                "ALTER TABLE location_aggregated_metrics ADD COLUMN bounce_rate INTEGER",
                [],
            )?;

            // Add bounce_rate to device_statistics table
            conn.execute(
                "ALTER TABLE device_statistics ADD COLUMN bounce_rate INTEGER",
                [],
            )?;

            // Add bounce_rate to device_aggregated_metrics table
            conn.execute(
                "ALTER TABLE device_aggregated_metrics ADD COLUMN bounce_rate INTEGER",
                [],
            )?;

            // Add bounce_rate to source_statistics table
            conn.execute(
                "ALTER TABLE source_statistics ADD COLUMN bounce_rate INTEGER",
                [],
            )?;

            // Add bounce_rate to source_aggregated_metrics table
            conn.execute(
                "ALTER TABLE source_aggregated_metrics ADD COLUMN bounce_rate INTEGER",
                [],
            )?;

            Ok(())
        }),
        Migration::new("Add last_visited_url to events table", 16, |conn| {
            // Add the new column
            conn.execute("ALTER TABLE events ADD COLUMN last_visited_url TEXT", [])?;

            // Update historical visit records with their last visited URL
            conn.execute(
                "WITH LastPageUrls AS (
                    SELECT 
                        v.id as visit_id,
                        v.visitor_id,
                        v.timestamp as visit_timestamp,
                        v.last_activity_at,
                        (
                            SELECT e.page_url
                            FROM events e
                            WHERE e.visitor_id = v.visitor_id
                            AND e.timestamp <= v.last_activity_at
                            AND e.timestamp >= v.timestamp
                            ORDER BY e.timestamp DESC
                            LIMIT 1
                        ) as last_url
                    FROM events v
                    WHERE v.event_type = 'visit'
                )
                UPDATE events
                SET last_visited_url = (
                    SELECT lpu.last_url
                    FROM LastPageUrls lpu
                    WHERE lpu.visit_id = events.id
                )
                WHERE event_type = 'visit'",
                [],
            )?;
            Ok(())
        }),
        Migration::new("Add URL paths to events table", 17, |conn| {
            // Add the new columns
            conn.execute("ALTER TABLE events ADD COLUMN page_url_path TEXT", [])?;
            conn.execute(
                "ALTER TABLE events ADD COLUMN last_visited_url_path TEXT",
                [],
            )?;

            // Update page_url_path for all events
            conn.execute(
                "UPDATE events 
                SET page_url_path = CASE 
                    WHEN page_url IS NULL THEN '/'
                    WHEN page_url NOT LIKE 'http%' THEN '/'
                    WHEN instr(page_url, '://') = 0 THEN '/'
                    WHEN substr(page_url, instr(page_url, '://')+3) NOT LIKE '%/%' THEN '/'
                    ELSE (
                        CASE
                            WHEN instr(
                                substr(substr(page_url, instr(page_url, '://')+3), 
                                    instr(substr(page_url, instr(page_url, '://')+3), '/')
                                ),
                                '?'
                            ) > 0
                            THEN substr(
                                substr(substr(page_url, instr(page_url, '://')+3), 
                                    instr(substr(page_url, instr(page_url, '://')+3), '/')
                                ),
                                1,
                                instr(
                                    substr(substr(page_url, instr(page_url, '://')+3), 
                                        instr(substr(page_url, instr(page_url, '://')+3), '/')
                                    ),
                                    '?'
                                ) - 1
                            )
                            WHEN instr(
                                substr(substr(page_url, instr(page_url, '://')+3), 
                                    instr(substr(page_url, instr(page_url, '://')+3), '/')
                                ),
                                '#'
                            ) > 0
                            THEN substr(
                                substr(substr(page_url, instr(page_url, '://')+3), 
                                    instr(substr(page_url, instr(page_url, '://')+3), '/')
                                ),
                                1,
                                instr(
                                    substr(substr(page_url, instr(page_url, '://')+3), 
                                        instr(substr(page_url, instr(page_url, '://')+3), '/')
                                    ),
                                    '#'
                                ) - 1
                            )
                            ELSE substr(substr(page_url, instr(page_url, '://')+3), 
                                instr(substr(page_url, instr(page_url, '://')+3), '/'))
                        END
                    )
                END
                WHERE page_url IS NOT NULL",
                [],
            )?;

            // Update last_visited_url_path for visit events
            conn.execute(
                "UPDATE events 
                SET last_visited_url_path = CASE 
                    WHEN last_visited_url IS NULL THEN '/'
                    WHEN last_visited_url NOT LIKE 'http%' THEN '/'
                    WHEN instr(last_visited_url, '://') = 0 THEN '/'
                    WHEN substr(last_visited_url, instr(last_visited_url, '://')+3) NOT LIKE '%/%' THEN '/'
                    ELSE (
                        CASE
                            WHEN instr(
                                substr(substr(last_visited_url, instr(last_visited_url, '://')+3), 
                                    instr(substr(last_visited_url, instr(last_visited_url, '://')+3), '/')
                                ),
                                '?'
                            ) > 0
                            THEN substr(
                                substr(substr(last_visited_url, instr(last_visited_url, '://')+3), 
                                    instr(substr(last_visited_url, instr(last_visited_url, '://')+3), '/')
                                ),
                                1,
                                instr(
                                    substr(substr(last_visited_url, instr(last_visited_url, '://')+3), 
                                        instr(substr(last_visited_url, instr(last_visited_url, '://')+3), '/')
                                    ),
                                    '?'
                                ) - 1
                            )
                            WHEN instr(
                                substr(substr(last_visited_url, instr(last_visited_url, '://')+3), 
                                    instr(substr(last_visited_url, instr(last_visited_url, '://')+3), '/')
                                ),
                                '#'
                            ) > 0
                            THEN substr(
                                substr(substr(last_visited_url, instr(last_visited_url, '://')+3), 
                                    instr(substr(last_visited_url, instr(last_visited_url, '://')+3), '/')
                                ),
                                1,
                                instr(
                                    substr(substr(last_visited_url, instr(last_visited_url, '://')+3), 
                                        instr(substr(last_visited_url, instr(last_visited_url, '://')+3), '/')
                                    ),
                                    '#'
                                ) - 1
                            )
                            ELSE substr(substr(last_visited_url, instr(last_visited_url, '://')+3), 
                                instr(substr(last_visited_url, instr(last_visited_url, '://')+3), '/'))
                        END
                    )
                END
                WHERE event_type = 'visit' AND last_visited_url IS NOT NULL",
                [],
            )?;

            // Create indices for the new columns
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_page_url_path ON events(page_url_path)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_last_visited_url_path ON events(last_visited_url_path)",
                [],
            )?;

            Ok(())
        }),
        Migration::new("Add page metrics tables", 18, |conn| {
            // Create page statistics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS page_statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_type TEXT NOT NULL,
                    period_start INTEGER NOT NULL,
                    page_path TEXT NOT NULL,
                    entry_page_path TEXT NOT NULL,
                    exit_page_path TEXT NOT NULL,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    avg_visit_duration INTEGER,
                    bounce_rate INTEGER,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_type, period_start, page_path, entry_page_path, exit_page_path)
                )",
                [],
            )?;

            // Create page aggregated metrics table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS page_aggregated_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_name TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL,
                    page_path TEXT NOT NULL,
                    entry_page_path TEXT NOT NULL,
                    exit_page_path TEXT NOT NULL,
                    visitors INTEGER NOT NULL,
                    visits INTEGER NOT NULL,
                    pageviews INTEGER NOT NULL,
                    avg_visit_duration INTEGER,
                    bounce_rate INTEGER,
                    created_at INTEGER NOT NULL,
                    UNIQUE(period_name, start_ts, end_ts, page_path, entry_page_path, exit_page_path)
                )",
                [],
            )?;

            // Add indices for better query performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_page_statistics_period 
                 ON page_statistics(period_type, period_start)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_page_aggregated_metrics_period 
                 ON page_aggregated_metrics(period_name, start_ts, end_ts)",
                [],
            )?;

            // Add indices for the path columns since they'll be used in grouping
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_page_statistics_paths 
                 ON page_statistics(page_path, entry_page_path, exit_page_path)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_page_aggregated_metrics_paths 
                 ON page_aggregated_metrics(page_path, entry_page_path, exit_page_path)",
                [],
            )?;

            Ok(())
        }),
    ]
}

async fn run_migrations(db: &Connection) -> Result<()> {
    info!("Running database migrations...");

    // Create migrations table and run migrations in a transaction
    db.call(|conn| {
        // Create migrations table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS migrations (
                id INTEGER PRIMARY KEY,
                version INTEGER NOT NULL UNIQUE,
                name TEXT NOT NULL,
                executed_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Get all migrations
        let migrations = get_migrations();

        // Get already executed migrations
        let mut stmt = conn.prepare("SELECT version FROM migrations ORDER BY version DESC")?;
        let executed_versions: Vec<i32> = stmt
            .query_map([], |row| row.get(0))?
            .filter_map(Result::ok)
            .collect();

        // Run each non-executed migration in a transaction
        for migration in migrations {
            if !executed_versions.contains(&migration.version) {
                info!(
                    "Running migration {} (version {}): {}",
                    migration.version, migration.name, migration.name
                );

                conn.execute("BEGIN TRANSACTION", [])?;

                // Run the migration
                (migration.up)(conn)?;

                // Record the migration
                conn.execute(
                    "INSERT INTO migrations (version, name, executed_at) VALUES (?1, ?2, unixepoch())",
                    params![&migration.version, &migration.name],
                )?;

                conn.execute("COMMIT", [])?;

                info!("Migration {} completed successfully", migration.version);
            }
        }

        Ok(())
    })
    .await?;

    info!("All database migrations completed successfully");
    Ok(())
}
