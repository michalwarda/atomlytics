mod analytics;
mod auth;
mod config;
mod domain;
mod entities;
mod geo;
mod http;
mod ui;

use anyhow::Result;
use migration::MigratorTrait;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::{sync::Arc, time::Duration};
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use tracing::Level;

use crate::{config::AppConfig, geo::GeoServices};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub db: DatabaseConnection,
    pub geo: Arc<GeoServices>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = AppConfig::from_env()?;

    if std::env::args().nth(1).as_deref() == Some("migrate") {
        let db = connect_database(&config.database_url).await?;
        migration::Migrator::up(&db, None).await?;
        return Ok(());
    }

    let db = connect_database(&config.database_url).await?;
    migration::Migrator::up(&db, None).await?;

    let geo = GeoServices::load(&config.maxmind_db_path, &config.user_agent_regex_path)?;
    let state = AppState {
        config: config.clone(),
        db,
        geo: Arc::new(geo),
    };

    let app = http::router(state)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
    tracing::info!("listening on http://{}", config.bind_address);
    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .await?;
    Ok(())
}

async fn connect_database(database_url: &str) -> Result<DatabaseConnection> {
    let mut options = ConnectOptions::new(database_url.to_string());
    options
        .max_connections(10)
        .min_connections(1)
        .connect_timeout(Duration::from_secs(10))
        .acquire_timeout(Duration::from_secs(10))
        .sqlx_logging(false);
    Ok(Database::connect(options).await?)
}
