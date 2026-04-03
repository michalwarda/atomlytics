use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub bind_address: SocketAddr,
    pub database_url: String,
    pub dashboard_username: String,
    pub dashboard_password: String,
    pub allow_demo_geo_override: bool,
    pub maxmind_db_path: PathBuf,
    pub user_agent_regex_path: PathBuf,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let host = env::var("APP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("PORT").unwrap_or_else(|_| "3000".to_string());
        let bind_address = format!("{host}:{port}")
            .parse()
            .with_context(|| format!("invalid bind address {host}:{port}"))?;

        Ok(Self {
            bind_address,
            database_url: env::var("DATABASE_URL")
                .context("DATABASE_URL must be set for PostgreSQL")?,
            dashboard_username: env::var("DASHBOARD_USERNAME")
                .unwrap_or_else(|_| "admin".to_string()),
            dashboard_password: env::var("DASHBOARD_PASSWORD")
                .unwrap_or_else(|_| "admin".to_string()),
            allow_demo_geo_override: env::var("ALLOW_DEMO_GEO_OVERRIDE")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                .unwrap_or(false),
            maxmind_db_path: env::var("MAXMIND_DB_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("GeoLite2-City.mmdb")),
            user_agent_regex_path: env::var("UA_REGEX_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("regexes.yaml")),
        })
    }
}
