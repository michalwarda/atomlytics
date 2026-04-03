use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use maxminddb::geoip2;
use uaparser::{Parser, UserAgentParser};

#[derive(Clone)]
pub struct GeoServices {
    geoip: Arc<maxminddb::Reader<Vec<u8>>>,
    user_agent_parser: Arc<UserAgentParser>,
}

#[derive(Clone, Debug)]
pub struct GeoLocation {
    pub country: String,
    pub region: String,
    pub city: String,
}

#[derive(Clone, Debug)]
pub struct UserAgentInfo {
    pub browser: String,
    pub operating_system: String,
    pub device_type: String,
}

impl GeoServices {
    pub fn load(maxmind_path: &Path, ua_regex_path: &Path) -> Result<Self> {
        let geoip = maxminddb::Reader::open_readfile(maxmind_path).with_context(|| {
            format!(
                "failed to load MaxMind database from {}",
                maxmind_path.display()
            )
        })?;
        let user_agent_parser = UserAgentParser::from_yaml(
            ua_regex_path
                .to_str()
                .context("UA_REGEX_PATH must be valid UTF-8")?,
        )
            .with_context(|| format!("failed to load {}", ua_regex_path.display()))?;

        Ok(Self {
            geoip: Arc::new(geoip),
            user_agent_parser: Arc::new(user_agent_parser),
        })
    }

    pub fn lookup_location(&self, ip: &str) -> GeoLocation {
        let parsed = match ip.parse() {
            Ok(parsed) => parsed,
            Err(_) => return GeoLocation::unknown(),
        };

        match self.geoip.lookup(parsed) {
            Ok(result) => match result.decode::<geoip2::City>() {
                Ok(Some(city)) => GeoLocation {
                    country: extract_name(&city.country.names),
                    region: extract_name(
                        &city
                            .subdivisions
                            .first()
                            .map(|subdivision| &subdivision.names)
                            .cloned()
                            .unwrap_or_default(),
                    ),
                    city: extract_name(&city.city.names),
                },
                _ => GeoLocation::unknown(),
            },
            Err(_) => GeoLocation::unknown(),
        }
    }

    pub fn parse_user_agent(&self, user_agent: &str) -> UserAgentInfo {
        let parsed = self.user_agent_parser.parse(user_agent);
        let browser = format!(
            "{} {}",
            parsed.user_agent.family,
            parsed.user_agent.major.unwrap_or_default()
        )
        .trim()
        .to_string();
        let operating_system = format!("{} {}", parsed.os.family, parsed.os.major.unwrap_or_default())
            .trim()
            .to_string();
        let family = parsed.device.family.to_lowercase();
        let device_type = if family.contains("mobile")
            || family.contains("phone")
            || family.contains("android")
            || family.contains("iphone")
        {
            "Mobile"
        } else if family.contains("tablet") || family.contains("ipad") {
            "Tablet"
        } else if family.contains("bot") || family.contains("crawler") || family.contains("spider") {
            "Bot"
        } else {
            "Desktop"
        };

        UserAgentInfo {
            browser: fallback_unknown(browser),
            operating_system: fallback_unknown(operating_system),
            device_type: device_type.to_string(),
        }
    }
}

impl GeoLocation {
    pub fn unknown() -> Self {
        Self {
            country: "Unknown".to_string(),
            region: "Unknown".to_string(),
            city: "Unknown".to_string(),
        }
    }
}

fn extract_name(names: &geoip2::Names<'_>) -> String {
    names
        .english
        .map(ToString::to_string)
        .unwrap_or_else(|| "Unknown".to_string())
}

fn fallback_unknown(value: String) -> String {
    if value.trim().is_empty() {
        "Unknown".to_string()
    } else {
        value
    }
}
