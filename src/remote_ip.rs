use axum::http::HeaderMap;
use regex::Regex;
use std::net::SocketAddr;

pub struct RemoteIp;

impl RemoteIp {
    pub fn get(headers: &HeaderMap, socket_addr: &SocketAddr) -> String {
        let x_atomlytics_ip = headers.get("x-atomlytics-ip").and_then(|h| h.to_str().ok());
        let cf_connecting_ip = headers
            .get("cf-connecting-ip")
            .and_then(|h| h.to_str().ok());
        let x_forwarded_for = headers.get("x-forwarded-for").and_then(|h| h.to_str().ok());
        let b_forwarded_for = headers.get("b-forwarded-for").and_then(|h| h.to_str().ok());
        let forwarded = headers.get("forwarded").and_then(|h| h.to_str().ok());

        if let Some(ip) = x_atomlytics_ip {
            if !ip.is_empty() {
                return Self::clean_ip(ip);
            }
        }

        if let Some(ip) = cf_connecting_ip {
            if !ip.is_empty() {
                return Self::clean_ip(ip);
            }
        }

        if let Some(ip) = b_forwarded_for {
            if !ip.is_empty() {
                return Self::parse_forwarded_for(ip);
            }
        }

        if let Some(ip) = x_forwarded_for {
            if !ip.is_empty() {
                return Self::parse_forwarded_for(ip);
            }
        }

        if let Some(forwarded_header) = forwarded {
            if !forwarded_header.is_empty() {
                if let Ok(re) = Regex::new(r"for=(?<for>[^;,]+).*$") {
                    if let Some(caps) = re.captures(forwarded_header) {
                        if let Some(ip) = caps.name("for") {
                            let ip_str = ip.as_str().trim_matches('"');
                            return Self::clean_ip(ip_str);
                        }
                    }
                }
            }
        }

        socket_addr.ip().to_string()
    }

    fn clean_ip(ip_and_port: &str) -> String {
        let re = Regex::new(r"((\.\d+)|(\]))(?P<port>:[0-9]+)$").unwrap();

        let ip = if let Some(caps) = re.captures(ip_and_port) {
            if let Some(port) = caps.name("port") {
                let port_str = port.as_str();
                ip_and_port[..ip_and_port.len() - port_str.len()].to_string()
            } else {
                ip_and_port.to_string()
            }
        } else {
            ip_and_port.to_string()
        };

        ip.trim_start_matches('[').trim_end_matches(']').to_string()
    }

    fn parse_forwarded_for(header: &str) -> String {
        header
            .split(',')
            .next()
            .map(|s| s.trim())
            .map(|s| Self::clean_ip(s))
            .unwrap_or_else(|| "".to_string())
    }
}
